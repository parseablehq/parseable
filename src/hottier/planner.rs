use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};

use crate::{
    catalog::manifest::{File, Manifest},
    utils::extract_datetime,
};

#[derive(Debug)]
pub(super) struct WorkItem {
    pub timestamp: DateTime<Utc>,
    pub minute_path: PathBuf,
    pub local_path: PathBuf,
    pub file: File,
}

#[derive(Debug, Default)]
pub(super) struct WorkPlan {
    pub source_watermark: Option<DateTime<Utc>>,
    pub inventory_files: usize,
    pub active_minute_paths: BTreeSet<PathBuf>,
    pub items: Vec<WorkItem>,
}

pub(super) fn build_work(
    manifests: &BTreeMap<String, Vec<Manifest>>,
    latest_minutes: u64,
    cache_root: &Path,
    is_valid_local: impl Fn(&Path, u64) -> bool,
) -> WorkPlan {
    let discovered = manifests
        .values()
        .flatten()
        .flat_map(|manifest| manifest.files.iter())
        .filter_map(|file| {
            extract_datetime(&file.file_path).map(|timestamp| (timestamp.and_utc(), file))
        })
        .collect::<Vec<_>>();
    let mut unique = BTreeMap::new();
    for (timestamp, file) in discovered {
        unique
            .entry(file.file_path.clone())
            .or_insert((timestamp, file));
    }
    let mut files = unique.into_values().collect::<Vec<_>>();
    let source_watermark = files.iter().map(|(timestamp, _)| *timestamp).max();
    let Some(watermark) = source_watermark else {
        return WorkPlan::default();
    };
    let cutoff = watermark - chrono::Duration::minutes(latest_minutes as i64);

    let active = files
        .drain(..)
        .filter(|(timestamp, _)| *timestamp > cutoff)
        .collect::<Vec<_>>();
    let active_minute_paths = active
        .iter()
        .filter_map(|(_, file)| {
            minute_ancestor(&cache_root.join(&file.file_path)).map(Path::to_path_buf)
        })
        .collect();
    let inventory_files = active.len();
    let mut items = active
        .into_iter()
        .filter_map(|(timestamp, file)| {
            let local_path = cache_root.join(&file.file_path);
            if is_valid_local(&local_path, file.file_size) {
                return None;
            }
            let minute_path = minute_ancestor(&local_path)?.to_path_buf();
            Some(WorkItem {
                timestamp,
                minute_path,
                local_path,
                file: file.clone(),
            })
        })
        .collect::<Vec<_>>();
    items.sort_by(|left, right| {
        left.timestamp
            .cmp(&right.timestamp)
            .then_with(|| left.file.file_path.cmp(&right.file.file_path))
    });

    WorkPlan {
        source_watermark: Some(watermark),
        inventory_files,
        active_minute_paths,
        items,
    }
}

pub(super) fn minute_ancestor(path: &Path) -> Option<&Path> {
    for ancestor in path.ancestors() {
        let name = ancestor.file_name()?.to_string_lossy();
        if name.starts_with("minute=") {
            let has_hour = ancestor
                .parent()
                .and_then(Path::file_name)
                .and_then(|hour| hour.to_str())
                .is_some_and(|hour| hour.starts_with("hour="));
            let has_date = ancestor
                .parent()
                .and_then(Path::parent)
                .and_then(Path::file_name)
                .and_then(|date| date.to_str())
                .is_some_and(|date| date.starts_with("date="));
            if has_date && has_hour {
                return Some(ancestor);
            }
        }
    }
    None
}

pub(super) fn reconcile_local_file(path: &Path, expected_size: u64) -> bool {
    match std::fs::metadata(path) {
        Ok(metadata) if metadata.len() == expected_size => true,
        Ok(_) => {
            let _ = std::fs::remove_file(path);
            false
        }
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, path::Path};

    use chrono::{TimeZone, Utc};

    use crate::catalog::manifest::{File, Manifest};

    use super::{build_work, minute_ancestor, reconcile_local_file};

    fn file(path: &str, size: u64) -> File {
        File {
            file_path: path.to_owned(),
            file_size: size,
            ..File::default()
        }
    }

    #[test]
    fn work_uses_source_watermark_and_orders_oldest_path_first() {
        let mut manifests = BTreeMap::new();
        manifests.insert(
            "date=2026-07-16".to_owned(),
            vec![
                Manifest {
                    files: vec![
                        file("logs/date=2026-07-16/hour=12/minute=14/b.parquet", 2),
                        file("logs/date=2026-07-16/hour=12/minute=00/z.parquet", 3),
                        file("logs/date=2026-07-16/hour=12/minute=14/a.parquet", 1),
                        file("logs/date=2026-07-16/hour=11/minute=59/old.parquet", 4),
                    ],
                    ..Manifest::default()
                },
                Manifest {
                    files: vec![file("logs/date=2026-07-16/hour=12/minute=14/a.parquet", 1)],
                    ..Manifest::default()
                },
            ],
        );

        let plan = build_work(
            &manifests,
            15,
            Path::new("/cache"),
            |_path, _expected_size| false,
        );

        assert_eq!(
            plan.source_watermark,
            Some(Utc.with_ymd_and_hms(2026, 7, 16, 12, 14, 0).unwrap())
        );
        assert_eq!(
            plan.items
                .iter()
                .map(|item| item.file.file_path.as_str())
                .collect::<Vec<_>>(),
            vec![
                "logs/date=2026-07-16/hour=12/minute=00/z.parquet",
                "logs/date=2026-07-16/hour=12/minute=14/a.parquet",
                "logs/date=2026-07-16/hour=12/minute=14/b.parquet",
            ]
        );
    }

    #[test]
    fn custom_partition_file_groups_by_date_hour_minute_ancestor() {
        let path = Path::new(
            "/cache/logs/date=2026-07-16/hour=12/minute=14/region=west/service=api/a.parquet",
        );

        assert_eq!(
            minute_ancestor(path),
            Some(Path::new("/cache/logs/date=2026-07-16/hour=12/minute=14"))
        );
    }

    #[test]
    fn wrong_sized_local_file_is_removed_before_reservation() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("wrong.parquet");
        std::fs::write(&path, [0_u8; 3]).unwrap();

        assert!(!reconcile_local_file(&path, 7));
        assert!(!path.exists());
    }
}
