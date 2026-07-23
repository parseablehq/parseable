use std::{
    collections::{BTreeMap, HashMap},
    io,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use tokio::io::AsyncWriteExt;

use super::planner::minute_ancestor;

pub(super) const INDEX_FILENAME: &str = ".hot_tier.index.v1.json";
const PARTIAL_INDEX_FILENAME: &str = ".hot_tier.index.v1.json.partial";

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
struct Checkpoint {
    #[serde(default)]
    source_watermark: Option<DateTime<Utc>>,
    #[serde(default)]
    minutes: BTreeMap<String, MinuteTotals>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub(super) struct MinuteTotals {
    pub bytes: u64,
    pub files: u64,
    #[serde(default)]
    pub verified: bool,
}

#[derive(Debug, Default)]
pub(super) struct RuntimeState {
    pub source_watermark: Option<DateTime<Utc>>,
    pub minutes: BTreeMap<String, MinuteTotals>,
    reservations: HashMap<String, u64>,
    inflight_buckets: HashMap<String, usize>,
}

impl RuntimeState {
    pub fn used_bytes(&self) -> u64 {
        self.minutes.values().map(|minute| minute.bytes).sum()
    }

    pub fn reserved_bytes(&self) -> u64 {
        self.reservations.values().sum()
    }

    pub fn free_bytes(&self, quota: u64) -> u64 {
        quota.saturating_sub(self.used_bytes().saturating_add(self.reserved_bytes()))
    }

    pub fn try_reserve(&mut self, path: &str, bytes: u64, quota: u64) -> bool {
        if self.reservations.contains_key(path) || bytes > self.free_bytes(quota) {
            return false;
        }
        self.reservations.insert(path.to_owned(), bytes);
        true
    }

    pub fn refund(&mut self, path: &str) {
        self.reservations.remove(path);
    }

    pub fn commit(&mut self, path: &str, minute: String) {
        let Some(bytes) = self.reservations.remove(path) else {
            return;
        };
        let totals = self.minutes.entry(minute).or_default();
        totals.bytes = totals.bytes.saturating_add(bytes);
        totals.files = totals.files.saturating_add(1);
        totals.verified = true;
    }

    pub fn mark_bucket_inflight(&mut self, minute: &str) {
        *self.inflight_buckets.entry(minute.to_owned()).or_default() += 1;
    }

    pub fn unmark_bucket_inflight(&mut self, minute: &str) {
        let Some(count) = self.inflight_buckets.get_mut(minute) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            self.inflight_buckets.remove(minute);
        }
    }

    pub fn oldest_evictable_bucket(&self) -> Option<&str> {
        self.minutes
            .keys()
            .find(|minute| !self.inflight_buckets.contains_key(*minute))
            .map(String::as_str)
    }

    pub fn remove_bucket(&mut self, minute: &str) -> u64 {
        self.minutes
            .remove(minute)
            .map(|totals| totals.bytes)
            .unwrap_or_default()
    }
}

pub(super) async fn load_or_rebuild(stream_root: &Path) -> io::Result<RuntimeState> {
    tokio::fs::create_dir_all(stream_root).await?;
    let checkpoint_path = stream_root.join(INDEX_FILENAME);
    if let Ok(bytes) = tokio::fs::read(&checkpoint_path).await
        && let Ok(mut checkpoint) = serde_json::from_slice::<Checkpoint>(&bytes)
    {
        for totals in checkpoint.minutes.values_mut() {
            totals.verified = false;
        }
        return Ok(RuntimeState {
            source_watermark: checkpoint.source_watermark,
            minutes: checkpoint.minutes,
            ..RuntimeState::default()
        });
    }

    let (minutes, legacy_manifests) = scan_stream_root(stream_root).await?;
    let state = RuntimeState {
        minutes,
        ..RuntimeState::default()
    };
    persist_checkpoint(stream_root, &state).await?;

    // A replacement checkpoint is durable and visible before legacy state is removed.
    for manifest in legacy_manifests {
        tokio::fs::remove_file(manifest).await?;
    }
    Ok(state)
}

pub(super) async fn persist_checkpoint(stream_root: &Path, state: &RuntimeState) -> io::Result<()> {
    tokio::fs::create_dir_all(stream_root).await?;
    let bytes = serde_json::to_vec(&Checkpoint {
        source_watermark: state.source_watermark,
        minutes: state.minutes.clone(),
    })
    .map_err(io::Error::other)?;
    let partial_path = stream_root.join(PARTIAL_INDEX_FILENAME);
    let final_path = stream_root.join(INDEX_FILENAME);
    let mut file = tokio::fs::File::create(&partial_path).await?;
    file.write_all(&bytes).await?;
    file.flush().await?;
    let file = file.into_std().await;
    tokio::task::spawn_blocking(move || file.sync_all())
        .await
        .map_err(io::Error::other)??;
    tokio::fs::rename(partial_path, final_path).await
}

async fn scan_stream_root(
    stream_root: &Path,
) -> io::Result<(BTreeMap<String, MinuteTotals>, Vec<PathBuf>)> {
    let mut minutes = BTreeMap::<String, MinuteTotals>::new();
    let mut legacy_manifests = Vec::new();
    let mut pending = vec![stream_root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let mut entries = tokio::fs::read_dir(directory).await?;
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            let path = entry.path();
            if file_type.is_dir() {
                pending.push(path);
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            if entry.file_name() == "hottier.manifest.json" {
                legacy_manifests.push(path);
                continue;
            }
            if path.extension().and_then(|extension| extension.to_str()) != Some("parquet") {
                continue;
            }
            let Some(minute) = minute_ancestor(&path) else {
                continue;
            };
            let Ok(minute) = minute.strip_prefix(stream_root) else {
                continue;
            };
            let totals = minutes
                .entry(minute.to_string_lossy().into_owned())
                .or_default();
            totals.bytes = totals.bytes.saturating_add(entry.metadata().await?.len());
            totals.files = totals.files.saturating_add(1);
            totals.verified = true;
        }
    }
    Ok((minutes, legacy_manifests))
}

pub(super) async fn verify_bucket(stream_root: &Path, minute: &str) -> io::Result<MinuteTotals> {
    let bucket = stream_root.join(minute);
    if !bucket.exists() {
        return Ok(MinuteTotals {
            verified: true,
            ..MinuteTotals::default()
        });
    }
    let (minutes, _) = scan_stream_root(&bucket).await?;
    let mut total = MinuteTotals {
        verified: true,
        ..MinuteTotals::default()
    };
    for value in minutes.values() {
        total.bytes = total.bytes.saturating_add(value.bytes);
        total.files = total.files.saturating_add(value.files);
    }
    Ok(total)
}

pub(super) async fn cleanup_stale_partials(stream_root: PathBuf) -> io::Result<usize> {
    let mut removed = 0;
    let mut pending = vec![stream_root];
    while let Some(directory) = pending.pop() {
        let mut entries = match tokio::fs::read_dir(directory).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                pending.push(entry.path());
            } else if entry.file_name().to_string_lossy().ends_with(".partial") {
                tokio::fs::remove_file(entry.path()).await?;
                removed += 1;
            }
        }
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use crate::catalog::manifest::{File, Manifest};

    use super::{
        INDEX_FILENAME, MinuteTotals, RuntimeState, cleanup_stale_partials, load_or_rebuild,
        persist_checkpoint,
    };

    #[test]
    fn reservations_are_per_file_and_always_refunded_or_committed() {
        let mut state = RuntimeState::default();
        state.minutes.insert(
            "date=2026-07-16/hour=11/minute=59".to_owned(),
            MinuteTotals {
                bytes: 20,
                files: 1,
                verified: true,
            },
        );

        assert!(state.try_reserve("a.parquet", 50, 100));
        assert!(!state.try_reserve("oversized.parquet", 40, 100));
        assert_eq!(state.used_bytes(), 20);
        assert_eq!(state.reserved_bytes(), 50);
        assert_eq!(state.free_bytes(100), 30);

        state.refund("a.parquet");
        assert_eq!(state.reserved_bytes(), 0);
        assert_eq!(state.free_bytes(100), 80);

        assert!(state.try_reserve("b.parquet", 50, 100));
        state.commit("b.parquet", "date=2026-07-16/hour=12/minute=00".to_owned());
        assert_eq!(state.used_bytes(), 70);
        assert_eq!(state.reserved_bytes(), 0);
        assert_eq!(state.free_bytes(100), 30);
    }

    #[test]
    fn oldest_non_inflight_bucket_is_selected_for_eviction() {
        let mut state = RuntimeState::default();
        for minute in ["minute=00", "minute=01", "minute=02"] {
            state.minutes.insert(
                format!("date=2026-07-16/hour=12/{minute}"),
                MinuteTotals {
                    bytes: 10,
                    files: 1,
                    verified: true,
                },
            );
        }
        state.mark_bucket_inflight("date=2026-07-16/hour=12/minute=00");

        assert_eq!(
            state.oldest_evictable_bucket(),
            Some("date=2026-07-16/hour=12/minute=01")
        );
    }

    #[tokio::test]
    async fn missing_checkpoint_rebuilds_custom_partition_buckets() {
        let temp = tempdir().unwrap();
        let stream_root = temp.path().join("logs");
        let minute = stream_root.join("date=2026-07-16/hour=12/minute=14/region=west/service=api");
        fs::create_dir_all(&minute).unwrap();
        fs::write(minute.join("a.parquet"), [1_u8; 7]).unwrap();
        fs::create_dir_all(stream_root.join("unknown/stuff")).unwrap();
        fs::write(stream_root.join("unknown/stuff/not-data"), [1_u8; 20]).unwrap();

        let state = load_or_rebuild(&stream_root).await.unwrap();

        assert_eq!(state.used_bytes(), 7);
        assert_eq!(state.minutes.len(), 1);
        assert!(stream_root.join(INDEX_FILENAME).exists());
    }

    #[tokio::test]
    async fn corrupt_checkpoint_falls_back_to_filesystem_rebuild() {
        let temp = tempdir().unwrap();
        let stream_root = temp.path().join("logs");
        let minute = stream_root.join("date=2026-07-16/hour=12/minute=14");
        fs::create_dir_all(&minute).unwrap();
        fs::write(minute.join("a.parquet"), [1_u8; 9]).unwrap();
        fs::write(stream_root.join(INDEX_FILENAME), b"not-json").unwrap();

        let state = load_or_rebuild(&stream_root).await.unwrap();

        assert_eq!(state.used_bytes(), 9);
        serde_json::from_slice::<serde_json::Value>(
            &fs::read(stream_root.join(INDEX_FILENAME)).unwrap(),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn legacy_manifest_is_deleted_only_after_checkpoint_is_written() {
        let temp = tempdir().unwrap();
        let stream_root = temp.path().join("logs");
        let minute = stream_root.join("date=2026-07-16/hour=12/minute=14");
        fs::create_dir_all(&minute).unwrap();
        fs::write(minute.join("a.parquet"), [1_u8; 5]).unwrap();
        let legacy_path = stream_root
            .join("date=2026-07-16")
            .join("hottier.manifest.json");
        fs::write(
            &legacy_path,
            serde_json::to_vec(&Manifest {
                files: vec![File {
                    file_path: "logs/date=2026-07-16/hour=12/minute=14/a.parquet".to_owned(),
                    file_size: 5,
                    ..File::default()
                }],
                ..Manifest::default()
            })
            .unwrap(),
        )
        .unwrap();

        let state = load_or_rebuild(&stream_root).await.unwrap();

        assert_eq!(state.used_bytes(), 5);
        assert!(stream_root.join(INDEX_FILENAME).exists());
        assert!(!legacy_path.exists());
    }

    #[tokio::test]
    async fn checkpoint_restart_restores_committed_state_without_reservations() {
        let temp = tempdir().unwrap();
        let mut state = RuntimeState::default();
        state.minutes.insert(
            "date=2026-07-16/hour=12/minute=14".to_owned(),
            MinuteTotals {
                bytes: 11,
                files: 2,
                verified: true,
            },
        );
        assert!(state.try_reserve("inflight.parquet", 7, 100));
        persist_checkpoint(temp.path(), &state).await.unwrap();

        let restored = load_or_rebuild(temp.path()).await.unwrap();

        assert_eq!(restored.used_bytes(), 11);
        assert_eq!(restored.reserved_bytes(), 0);
    }

    #[tokio::test]
    async fn stale_partial_cleanup_does_not_remove_committed_parquet() {
        let temp = tempdir().unwrap();
        fs::write(temp.path().join("crash.parquet.partial"), [1_u8; 3]).unwrap();
        fs::write(temp.path().join("committed.parquet"), [1_u8; 5]).unwrap();

        let removed = cleanup_stale_partials(temp.path().to_path_buf())
            .await
            .unwrap();

        assert_eq!(removed, 1);
        assert!(!temp.path().join("crash.parquet.partial").exists());
        assert!(temp.path().join("committed.parquet").exists());
    }
}
