use std::{collections::HashMap, path::PathBuf, sync::Arc};

use chrono::{TimeZone, Utc};
use datafusion::{common::Column, prelude::Expr};
use itertools::Itertools;
use relative_path::RelativePathBuf;

use crate::query::stream_schema_provider::extract_primary_filter;
use crate::{
    catalog::{
        Snapshot,
        manifest::{File, Manifest},
        snapshot,
    },
    event,
    parseable::PARSEABLE,
    query::{PartialTimeFilter, stream_schema_provider::ManifestExt},
    storage::{ObjectStorage, ObjectStorageError, ObjectStoreFormat, STREAM_ROOT_DIRECTORY},
    utils::time::TimeRange,
};

pub fn create_time_filter(
    time_range: &TimeRange,
    time_partition: Option<String>,
    table_name: &str,
) -> Vec<Expr> {
    let mut new_filters = vec![];
    let start_time = time_range.start.naive_utc();
    let end_time = time_range.end.naive_utc();
    let mut _start_time_filter: Expr;
    let mut _end_time_filter: Expr;

    match time_partition {
        Some(time_partition) => {
            _start_time_filter = PartialTimeFilter::Low(std::ops::Bound::Included(start_time))
                .binary_expr(Expr::Column(Column::new(
                    Some(table_name.to_owned()),
                    time_partition.clone(),
                )));
            _end_time_filter =
                PartialTimeFilter::High(std::ops::Bound::Excluded(end_time)).binary_expr(
                    Expr::Column(Column::new(Some(table_name.to_owned()), time_partition)),
                );
        }
        None => {
            _start_time_filter = PartialTimeFilter::Low(std::ops::Bound::Included(start_time))
                .binary_expr(Expr::Column(Column::new(
                    Some(table_name.to_owned()),
                    event::DEFAULT_TIMESTAMP_KEY,
                )));
            _end_time_filter = PartialTimeFilter::High(std::ops::Bound::Excluded(end_time))
                .binary_expr(Expr::Column(Column::new(
                    Some(table_name.to_owned()),
                    event::DEFAULT_TIMESTAMP_KEY,
                )));
        }
    }

    new_filters.push(_start_time_filter);
    new_filters.push(_end_time_filter);

    new_filters
}

pub async fn fetch_parquet_file_paths(
    stream: &str,
    time_range: &TimeRange,
) -> Result<HashMap<RelativePathBuf, Vec<File>>, ObjectStorageError> {
    let glob_storage = PARSEABLE.storage.get_object_store();

    let object_store_format = glob_storage.get_object_store_format(stream).await?;

    let time_partition = object_store_format.time_partition;

    let time_filter_expr = create_time_filter(time_range, time_partition.clone(), stream);

    let time_filters = extract_primary_filter(&time_filter_expr, &time_partition);

    let mut merged_snapshot: snapshot::Snapshot = snapshot::Snapshot::default();

    let path = RelativePathBuf::from_iter([stream, STREAM_ROOT_DIRECTORY]);
    let obs = glob_storage
        .get_objects(
            Some(&path),
            Box::new(|file_name| file_name.ends_with("stream.json")),
        )
        .await;
    if let Ok(obs) = obs {
        for ob in obs {
            if let Ok(object_store_format) = serde_json::from_slice::<ObjectStoreFormat>(&ob) {
                let snapshot = object_store_format.snapshot;
                for manifest in snapshot.manifest_list {
                    merged_snapshot.manifest_list.push(manifest);
                }
            }
        }
    }

    let manifest_files = collect_manifest_files(
        glob_storage,
        merged_snapshot
            .manifests(&time_filters)
            .into_iter()
            .sorted_by_key(|file| file.time_lower_bound)
            .map(|item| item.manifest_path)
            .collect(),
    )
    .await?;

    let mut parquet_files: HashMap<RelativePathBuf, Vec<File>> = HashMap::new();

    let mut selected_files = manifest_files
        .into_iter()
        .flat_map(|file| file.files)
        .rev()
        .collect_vec();

    for filter in time_filter_expr {
        selected_files.retain(|file| !file.can_be_pruned(&filter))
    }

    selected_files
        .into_iter()
        .filter_map(|file| {
            let date = file.file_path.split("/").collect_vec();

            let year = &date[1][5..9];
            let month = &date[1][10..12];
            let day = &date[1][13..15];
            let hour = &date[2][5..7];
            let min = &date[3][7..9];
            let file_date = Utc
                .with_ymd_and_hms(
                    year.parse::<i32>().unwrap(),
                    month.parse::<u32>().unwrap(),
                    day.parse::<u32>().unwrap(),
                    hour.parse::<u32>().unwrap(),
                    min.parse::<u32>().unwrap(),
                    0,
                )
                .unwrap();

            if file_date < time_range.start {
                None
            } else {
                let date = date.as_slice()[1..4].iter().map(|s| s.to_string());

                let date = RelativePathBuf::from_iter(date);

                parquet_files.entry(date).or_default().push(file);
                Some("")
            }
        })
        .for_each(|_| {});

    Ok(parquet_files)
}

async fn collect_manifest_files(
    storage: Arc<dyn ObjectStorage>,
    manifest_urls: Vec<String>,
) -> Result<Vec<Manifest>, ObjectStorageError> {
    let mut tasks = Vec::new();
    manifest_urls.into_iter().for_each(|path| {
        let path = RelativePathBuf::from_path(PathBuf::from(path)).expect("Invalid path");
        let storage = Arc::clone(&storage);
        tasks.push(tokio::task::spawn(async move {
            storage.get_object(&path).await
        }));
    });

    let mut op = Vec::new();
    for task in tasks {
        let file = task.await??;
        op.push(file);
    }

    Ok(op
        .into_iter()
        .map(|res| serde_json::from_slice(&res).expect("Data is invalid for Manifest"))
        .collect())
}
