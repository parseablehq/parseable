//! Query-owned references to immutable, published staging Parquet files.

use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use datafusion::{
    datasource::listing::PartitionedFile,
    error::{DataFusionError, Result},
};

/// Keep the original name for canonical upload-path comparison; scans use only
/// the leased path. Published `.parquet` files must never be rewritten in place.
pub(super) struct CapturedFile {
    original: PathBuf,
    file: PartitionedFile,
}

pub(super) struct LeasedFile {
    original: PathBuf,
    leased: PathBuf,
    size: u64,
    lease: Arc<StagingLease>,
}

const LEASE_PREFIX: &str = ".query-parquet-";
const OWNER_LOCK: &str = ".owner.lock";

#[derive(Debug)]
struct StagingLease {
    directory: Option<tempfile::TempDir>,
    _owner: Option<fs::File>,
}

impl StagingLease {
    fn path(&self) -> &Path {
        self.directory
            .as_ref()
            .expect("live lease directory")
            .path()
    }

    fn new(directory: &Path) -> Result<Arc<Self>> {
        let mut lease = Self {
            directory: Some(
                tempfile::Builder::new()
                    .prefix(LEASE_PREFIX)
                    .tempdir_in(directory)
                    .map_err(|error| capture_error(directory, error))?,
            ),
            _owner: None,
        };
        let pending = lease.path().join(".owner.pending");
        let owner = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&pending)
            .map_err(|error| capture_error(directory, error))?;
        owner
            .try_lock()
            .map_err(|error| capture_error(directory, error))?;
        lease._owner = Some(owner);
        // Publish the ownership file only AFTER taking its lock and BEFORE
        // creating hard links. A sweeper cannot mistake setup for an orphan.
        fs::rename(&pending, lease.path().join(OWNER_LOCK))
            .map_err(|error| capture_error(directory, error))?;
        Ok(Arc::new(lease))
    }
}

impl Drop for StagingLease {
    fn drop(&mut self) {
        if let Some(directory) = self.directory.take() {
            let path = directory.path().to_path_buf();
            // Keep ownership locked during removal. close(), unlike TempDir's
            // default Drop, reports cleanup failures instead of hiding them.
            if let Err(error) = directory.close() {
                tracing::warn!(path = %path.display(), %error, "Cannot remove staging query lease");
            }
        }
    }
}

/// Runs during query capture, outside stream-registry locks. Safe while serving
/// active queries, including in another server process.
/// Only the OS lock proves a lease is abandoned; never infer it from age/PID.
/// Unpublished ownership files are skipped (setup has not created any links).
pub(crate) fn reclaim_staging_leases(directory: &Path) {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => {
            tracing::warn!(path = %directory.display(), %error, "Cannot list staging query leases");
            return;
        }
    };
    for entry in entries {
        let result = (|| -> std::io::Result<()> {
            let entry = entry?;
            if !entry
                .file_name()
                .to_string_lossy()
                .starts_with(LEASE_PREFIX)
                || !entry.file_type()?.is_dir()
            {
                return Ok(());
            }
            let path = entry.path();
            let owner_path = path.join(OWNER_LOCK);
            // Avoid following links outside our private, generated directory.
            if !fs::symlink_metadata(&owner_path)?.file_type().is_file() {
                return Ok(());
            }
            let owner = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(owner_path)?;
            match owner.try_lock() {
                Ok(()) => {
                    fs::remove_dir_all(&path)?;
                    tracing::debug!(path = %path.display(), "Removed abandoned staging query lease");
                }
                Err(std::fs::TryLockError::WouldBlock) => {}
                Err(std::fs::TryLockError::Error(error)) => return Err(error),
            }
            Ok(())
        })();
        if let Err(error) = result {
            // A concurrent drop/sweep may already have removed the directory;
            // a not-yet-published owner lock is intentionally skipped too.
            if error.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(path = %directory.display(), %error, "Cannot reclaim staging query lease");
            }
        }
    }
}

fn capture_error(path: &Path, error: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(format!(
        "Cannot capture staging Parquet {} for query: {error}",
        path.display()
    ))
}

pub(super) fn lease_staging_parquet(directory: &Path) -> Result<Vec<LeasedFile>> {
    reclaim_staging_leases(directory);
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        // Streams that have never staged data have no directory yet. Once a
        // filename is observed, any disappearance is an error, never an omission.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(capture_error(directory, error)),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let path = entry
            .map_err(|error| capture_error(directory, error))?
            .path();
        if path
            .extension()
            .is_some_and(|extension| extension == "parquet")
        {
            paths.push(path);
        }
    }
    paths.sort_by(|a, b| b.cmp(a));
    capture_paths(directory, paths)
}

#[cfg(test)]
fn capture_staging_parquet(directory: &Path) -> Result<Vec<CapturedFile>> {
    validate_staging_parquet(lease_staging_parquet(directory)?)
}

/// Exercise lease acquisition and validation from stream publication tests without
/// exposing query capture internals outside test builds.
#[cfg(test)]
pub(crate) fn capture_staging_parquet_for_test(directory: &Path) -> Result<Vec<PartitionedFile>> {
    capture_staging_parquet(directory)
        .map(|files| files.into_iter().map(|captured| captured.file).collect())
}

fn capture_paths(directory: &Path, paths: Vec<PathBuf>) -> Result<Vec<LeasedFile>> {
    if paths.is_empty() {
        return Ok(Vec::new());
    }
    // Same directory/filesystem as the sources. Neither this directory nor its
    // `.lease` children match the upload scanner's top-level `.parquet` filter.
    let lease = StagingLease::new(directory)?;
    let mut files = Vec::with_capacity(paths.len());
    for (index, original) in paths.into_iter().enumerate() {
        let leased = lease.path().join(format!("{index}.lease"));
        // Fail closed on unsupported hard links, cross-device links, permission
        // errors and upload cleanup winning this race. RAII rolls back all links.
        fs::hard_link(&original, &leased).map_err(|error| capture_error(&original, error))?;
        let metadata = leased
            .metadata()
            .map_err(|error| capture_error(&original, error))?;
        if !metadata.is_file() {
            return Err(capture_error(&original, "not a regular file"));
        }
        files.push(LeasedFile {
            original,
            leased,
            size: metadata.len(),
            lease: Arc::clone(&lease),
        });
    }
    Ok(files)
}

pub(super) fn validate_staging_parquet(files: Vec<LeasedFile>) -> Result<Vec<CapturedFile>> {
    let mut captured = Vec::with_capacity(files.len());
    for LeasedFile {
        original,
        leased,
        size,
        lease,
    } in files
    {
        // Validate only the retained immutable inode. The uploader uses the
        // same predicate: corrupt footers and zero-row files remain excluded,
        // while stat/open/read failures fail the query clearly.
        if !crate::parseable::Stream::validate_parquet_file(&leased, "query staging")
            .map_err(|error| capture_error(&original, error))?
        {
            fs::remove_file(&leased).map_err(|error| capture_error(&original, error))?;
            continue;
        }
        let location = object_store::path::Path::from_absolute_path(&leased)
            .map_err(|error| capture_error(&original, error))?;
        let file = PartitionedFile::new(location.to_string(), size).with_extension(lease);
        captured.push(CapturedFile { original, file });
    }
    // FileScanConfig and ParquetFileReader retain PartitionedFile extensions.
    // Their Arc clones keep these links alive even if the plan is dropped while
    // its stream is still running. No execution wrapper or custom reader needed.
    Ok(captured)
}

pub(super) fn exclude_uploaded(
    files: Vec<CapturedFile>,
    selected_remote: &HashSet<&str>,
    canonical_path: impl Fn(&Path) -> Result<String>,
) -> Result<Vec<PartitionedFile>> {
    let mut retained = Vec::with_capacity(files.len());
    for captured in files {
        if !selected_remote.contains(canonical_path(&captured.original)?.as_str()) {
            retained.push(captured.file);
        }
    }
    Ok(retained)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::{
        execution::SendableRecordBatchStream,
        physical_plan::{ExecutionPlan, Statistics, collect, execute_stream},
        prelude::{SessionConfig, SessionContext},
    };
    use futures_util::TryStreamExt;
    use parquet::arrow::ArrowWriter;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]))
    }

    fn write_file(directory: &Path, name: &str, values: Vec<i64>) -> PathBuf {
        let path = directory.join(name);
        let batch =
            RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(values))]).unwrap();
        let mut writer =
            ArrowWriter::try_new(fs::File::create(&path).unwrap(), schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path
    }

    fn lease_directories(directory: &Path) -> Vec<PathBuf> {
        fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with(".query-parquet-")
            })
            .collect()
    }

    fn take_files(files: Vec<CapturedFile>) -> Vec<PartitionedFile> {
        files.into_iter().map(|captured| captured.file).collect()
    }

    async fn plan(context: &SessionContext, files: Vec<PartitionedFile>) -> Arc<dyn ExecutionPlan> {
        super::super::parquet_scan_plan(
            schema(),
            datafusion::execution::object_store::ObjectStoreUrl::local_filesystem(),
            vec![files],
            Statistics::new_unknown(&schema()),
            None,
            &[],
            None,
            &context.state(),
            2,
        )
        .await
        .unwrap()
    }

    fn count_sum(batches: &[RecordBatch]) -> (usize, i64) {
        let count = batches.iter().map(RecordBatch::num_rows).sum();
        let sum = batches
            .iter()
            .map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .sum::<i64>()
            })
            .sum();
        (count, sum)
    }

    #[tokio::test]
    async fn staging_lease_survives_original_deletion_and_native_plan_replacement() {
        let directory = tempfile::tempdir().unwrap();
        let first = write_file(directory.path(), "first.parquet", vec![1, 2, 3]);
        let second = write_file(directory.path(), "second.parquet", vec![10, 20]);
        let context = SessionContext::new();
        let unleased_files = [&first, &second]
            .into_iter()
            .map(|path| {
                PartitionedFile::new(
                    object_store::path::Path::from_absolute_path(path)
                        .unwrap()
                        .to_string(),
                    path.metadata().unwrap().len(),
                )
            })
            .collect();
        let old_strategy = plan(&context, unleased_files).await;
        let scan = plan(
            &context,
            take_files(capture_staging_parquet(directory.path()).unwrap()),
        )
        .await;
        assert!(
            scan.name().contains("DataSourceExec"),
            "must remain a native Parquet scan"
        );
        #[allow(deprecated)]
        let replacement = Arc::clone(&scan).with_new_children(vec![]).unwrap();
        drop(scan);
        // Exercise the native optimizer's range splitting as well as replacement.
        let mut config = datafusion::common::config::ConfigOptions::default();
        config.optimizer.repartition_file_min_size = 0;
        let repartitioned = replacement
            .repartitioned(4, &config)
            .unwrap()
            .expect("native Parquet range repartitioning remains available");
        drop(replacement);
        // Deterministically reproduce upload cleanup AFTER planning, BEFORE I/O.
        fs::remove_file(first).unwrap();
        fs::remove_file(second).unwrap();
        assert!(
            collect(old_strategy, context.task_ctx()).await.is_err(),
            "the original mutable-path strategy must reproduce file-not-found"
        );
        assert_eq!(
            count_sum(&collect(repartitioned, context.task_ctx()).await.unwrap()),
            (5, 36)
        );
        assert!(lease_directories(directory.path()).is_empty());
    }

    #[tokio::test]
    async fn staging_lease_stream_outlives_plan_and_cancellation_cleans_up() {
        let directory = tempfile::tempdir().unwrap();
        let original = write_file(directory.path(), "rows.parquet", (1..=100).collect());
        let context = SessionContext::new_with_config(SessionConfig::new().with_batch_size(2));
        let scan = plan(
            &context,
            take_files(capture_staging_parquet(directory.path()).unwrap()),
        )
        .await;
        let stream: SendableRecordBatchStream =
            execute_stream(Arc::clone(&scan), context.task_ctx()).unwrap();
        fs::remove_file(original).unwrap();
        drop(scan);
        assert!(!lease_directories(directory.path()).is_empty());
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(count_sum(&batches), (100, 5050));
        assert!(lease_directories(directory.path()).is_empty());

        let original = write_file(directory.path(), "cancel.parquet", vec![7, 8, 9]);
        let scan = plan(
            &context,
            take_files(capture_staging_parquet(directory.path()).unwrap()),
        )
        .await;
        let stream = execute_stream(Arc::clone(&scan), context.task_ctx()).unwrap();
        fs::remove_file(original).unwrap();
        drop(scan);
        drop(stream); // Cancel without ever polling the stream.
        assert!(lease_directories(directory.path()).is_empty());
    }

    #[test]
    fn staging_lease_unexecuted_captures_are_independent_and_drop_cleanly() {
        let directory = tempfile::tempdir().unwrap();
        let original = write_file(directory.path(), "rows.parquet", vec![1, 2]);
        let first = capture_staging_parquet(directory.path()).unwrap();
        let second = capture_staging_parquet(directory.path()).unwrap();
        assert_eq!(first.len(), 1); // Lease files must never be recaptured.
        assert_eq!(second.len(), 1);
        assert_eq!(lease_directories(directory.path()).len(), 2);
        fs::remove_file(original).unwrap();
        drop(first);
        assert_eq!(lease_directories(directory.path()).len(), 1);
        drop(second);
        assert!(lease_directories(directory.path()).is_empty());
    }

    #[test]
    fn staging_lease_setup_failure_rolls_back_all_links_and_reports_missing_source() {
        let directory = tempfile::tempdir().unwrap();
        let existing = write_file(directory.path(), "rows.parquet", vec![1]);
        let missing = directory.path().join("deleted-after-listing.parquet");
        let result = capture_paths(directory.path(), vec![existing, missing]);
        let error = result
            .err()
            .expect("must fail instead of returning incomplete rows")
            .to_string();
        assert!(error.contains("deleted-after-listing.parquet"), "{error}");
        assert!(lease_directories(directory.path()).is_empty());
    }

    #[test]
    fn staging_lease_listing_error_fails_clearly() {
        let directory = tempfile::tempdir().unwrap();
        let not_directory = directory.path().join("not-a-directory");
        fs::write(&not_directory, b"file").unwrap();
        let error = capture_staging_parquet(&not_directory)
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("not-a-directory"), "{error}");
        assert!(lease_directories(directory.path()).is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn staging_lease_metadata_failure_rolls_back_links() {
        let directory = tempfile::tempdir().unwrap();
        let existing = write_file(directory.path(), "rows.parquet", vec![1]);
        let dangling = directory.path().join("dangling.parquet");
        std::os::unix::fs::symlink(directory.path().join("missing-target"), &dangling).unwrap();
        let error = capture_paths(directory.path(), vec![existing, dangling])
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("dangling.parquet"), "{error}");
        assert!(lease_directories(directory.path()).is_empty());
    }

    #[tokio::test]
    async fn staging_lease_invalid_footer_and_empty_parquet_keep_prior_skip_policy() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(
            directory.path().join("corrupt.parquet"),
            b"invalid footer with enough bytes",
        )
        .unwrap();
        write_file(directory.path(), "empty.parquet", vec![]);
        let valid = write_file(directory.path(), "valid.parquet", vec![1, 2, 3]);
        let leases = lease_staging_parquet(directory.path()).unwrap();
        assert_eq!(leases.len(), 3, "file validation follows lease acquisition");
        fs::remove_file(valid).unwrap();
        let files = validate_staging_parquet(leases).unwrap();
        assert_eq!(files.len(), 1, "only valid, nonempty Parquet is eligible");
        let context = SessionContext::new();
        let scan = plan(&context, take_files(files)).await;
        assert_eq!(
            count_sum(&collect(scan, context.task_ctx()).await.unwrap()),
            (3, 6)
        );
        assert!(lease_directories(directory.path()).is_empty());
    }

    #[tokio::test]
    async fn staging_lease_failed_plan_setup_and_unexecuted_plan_drop_clean_up() {
        let directory = tempfile::tempdir().unwrap();
        write_file(directory.path(), "rows.parquet", vec![1]);
        let context = SessionContext::new();
        let files = take_files(capture_staging_parquet(directory.path()).unwrap());
        let result = super::super::parquet_scan_plan(
            schema(),
            datafusion::execution::object_store::ObjectStoreUrl::local_filesystem(),
            vec![files],
            Statistics::new_unknown(&schema()),
            None,
            &[
                datafusion::prelude::col("p_timestamp").gt(datafusion::prelude::Expr::Literal(
                    datafusion::scalar::ScalarValue::TimestampMillisecond(Some(0), None),
                    None,
                )),
            ],
            None,
            &context.state(),
            2,
        )
        .await;
        assert!(result.is_err());
        assert!(lease_directories(directory.path()).is_empty());
        let scan = plan(
            &context,
            take_files(capture_staging_parquet(directory.path()).unwrap()),
        )
        .await;
        drop(scan);
        assert!(lease_directories(directory.path()).is_empty());
    }

    #[tokio::test]
    async fn staging_lease_remote_overlap_counts_each_file_once_with_canonical_partitions() {
        use crate::storage::object_storage::stream_relative_path;
        use crate::storage::{LocalFS, ObjectStorage};
        use relative_path::RelativePath;
        let directory = tempfile::tempdir().unwrap();
        let staging = directory.path().join("staging");
        let remote = directory.path().join("remote");
        fs::create_dir(&staging).unwrap();
        fs::create_dir(&remote).unwrap();
        let name = "date=2026-10-21.hour=10.minute=30.region=us.service=api.host.data.ULID.parquet";
        let original = write_file(&staging, name, vec![1, 2, 3]);
        let local_only = write_file(&staging, "local.parquet", vec![10, 20]);
        let store = LocalFS::new(remote.clone());
        let tenant = Some("tenant-a".to_owned());
        let custom = Some("region,service".to_owned());
        let canonical = |path: &Path| -> Result<String> {
            let relative = stream_relative_path(
                "events",
                path.file_name().unwrap().to_str().unwrap(),
                &custom,
                &tenant,
            );
            Ok(store.absolute_url(RelativePath::new(&relative)).to_string())
        };
        // Capture before publication, then publish the same immutable bytes.
        let captures = capture_staging_parquet(&staging).unwrap();
        let relative = stream_relative_path("events", name, &custom, &tenant);
        assert_eq!(
            relative,
            format!(
                "tenant-a/events/date=2026-10-21/hour=10/minute=30/region=us/service=api/host.data.ULID.parquet"
            )
        );
        let uploaded = remote.join(&relative);
        fs::create_dir_all(uploaded.parent().unwrap()).unwrap();
        fs::copy(&original, &uploaded).unwrap();
        let uploaded_key = canonical(&original).unwrap();
        let selected = HashSet::from([uploaded_key.as_str()]);
        let mut files = exclude_uploaded(captures, &selected, canonical).unwrap();
        assert_eq!(files.len(), 1);
        files.push(PartitionedFile::new(
            uploaded_key.clone(),
            uploaded.metadata().unwrap().len(),
        ));
        let context = SessionContext::new();
        let scan = plan(&context, files).await;
        fs::remove_file(original).unwrap();
        fs::remove_file(local_only).unwrap();
        assert_eq!(
            count_sum(&collect(scan, context.task_ctx()).await.unwrap()),
            (5, 36)
        );
        assert!(lease_directories(&staging).is_empty());
    }

    // Invoked only by the parent test as a separate process. Killing this
    // process bypasses destructors, exactly like a server crash/SIGKILL.
    #[test]
    fn staging_lease_process_owner_fixture() {
        use std::io::{Read, Write};
        let Some(directory) = std::env::var_os("PARSEABLE_TEST_LEASE_OWNER_DIRECTORY") else {
            return;
        };
        let directory = PathBuf::from(directory);
        let _files = capture_staging_parquet(&directory).unwrap();
        let lease = lease_directories(&directory).pop().unwrap();
        println!("\nLEASE_OWNER:{}", lease.display());
        std::io::stdout().flush().unwrap();
        std::io::stdin().read_exact(&mut [0]).unwrap();
    }

    #[test]
    fn staging_lease_sweeper_preserves_live_process_and_reclaims_killed_owner() {
        use std::io::BufRead;
        use std::process::{Child, Command, Stdio};
        struct ChildGuard(Child);
        impl Drop for ChildGuard {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let directory = tempfile::tempdir().unwrap();
        let original = write_file(directory.path(), "rows.parquet", vec![1, 2, 3]);
        let fixture = concat!(module_path!(), "::staging_lease_process_owner_fixture");
        // libtest names omit the leading crate name returned by module_path!.
        let fixture = fixture.split_once("::").unwrap().1;
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", fixture, "--nocapture", "--test-threads=1"])
            .env("PARSEABLE_TEST_LEASE_OWNER_DIRECTORY", directory.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut child = ChildGuard(child);
        let stdout = child.0.stdout.take().unwrap();
        let lease = std::io::BufReader::new(stdout)
            .lines()
            .take(20)
            .find_map(|line| {
                line.unwrap()
                    .strip_prefix("LEASE_OWNER:")
                    .map(PathBuf::from)
            })
            .expect("child must publish its locked lease before parent sweeps");
        fs::remove_file(original).unwrap();
        reclaim_staging_leases(directory.path());
        assert!(
            lease.join("0.lease").exists(),
            "another process still owns this query"
        );
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        reclaim_staging_leases(directory.path());
        assert!(
            !lease.exists(),
            "OS releases the dead owner's lock; orphan must be reclaimed"
        );
    }

    #[test]
    fn staging_lease_constructor_defers_orphan_cleanup_until_capture() {
        use crate::{cli::Options, metadata::LogStreamMetadata, parseable::Stream};

        let directory = tempfile::tempdir().unwrap();
        let options = Arc::new(Options {
            local_staging_path: directory.path().to_path_buf(),
            ..Default::default()
        });
        let staging = options.local_stream_data_path("deferred", &None);
        fs::create_dir_all(&staging).unwrap();
        let original = write_file(&staging, "rows.parquet", vec![1, 2, 3]);

        // Model a crashed owner: retain its hard-link directory but release the
        // ownership lock. No process or timing assumptions are needed.
        let mut abandoned = Arc::try_unwrap(StagingLease::new(&staging).unwrap()).unwrap();
        fs::hard_link(&original, abandoned.path().join("0.lease")).unwrap();
        let orphan = abandoned.directory.take().unwrap().keep();
        drop(abandoned);
        assert!(orphan.join("0.lease").exists());

        // get_or_create constructs streams under the shared registry write
        // lock. Construction must not scan/remove filesystem entries there.
        let stream = Stream::new(
            options,
            "deferred",
            LogStreamMetadata::default(),
            None,
            &None,
        );
        assert!(
            orphan.join("0.lease").exists(),
            "stream construction must defer orphan cleanup"
        );

        let captured = capture_staging_parquet(&stream.data_path).unwrap();
        assert!(
            !orphan.exists(),
            "the first query capture must reclaim the abandoned lease"
        );
        assert_eq!(captured.len(), 1);
        assert!(original.exists());
        drop(captured);
        assert!(lease_directories(&staging).is_empty());
    }

    #[test]
    fn staging_lease_sweeper_skips_unpublished_setup() {
        let directory = tempfile::tempdir().unwrap();
        let pending = tempfile::Builder::new()
            .prefix(LEASE_PREFIX)
            .tempdir_in(directory.path())
            .unwrap();
        fs::write(pending.path().join(".owner.pending"), []).unwrap();
        reclaim_staging_leases(directory.path());
        assert!(pending.path().exists());
    }

    #[test]
    fn staging_lease_dedup_keeps_files_not_selected_by_remote_pruning_or_limit() {
        let directory = tempfile::tempdir().unwrap();
        write_file(directory.path(), "rows.parquet", vec![1]);
        let files = capture_staging_parquet(directory.path()).unwrap();
        let retained = exclude_uploaded(files, &HashSet::new(), |_| {
            Ok("published-but-not-selected".into())
        })
        .unwrap();
        assert_eq!(retained.len(), 1);
        drop(retained);
        assert!(lease_directories(directory.path()).is_empty());
    }
}
