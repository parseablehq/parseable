/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};

use dashmap::DashMap;
use parquet::file::{
    metadata::{ParquetMetaData, RowGroupMetaData, SortingColumn},
    reader::FileReader,
    statistics::Statistics,
};

use crate::metastore::metastore_traits::MetastoreObject;

use super::column::Column;

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde_repr::Serialize_repr,
    serde_repr::Deserialize_repr,
)]
#[repr(u8)]
pub enum SortOrder {
    AscNullsFirst = 0,
    AscNullsLast,
    DescNullsLast,
    #[default]
    DescNullsFirst,
}

pub type SortInfo = (String, SortOrder);
pub const CURRENT_MANIFEST_VERSION: &str = "v1";

/// Leading bytes of a zstd frame, used to tell a compressed manifest from a
/// plain JSON one. Manifests written before compression was introduced are
/// still read as-is, and the file name is identical either way, so this is the
/// only thing distinguishing the two encodings.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Compression level for manifests. Manifest JSON repeats the same column names
/// across thousands of file entries, so it compresses roughly 15-25x even at
/// this cheap level; higher levels cost write time for very little extra.
const ZSTD_LEVEL: i32 = 3;

/// Cap on manifests being decoded at once by [`decode_manifest_blocking`].
///
/// Decoding is CPU bound, so there is nothing to gain past the core count, and
/// each in-flight decode holds a fully decompressed manifest in memory — which
/// is two orders of magnitude larger than the compressed form.
static DECODE_PERMITS: std::sync::LazyLock<std::sync::Arc<tokio::sync::Semaphore>> =
    std::sync::LazyLock::new(|| std::sync::Arc::new(tokio::sync::Semaphore::new(num_cpus::get())));

#[derive(Debug, thiserror::Error)]
pub enum ManifestCodecError {
    #[error("failed to compress manifest: {0}")]
    Compress(std::io::Error),

    #[error("manifest codec task failed: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("failed to decompress manifest: {0}")]
    Decompress(std::io::Error),

    #[error("failed to serialize manifest: {0}")]
    Serialize(serde_json::Error),

    #[error("failed to parse manifest: {0}")]
    Parse(serde_json::Error),
}

/// Encode a manifest for storage. This and [`decode_manifest`] are the only
/// places manifest bytes are produced or interpreted; going around them would
/// write a manifest nothing else can read.
pub fn encode_manifest(manifest: &Manifest) -> Result<bytes::Bytes, ManifestCodecError> {
    let json = serde_json::to_vec(manifest).map_err(ManifestCodecError::Serialize)?;
    let compressed =
        zstd::encode_all(json.as_slice(), ZSTD_LEVEL).map_err(ManifestCodecError::Compress)?;
    Ok(bytes::Bytes::from(compressed))
}

/// Encode a manifest on the blocking pool so JSON serialization and zstd
/// compression for large daily manifests do not stall async Tokio workers.
pub async fn encode_manifest_blocking(
    manifest: Manifest,
) -> Result<bytes::Bytes, ManifestCodecError> {
    tokio::task::spawn_blocking(move || encode_manifest(&manifest)).await?
}

/// Decode a manifest, accepting both the compressed and the plain JSON form.
///
/// Existing manifests are never rewritten, so uncompressed ones stay readable
/// indefinitely rather than being migrated.
pub fn decode_manifest(bytes: &[u8]) -> Result<Manifest, ManifestCodecError> {
    if bytes.starts_with(&ZSTD_MAGIC) {
        let json = zstd::decode_all(bytes).map_err(ManifestCodecError::Decompress)?;
        serde_json::from_slice(&json).map_err(ManifestCodecError::Parse)
    } else {
        serde_json::from_slice(bytes).map_err(ManifestCodecError::Parse)
    }
}

/// Decode a manifest on the blocking pool.
///
/// Decompressing and parsing a manifest is hundreds of milliseconds of pure
/// CPU. Run inline on an async task it serialises against every other decode
/// that task drives — callers fetch manifests with `buffered(..)`, which gives
/// concurrent I/O but polls all those futures from one task, so the decodes
/// queue up behind each other and each pending GET appears to take longer and
/// longer. Handing the work to the blocking pool lets them actually run in
/// parallel.
pub async fn decode_manifest_blocking(bytes: bytes::Bytes) -> Result<Manifest, ManifestCodecError> {
    // Owned permit moved into the task: dropping this future cancels the await
    // but not the blocking work, so the permit has to outlive it to keep
    // detached decodes counted.
    let permit = DECODE_PERMITS
        .clone()
        .acquire_owned()
        .await
        .expect("decode semaphore is never closed");

    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        decode_manifest(&bytes)
    })
    .await?
}

/// An entry in a manifest which points to a single file.
/// Additionally, it is meant to store the statistics for the file it
/// points to. Used for pruning file at planning level.
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct File {
    pub file_path: String,
    pub num_rows: u64,
    pub file_size: u64,
    pub ingestion_size: u64,
    pub columns: Vec<Column>,
    pub sort_order_id: Vec<SortInfo>,
}

/// A manifest file composed of multiple file entries.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Manifest {
    pub version: String,
    pub files: Vec<File>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            version: CURRENT_MANIFEST_VERSION.to_string(),
            files: Vec::default(),
        }
    }
}

impl Manifest {
    pub fn apply_change(&mut self, change: File) {
        if let Some(pos) = self
            .files
            .iter()
            .position(|file| file.file_path == change.file_path)
        {
            self.files[pos] = change
        } else {
            self.files.push(change)
        }
    }

    /// Applies multiple file additions/updates in `O(F + B)` instead of `O(F * B)`
    /// by indexing existing file paths once for the batch.
    pub fn apply_changes<I>(&mut self, changes: I)
    where
        I: IntoIterator<Item = File>,
    {
        let mut iter = changes.into_iter();
        let Some(first) = iter.next() else {
            return;
        };
        let Some(second) = iter.next() else {
            self.apply_change(first);
            return;
        };

        let (lower_hint, _) = iter.size_hint();
        let mut index: HashMap<String, usize> = HashMap::with_capacity(
            self.files
                .len()
                .saturating_add(2)
                .saturating_add(lower_hint),
        );
        for (pos, file) in self.files.iter().enumerate() {
            // Preserve `Iterator::position` first-match semantics if duplicates exist.
            index.entry(file.file_path.clone()).or_insert(pos);
        }

        self.files.reserve(2usize.saturating_add(lower_hint));
        for change in [first, second].into_iter().chain(iter) {
            if let Some(&pos) = index.get(&change.file_path) {
                self.files[pos] = change;
            } else {
                let pos = self.files.len();
                index.insert(change.file_path.clone(), pos);
                self.files.push(change);
            }
        }
    }
}

impl MetastoreObject for Manifest {
    fn get_object_path(&self) -> String {
        unimplemented!()
    }

    fn get_object_id(&self) -> String {
        unimplemented!()
    }
}

const MAX_PARQUET_METADATA_CACHE_ENTRIES: usize = 2048;
const PARQUET_METADATA_CACHE_TTL: Duration = Duration::from_secs(900);

struct CachedParquetMetaData {
    metadata: Arc<ParquetMetaData>,
    inserted_at: Instant,
}

static PARQUET_METADATA_CACHE: LazyLock<DashMap<PathBuf, CachedParquetMetaData>> =
    LazyLock::new(DashMap::new);

/// Caches the `ParquetMetaData` returned by `ArrowWriter::close()` for a newly
/// staged Parquet file so upload manifest creation can reuse it in memory
/// without re-opening the file and re-parsing its footer.
pub fn cache_parquet_metadata(path: &Path, metadata: Arc<ParquetMetaData>) {
    let now = Instant::now();
    if !PARQUET_METADATA_CACHE.contains_key(path)
        && PARQUET_METADATA_CACHE.len() >= MAX_PARQUET_METADATA_CACHE_ENTRIES
    {
        PARQUET_METADATA_CACHE
            .retain(|_, entry| now.duration_since(entry.inserted_at) < PARQUET_METADATA_CACHE_TTL);
        let mut attempts = 0usize;
        while PARQUET_METADATA_CACHE.len() >= MAX_PARQUET_METADATA_CACHE_ENTRIES && attempts < 8 {
            attempts += 1;
            let mut oldest: Option<(Instant, PathBuf)> = None;
            for entry in PARQUET_METADATA_CACHE.iter() {
                let ts = entry.value().inserted_at;
                if oldest.as_ref().is_none_or(|(min_ts, _)| ts < *min_ts) {
                    oldest = Some((ts, entry.key().clone()));
                }
            }
            let Some((_, key)) = oldest else {
                break;
            };
            PARQUET_METADATA_CACHE.remove(&key);
        }
    }
    PARQUET_METADATA_CACHE.insert(
        path.to_path_buf(),
        CachedParquetMetaData {
            metadata,
            inserted_at: now,
        },
    );
}

/// Takes and removes cached `ParquetMetaData` for a staged Parquet path, if present and fresh.
pub fn take_parquet_metadata(path: &Path) -> Option<Arc<ParquetMetaData>> {
    let (_, entry) = PARQUET_METADATA_CACHE.remove(path)?;
    if entry.inserted_at.elapsed() < PARQUET_METADATA_CACHE_TTL {
        Some(entry.metadata)
    } else {
        None
    }
}

/// Removes any cached `ParquetMetaData` for `path` (e.g. when a staged file is retired).
pub fn evict_parquet_metadata(path: &Path) {
    PARQUET_METADATA_CACHE.remove(path);
}

/// Builds a manifest `File` entry directly from in-memory `ParquetMetaData` and
/// the known on-disk `file_size`, avoiding any disk I/O or footer re-parse.
pub fn create_from_parquet_metadata(
    object_store_path: String,
    file_size: u64,
    metadata: &ParquetMetaData,
) -> File {
    let file_meta = metadata.file_metadata();
    let row_groups = metadata.row_groups();

    let mut manifest_file = File {
        file_path: object_store_path,
        file_size,
        num_rows: file_meta.num_rows() as u64,
        ingestion_size: row_groups
            .iter()
            .fold(0, |acc, x| acc + x.total_byte_size() as u64),
        columns: column_statistics(row_groups).into_values().collect(),
        sort_order_id: Vec::new(),
    };

    let mut sort_orders = sort_order(row_groups);
    if let Some(last_sort_order) = sort_orders.pop()
        && sort_orders
            .into_iter()
            .all(|sort_order| sort_order == last_sort_order)
    {
        manifest_file.sort_order_id = last_sort_order;
    }

    manifest_file
}

pub fn create_from_parquet_file(
    object_store_path: String,
    fs_file_path: &std::path::Path,
) -> anyhow::Result<File> {
    if let Some(metadata) = take_parquet_metadata(fs_file_path) {
        let file_size = std::fs::metadata(fs_file_path)?.len();
        return Ok(create_from_parquet_metadata(
            object_store_path,
            file_size,
            &metadata,
        ));
    }

    let file = std::fs::File::open(fs_file_path)?;
    let file_size = file.metadata()?.len();
    let file = parquet::file::serialized_reader::SerializedFileReader::new(file)?;
    Ok(create_from_parquet_metadata(
        object_store_path,
        file_size,
        file.metadata(),
    ))
}

fn sort_order(
    row_groups: &[parquet::file::metadata::RowGroupMetaData],
) -> Vec<Vec<(String, SortOrder)>> {
    let mut sort_orders = Vec::new();
    for row_group in row_groups {
        // Keep an empty claim for unknown groups so file-level comparison
        // still includes every row group.
        let sort_order = row_group
            .sorting_columns()
            .map(Vec::as_slice)
            .unwrap_or_default();
        let sort_order = sort_order
            .iter()
            .map(|sort_order| {
                let SortingColumn {
                    column_idx,
                    descending,
                    nulls_first,
                } = sort_order;
                let col = row_group
                    .columns()
                    .get(usize::try_from(*column_idx).ok()?)?
                    .column_descr()
                    .path()
                    .string();
                let sort_info = match (descending, nulls_first) {
                    (true, true) => SortOrder::DescNullsFirst,
                    (true, false) => SortOrder::DescNullsLast,
                    (false, true) => SortOrder::AscNullsFirst,
                    (false, false) => SortOrder::AscNullsLast,
                };

                Some((col, sort_info))
            })
            // An invalid key invalidates the whole claim, including any
            // valid prefix or suffix of the sorting columns.
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();

        sort_orders.push(sort_order);
    }
    sort_orders
}

fn column_statistics(row_groups: &[RowGroupMetaData]) -> HashMap<String, Column> {
    let mut columns: HashMap<String, Column> = HashMap::new();
    for row_group in row_groups {
        for col in row_group.columns() {
            let col_name = col.column_descr().path().string();
            if let Some(entry) = columns.get_mut(&col_name) {
                entry.compressed_size += col.compressed_size() as u64;
                entry.uncompressed_size += col.uncompressed_size() as u64;
                entry.null_count = entry
                    .null_count
                    .zip(col.statistics().and_then(Statistics::null_count_opt))
                    .and_then(|(current, other)| current.checked_add(other));
                if let Some(other) = col.statistics().and_then(|stats| stats.try_into().ok()) {
                    entry.stats = entry.stats.clone().and_then(|this| this.update(other));
                }
            } else {
                columns.insert(
                    col_name.clone(),
                    Column {
                        name: col_name,
                        stats: col.statistics().and_then(|stats| stats.try_into().ok()),
                        null_count: col.statistics().and_then(Statistics::null_count_opt),
                        uncompressed_size: col.uncompressed_size() as u64,
                        compressed_size: col.compressed_size() as u64,
                    },
                );
            }
        }
    }
    columns
}

#[cfg(test)]
mod codec_tests {
    use super::*;

    fn sample_manifest(file_count: usize) -> Manifest {
        let files = (0..file_count)
            .map(|i| File {
                file_path: format!("stream/date=2026-07-29/hour=00/file-{i}.parquet"),
                num_rows: 262_144,
                file_size: 1_048_576,
                ingestion_size: 2_097_152,
                columns: (0..64)
                    .map(|c| Column {
                        name: format!("some_reasonably_long_column_name_{c}"),
                        stats: None,
                        null_count: None,
                        uncompressed_size: 1024,
                        compressed_size: 512,
                    })
                    .collect(),
                sort_order_id: Vec::new(),
            })
            .collect();
        Manifest {
            version: CURRENT_MANIFEST_VERSION.to_string(),
            files,
        }
    }

    #[test]
    fn round_trips() {
        let manifest = sample_manifest(4);
        let decoded = decode_manifest(&encode_manifest(&manifest).unwrap()).unwrap();

        assert_eq!(decoded.version, manifest.version);
        assert_eq!(decoded.files.len(), manifest.files.len());
        assert_eq!(decoded.files[0].file_path, manifest.files[0].file_path);
        assert_eq!(
            decoded.files[0].columns.len(),
            manifest.files[0].columns.len()
        );
    }

    #[test]
    fn encodes_as_zstd() {
        let encoded = encode_manifest(&sample_manifest(1)).unwrap();
        assert!(
            encoded.starts_with(&ZSTD_MAGIC),
            "manifest was not compressed"
        );
    }

    /// Manifests written before compression are never rewritten, so plain JSON
    /// has to stay readable forever.
    #[test]
    fn reads_uncompressed_manifests() {
        let manifest = sample_manifest(3);
        let plain = serde_json::to_vec(&manifest).unwrap();
        assert!(!plain.starts_with(&ZSTD_MAGIC));

        let decoded = decode_manifest(&plain).unwrap();
        assert_eq!(decoded.files.len(), 3);
        assert_eq!(decoded.files[2].file_path, manifest.files[2].file_path);
    }

    /// Guards against the codec silently degrading to a no-op: this shape is
    /// the whole reason compression is worth doing.
    #[test]
    fn compresses_repetitive_manifests_substantially() {
        let manifest = sample_manifest(64);
        let plain_len = serde_json::to_vec(&manifest).unwrap().len();
        let encoded_len = encode_manifest(&manifest).unwrap().len();

        assert!(
            encoded_len * 10 < plain_len,
            "expected >10x compression, got {plain_len} -> {encoded_len}"
        );
    }

    #[test]
    fn rejects_truncated_compressed_input() {
        let encoded = encode_manifest(&sample_manifest(8)).unwrap();
        let truncated = &encoded[..encoded.len() / 2];

        assert!(matches!(
            decode_manifest(truncated),
            Err(ManifestCodecError::Decompress(_))
        ));
    }

    #[test]
    fn rejects_garbage_input() {
        assert!(matches!(
            decode_manifest(b"not json, not zstd"),
            Err(ManifestCodecError::Parse(_))
        ));
    }
}

#[cfg(test)]
mod sorting_tests {
    use std::{io::Write, sync::Arc};

    use parquet::{
        data_type::Int64Type,
        file::{
            metadata::{ParquetMetaData, ParquetMetaDataWriter},
            properties::WriterProperties,
            serialized_reader::SerializedFileReader,
            writer::{SerializedFileWriter, TrackedWrite},
        },
        schema::parser::parse_message_type,
    };
    use tempfile::NamedTempFile;

    use super::*;

    fn sorting_column(column_idx: i32, descending: bool, nulls_first: bool) -> SortingColumn {
        SortingColumn {
            column_idx,
            descending,
            nulls_first,
        }
    }

    // Write real column pages, then replace only the footer's per-row-group
    // sorting declarations (WriterProperties applies one declaration to all groups).
    fn parquet_with_sorting(sorting: Vec<Option<Vec<SortingColumn>>>) -> NamedTempFile {
        let schema = Arc::new(
            parse_message_type(
                "message test { REQUIRED INT64 first; \
                 REQUIRED GROUP nested { REQUIRED INT64 leaf; } }",
            )
            .unwrap(),
        );
        let mut bytes = Vec::new();
        let mut writer = SerializedFileWriter::new(
            &mut bytes,
            schema,
            Arc::new(WriterProperties::builder().build()),
        )
        .unwrap();
        for _ in &sorting {
            let mut row_group = writer.next_row_group().unwrap();
            while let Some(mut column) = row_group.next_column().unwrap() {
                column
                    .typed::<Int64Type>()
                    .write_batch(&[1, 2], None, None)
                    .unwrap();
                column.close().unwrap();
            }
            row_group.close().unwrap();
        }
        let metadata = writer.close().unwrap();
        let row_groups = metadata
            .row_groups()
            .iter()
            .cloned()
            .zip(sorting.iter().cloned())
            .map(|(group, sorting)| {
                group
                    .into_builder()
                    .set_sorting_columns(sorting)
                    .build()
                    .unwrap()
            })
            .collect();
        let metadata = ParquetMetaData::new(metadata.file_metadata().clone(), row_groups);
        let footer_len =
            u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap());
        let file = NamedTempFile::new().unwrap();
        let mut output = TrackedWrite::new(file.reopen().unwrap());
        output
            .write_all(&bytes[..bytes.len() - 8 - footer_len as usize])
            .unwrap();
        ParquetMetaDataWriter::new_with_tracked(output, &metadata)
            .finish()
            .unwrap();

        let reader = SerializedFileReader::new(file.reopen().unwrap()).unwrap();
        assert_eq!(
            reader
                .metadata()
                .row_groups()
                .iter()
                .map(|group| group.sorting_columns().cloned())
                .collect::<Vec<_>>(),
            sorting,
        );
        let rows = reader
            .get_row_iter(None)
            .unwrap()
            .collect::<parquet::errors::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(rows.len(), sorting.len() * 2);
        file
    }

    fn manifest_with_sorting(sorting: Vec<Option<Vec<SortingColumn>>>) -> File {
        let row_count = sorting.len() as u64 * 2;
        let file = parquet_with_sorting(sorting);
        let manifest = create_from_parquet_file("object.parquet".to_owned(), file.path()).unwrap();
        assert_eq!(manifest.file_path, "object.parquet");
        assert_eq!(manifest.num_rows, row_count);
        assert_eq!(manifest.file_size, file.as_file().metadata().unwrap().len());
        assert!(manifest.ingestion_size > 0);
        let mut names = manifest
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        names.sort_unstable();
        assert_eq!(names, ["first", "nested.leaf"]);
        manifest
    }

    #[test]
    fn sort_order_preserves_row_group_alignment() {
        let valid = vec![sorting_column(1, true, false)];
        let file = parquet_with_sorting(vec![
            None,
            Some(valid.clone()),
            Some(vec![]),
            None,
            Some(valid),
        ]);
        let reader = SerializedFileReader::new(file.reopen().unwrap()).unwrap();
        let claim = vec![("nested.leaf".to_owned(), SortOrder::DescNullsLast)];
        assert_eq!(
            sort_order(reader.metadata().row_groups()),
            vec![vec![], claim.clone(), vec![], vec![], claim]
        );
    }

    #[test]
    fn manifest_from_parquet_without_sorting_columns() {
        for sorting in [None, Some(vec![])] {
            assert!(
                manifest_with_sorting(vec![sorting.clone(), sorting])
                    .sort_order_id
                    .is_empty()
            );
        }
    }

    #[test]
    fn manifest_from_parquet_mixed_sorting_columns_has_no_claim() {
        let valid = Some(vec![sorting_column(1, true, false)]);
        for missing in [None, Some(vec![])] {
            // Unknown groups at either end must participate in the file-level comparison.
            for sorting in [
                vec![missing.clone(), valid.clone()],
                vec![valid.clone(), missing],
            ] {
                assert!(manifest_with_sorting(sorting).sort_order_id.is_empty());
            }
        }
        let different = Some(vec![sorting_column(1, false, false)]);
        assert!(
            manifest_with_sorting(vec![valid, different])
                .sort_order_id
                .is_empty()
        );
    }

    #[test]
    fn manifest_from_parquet_preserves_sort_flags_and_nested_paths() {
        for (descending, nulls_first, expected) in [
            (false, false, SortOrder::AscNullsLast),
            (false, true, SortOrder::AscNullsFirst),
            (true, false, SortOrder::DescNullsLast),
            (true, true, SortOrder::DescNullsFirst),
        ] {
            let sorting = Some(vec![
                sorting_column(1, descending, nulls_first),
                sorting_column(0, false, true),
            ]);
            assert_eq!(
                manifest_with_sorting(vec![sorting.clone(), sorting]).sort_order_id,
                vec![
                    ("nested.leaf".to_owned(), expected),
                    ("first".to_owned(), SortOrder::AscNullsFirst)
                ],
            );
        }
    }

    #[test]
    fn sort_order_invalid_index_discards_entire_row_group() {
        let valid = sorting_column(0, false, true);
        for index in [-1, 2, i32::MIN, i32::MAX] {
            for position in 0..=2 {
                let mut sorting = vec![valid.clone(), sorting_column(1, true, false)];
                sorting.insert(position, sorting_column(index, true, false));
                let file = parquet_with_sorting(vec![Some(sorting), Some(vec![valid.clone()])]);
                let reader = SerializedFileReader::new(file.reopen().unwrap()).unwrap();
                assert_eq!(
                    sort_order(reader.metadata().row_groups()),
                    vec![vec![], vec![("first".to_owned(), SortOrder::AscNullsFirst)]]
                );
                assert!(
                    create_from_parquet_file("object.parquet".to_owned(), file.path())
                        .unwrap()
                        .sort_order_id
                        .is_empty()
                );
            }
        }
    }
}

#[cfg(test)]
mod handoff_and_batch_tests {
    use std::sync::Arc;

    use arrow_array::{BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field as ArrowField, Schema};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use tempfile::NamedTempFile;

    use super::*;

    fn test_file(path: &str, num_rows: u64) -> File {
        File {
            file_path: path.to_owned(),
            num_rows,
            file_size: num_rows * 16,
            ingestion_size: num_rows * 32,
            columns: Vec::new(),
            sort_order_id: Vec::new(),
        }
    }

    #[test]
    fn cached_parquet_metadata_matches_disk_parse() {
        let schema = Arc::new(Schema::new(vec![
            ArrowField::new("ts", DataType::Int64, false),
            ArrowField::new("msg", DataType::Utf8, true),
            ArrowField::new("score", DataType::Float64, true),
            ArrowField::new("ok", DataType::Boolean, true),
        ]));
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![100, 200, 300, 400])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    None,
                    Some("c"),
                    Some("b"),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(1.5),
                    Some(2.5),
                    None,
                    Some(0.5),
                ])),
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    None,
                    Some(true),
                ])),
            ],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![500, 600, 700])),
                Arc::new(StringArray::from(vec![Some("z"), Some("m"), None])),
                Arc::new(Float64Array::from(vec![Some(9.25), None, Some(-3.0)])),
                Arc::new(BooleanArray::from(vec![
                    Some(false),
                    Some(false),
                    Some(true),
                ])),
            ],
        )
        .unwrap();

        let file = NamedTempFile::new().unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(4))
            .set_sorting_columns(Some(vec![SortingColumn {
                column_idx: 0,
                descending: false,
                nulls_first: false,
            }]))
            .build();
        let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, Some(props)).unwrap();
        writer.write(&batch1).unwrap();
        writer.write(&batch2).unwrap();
        let writer_metadata = Arc::new(writer.close().unwrap());

        let disk_manifest =
            create_from_parquet_file("stream/part.parquet".to_owned(), file.path()).unwrap();

        cache_parquet_metadata(file.path(), writer_metadata);
        let cached_manifest =
            create_from_parquet_file("stream/part.parquet".to_owned(), file.path()).unwrap();

        // Cache entry is consumed on first read.
        assert!(take_parquet_metadata(file.path()).is_none());

        assert_eq!(disk_manifest.file_path, cached_manifest.file_path);
        assert_eq!(disk_manifest.num_rows, cached_manifest.num_rows);
        assert_eq!(disk_manifest.file_size, cached_manifest.file_size);
        assert_eq!(disk_manifest.ingestion_size, cached_manifest.ingestion_size);
        assert_eq!(disk_manifest.sort_order_id, cached_manifest.sort_order_id);

        let mut disk_cols = disk_manifest.columns;
        let mut cached_cols = cached_manifest.columns;
        disk_cols.sort_by(|a, b| a.name.cmp(&b.name));
        cached_cols.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(
            serde_json::to_value(&disk_cols).unwrap(),
            serde_json::to_value(&cached_cols).unwrap()
        );
    }

    #[test]
    fn manifest_apply_changes_matches_sequential_apply_change() {
        let initial = Manifest {
            version: CURRENT_MANIFEST_VERSION.to_owned(),
            files: (0..25)
                .map(|i| test_file(&format!("stream/file-{i}.parquet"), i as u64 + 1))
                .collect(),
        };

        // Empty and single-item batches.
        let mut empty = initial.clone();
        empty.apply_changes(Vec::new());
        assert_eq!(empty.files.len(), initial.files.len());

        let mut single_batch = initial.clone();
        let mut single_seq = initial.clone();
        single_batch.apply_changes(vec![test_file("stream/file-5.parquet", 999)]);
        single_seq.apply_change(test_file("stream/file-5.parquet", 999));
        assert_eq!(
            serde_json::to_value(&single_batch).unwrap(),
            serde_json::to_value(&single_seq).unwrap()
        );

        // Multi-item batch with replacements, new appends, and duplicate keys within the batch.
        let batch = vec![
            test_file("stream/file-2.parquet", 200),
            test_file("stream/file-25.parquet", 250),
            test_file("stream/file-10.parquet", 1000),
            test_file("stream/file-25.parquet", 251),
            test_file("stream/file-26.parquet", 260),
            test_file("stream/file-2.parquet", 201),
        ];

        let mut batched = initial.clone();
        batched.apply_changes(batch.clone());

        let mut sequential = initial;
        for change in batch {
            sequential.apply_change(change);
        }

        assert_eq!(
            serde_json::to_value(&batched).unwrap(),
            serde_json::to_value(&sequential).unwrap()
        );
    }

    #[tokio::test]
    async fn encode_manifest_blocking_round_trips() {
        let manifest = Manifest {
            version: CURRENT_MANIFEST_VERSION.to_owned(),
            files: vec![
                test_file("stream/file-0.parquet", 10),
                test_file("stream/file-1.parquet", 20),
            ],
        };
        let encoded = encode_manifest_blocking(manifest.clone()).await.unwrap();
        let decoded = decode_manifest_blocking(encoded).await.unwrap();
        assert_eq!(
            serde_json::to_value(&decoded).unwrap(),
            serde_json::to_value(&manifest).unwrap()
        );
    }
}
