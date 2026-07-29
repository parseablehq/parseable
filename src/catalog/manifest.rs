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

use std::collections::HashMap;

use itertools::Itertools;
use parquet::file::{
    metadata::{RowGroupMetaData, SortingColumn},
    reader::FileReader,
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

#[derive(Debug, thiserror::Error)]
pub enum ManifestCodecError {
    #[error("failed to compress manifest: {0}")]
    Compress(std::io::Error),

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
}

impl MetastoreObject for Manifest {
    fn get_object_path(&self) -> String {
        unimplemented!()
    }

    fn get_object_id(&self) -> String {
        unimplemented!()
    }
}

pub fn create_from_parquet_file(
    object_store_path: String,
    fs_file_path: &std::path::Path,
) -> anyhow::Result<File> {
    let mut manifest_file = File {
        file_path: object_store_path,
        ..File::default()
    };

    let file = std::fs::File::open(fs_file_path)?;
    manifest_file.file_size = file.metadata()?.len();

    let file = parquet::file::serialized_reader::SerializedFileReader::new(file)?;
    let file_meta = file.metadata().file_metadata();
    let row_groups = file.metadata().row_groups();

    manifest_file.num_rows = file_meta.num_rows() as u64;
    manifest_file.ingestion_size = row_groups
        .iter()
        .fold(0, |acc, x| acc + x.total_byte_size() as u64);

    let columns = column_statistics(row_groups);
    manifest_file.columns = columns.into_values().collect();
    let mut sort_orders = sort_order(row_groups);
    if let Some(last_sort_order) = sort_orders.pop()
        && sort_orders
            .into_iter()
            .all(|sort_order| sort_order == last_sort_order)
    {
        manifest_file.sort_order_id = last_sort_order;
    }

    Ok(manifest_file)
}

fn sort_order(
    row_groups: &[parquet::file::metadata::RowGroupMetaData],
) -> Vec<Vec<(String, SortOrder)>> {
    let mut sort_orders = Vec::new();
    for row_group in row_groups {
        let sort_order = row_group.sorting_columns().unwrap();
        let sort_order = sort_order
            .iter()
            .map(|sort_order| {
                let SortingColumn {
                    column_idx,
                    descending,
                    nulls_first,
                } = sort_order;
                let col = row_group
                    .column(*column_idx as usize)
                    .column_descr()
                    .path()
                    .string();
                let sort_info = match (descending, nulls_first) {
                    (true, true) => SortOrder::DescNullsFirst,
                    (true, false) => SortOrder::DescNullsLast,
                    (false, true) => SortOrder::AscNullsFirst,
                    (false, false) => SortOrder::AscNullsLast,
                };

                (col, sort_info)
            })
            .collect_vec();

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
                if let Some(other) = col.statistics().and_then(|stats| stats.try_into().ok()) {
                    entry.stats = entry.stats.clone().and_then(|this| this.update(other));
                }
            } else {
                columns.insert(
                    col_name.clone(),
                    Column {
                        name: col_name,
                        stats: col.statistics().and_then(|stats| stats.try_into().ok()),
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
        assert_eq!(decoded.files[0].columns.len(), manifest.files[0].columns.len());
    }

    #[test]
    fn encodes_as_zstd() {
        let encoded = encode_manifest(&sample_manifest(1)).unwrap();
        assert!(encoded.starts_with(&ZSTD_MAGIC), "manifest was not compressed");
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
