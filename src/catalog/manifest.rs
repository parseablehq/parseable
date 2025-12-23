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
                    entry.stats = entry.stats.clone().map(|this| this.update(other));
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
