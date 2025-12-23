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

use std::{ops::Bound, sync::Arc};

use arrow_schema::Schema;
use datafusion::{
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    error::DataFusionError,
    logical_expr::col,
};
use itertools::Itertools;

use crate::{
    OBJECT_STORE_DATA_GRANULARITY, event::DEFAULT_TIMESTAMP_KEY, storage::ObjectStorage,
    utils::time::TimeRange,
};

use super::PartialTimeFilter;

// Listing Table Builder for querying old data
#[derive(Debug, Default)]
pub struct ListingTableBuilder {
    stream: String,
    listing: Vec<String>,
}

impl ListingTableBuilder {
    pub fn new(stream: String) -> Self {
        Self {
            stream,
            ..Self::default()
        }
    }

    pub async fn populate_via_listing(
        self,
        storage: Arc<dyn ObjectStorage>,
        time_filters: &[PartialTimeFilter],
    ) -> Result<Self, DataFusionError> {
        // Extract the minimum start time from the time filters.
        let start_time = time_filters
            .iter()
            .filter_map(|filter| match filter {
                PartialTimeFilter::Low(Bound::Excluded(x))
                | PartialTimeFilter::Low(Bound::Included(x)) => Some(x),
                _ => None,
            })
            .min();

        // Extract the maximum end time from the time filters.
        let end_time = time_filters
            .iter()
            .filter_map(|filter| match filter {
                PartialTimeFilter::High(Bound::Excluded(x))
                | PartialTimeFilter::High(Bound::Included(x)) => Some(x),
                _ => None,
            })
            .max();

        let Some((start_time, end_time)) = start_time.zip(end_time) else {
            return Err(DataFusionError::NotImplemented(
                "The time predicate is not supported because of possibly querying older data."
                    .to_string(),
            ));
        };

        // Generate prefixes for the given time range
        let prefixes = TimeRange::new(start_time.and_utc(), end_time.and_utc())
            .generate_prefixes(OBJECT_STORE_DATA_GRANULARITY);

        // Build all prefixes as relative paths
        let prefixes: Vec<_> = prefixes
            .into_iter()
            .map(|prefix| {
                relative_path::RelativePathBuf::from(format!("{}/{}", &self.stream, prefix))
            })
            .collect();

        // Use storage.list_dirs_relative for all prefixes and flatten results
        let mut listing = Vec::new();
        for prefix in prefixes {
            match storage.list_dirs_relative(&prefix).await {
                Ok(paths) => {
                    listing.extend(paths.into_iter().map(|p| prefix.join(p).to_string()));
                }
                Err(e) => {
                    return Err(DataFusionError::External(Box::new(e)));
                }
            }
        }

        let listing = listing.into_iter().sorted().rev().collect_vec();

        Ok(Self {
            stream: self.stream,
            listing,
        })
    }

    pub fn build(
        self,
        schema: Arc<Schema>,
        map: impl Fn(Vec<String>) -> Vec<ListingTableUrl>,
        time_partition: Option<String>,
    ) -> Result<Option<Arc<ListingTable>>, DataFusionError> {
        if self.listing.is_empty() {
            return Ok(None);
        }

        let file_sort_order = vec![vec![
            time_partition
                .map_or_else(|| col(DEFAULT_TIMESTAMP_KEY), col)
                .sort(true, false),
        ]];
        let file_format = ParquetFormat::default().with_enable_pruning(true);
        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(".parquet")
            .with_file_sort_order(file_sort_order)
            .with_collect_stat(true)
            .with_target_partitions(1);
        let config = ListingTableConfig::new_with_multi_paths(map(self.listing))
            .with_listing_options(listing_options)
            .with_schema(schema);
        let listing_table = ListingTable::try_new(config)?;

        Ok(Some(Arc::new(listing_table)))
    }
}
