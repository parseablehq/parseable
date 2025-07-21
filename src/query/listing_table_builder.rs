/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
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

use std::{collections::HashMap, ops::Bound, pin::Pin, sync::Arc};

use arrow_schema::Schema;
use datafusion::{
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    error::DataFusionError,
    logical_expr::col,
};
use futures_util::{Future, TryStreamExt, stream::FuturesUnordered};
use itertools::Itertools;
use object_store::{ObjectMeta, ObjectStore, path::Path};

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
        client: Arc<dyn ObjectStore>,
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

        // Categorizes prefixes into "minute" and general resolve lists.
        let mut minute_resolve = HashMap::<String, Vec<String>>::new();
        let mut all_resolve = Vec::new();
        for prefix in prefixes {
            let path = relative_path::RelativePathBuf::from(format!("{}/{}", &self.stream, prefix));
            let prefix = storage.absolute_url(path.as_relative_path()).to_string();
            if let Some(pos) = prefix.rfind("minute") {
                let hour_prefix = &prefix[..pos];
                minute_resolve
                    .entry(hour_prefix.to_owned())
                    .or_default()
                    .push(prefix);
            } else {
                all_resolve.push(prefix);
            }
        }

        /// Resolve all prefixes asynchronously and collect the object metadata.
        type ResolveFuture =
            Pin<Box<dyn Future<Output = Result<Vec<ObjectMeta>, object_store::Error>> + Send>>;
        let tasks: FuturesUnordered<ResolveFuture> = FuturesUnordered::new();
        for (listing_prefix, prefixes) in minute_resolve {
            let client = Arc::clone(&client);
            tasks.push(Box::pin(async move {
                let path = Path::from(listing_prefix);
                let mut objects = client.list(Some(&path)).try_collect::<Vec<_>>().await?;

                objects.retain(|obj| {
                    prefixes.iter().any(|prefix| {
                        obj.location
                            .prefix_matches(&object_store::path::Path::from(prefix.as_ref()))
                    })
                });

                Ok(objects)
            }));
        }

        for prefix in all_resolve {
            let client = Arc::clone(&client);
            tasks.push(Box::pin(async move {
                client
                    .list(Some(&object_store::path::Path::from(prefix)))
                    .try_collect::<Vec<_>>()
                    .await
            }));
        }

        let listing = tasks
            .try_collect::<Vec<Vec<ObjectMeta>>>()
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?
            .into_iter()
            .flat_map(|res| {
                res.into_iter()
                    .map(|obj| obj.location.to_string())
                    .collect::<Vec<String>>()
            })
            .sorted()
            .rev()
            .collect_vec();

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
