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
    logical_expr::{col, Expr},
};
use futures_util::{future, stream::FuturesUnordered, Future, TryStreamExt};
use itertools::Itertools;
use object_store::{ObjectMeta, ObjectStore};

use crate::{
    event::DEFAULT_TIMESTAMP_KEY,
    storage::{ObjectStorage, OBJECT_STORE_DATA_GRANULARITY},
    utils::TimePeriod,
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
        storage: Arc<dyn ObjectStorage + Send>,
        client: Arc<dyn ObjectStore>,
        time_filters: &[PartialTimeFilter],
    ) -> Result<Self, DataFusionError> {
        let start_time = time_filters
            .iter()
            .filter_map(|x| match x {
                PartialTimeFilter::Low(Bound::Excluded(x)) => Some(x),
                PartialTimeFilter::Low(Bound::Included(x)) => Some(x),
                _ => None,
            })
            .min()
            .cloned();

        let end_time = time_filters
            .iter()
            .filter_map(|x| match x {
                PartialTimeFilter::High(Bound::Excluded(x)) => Some(x),
                PartialTimeFilter::High(Bound::Included(x)) => Some(x),
                _ => None,
            })
            .max()
            .cloned();

        let Some((start_time, end_time)) = start_time.zip(end_time) else {
            return Err(DataFusionError::NotImplemented(
                "The time predicate is not supported because of possibly querying older data."
                    .to_string(),
            ));
        };

        let prefixes = TimePeriod::new(
            start_time.and_utc(),
            end_time.and_utc(),
            OBJECT_STORE_DATA_GRANULARITY,
        )
        .generate_prefixes();

        let prefixes = prefixes
            .into_iter()
            .map(|entry| {
                let path =
                    relative_path::RelativePathBuf::from(format!("{}/{}", &self.stream, entry));
                storage.absolute_url(path.as_relative_path()).to_string()
            })
            .collect_vec();

        let mut minute_resolve: HashMap<String, Vec<String>> = HashMap::new();
        let mut all_resolve = Vec::new();

        for prefix in prefixes {
            let components = prefix.split_terminator('/');
            if components.last().is_some_and(|x| x.starts_with("minute")) {
                let hour_prefix = &prefix[0..prefix.rfind("minute").expect("minute exists")];
                minute_resolve
                    .entry(hour_prefix.to_owned())
                    .or_insert(Vec::new())
                    .push(prefix);
            } else {
                all_resolve.push(prefix)
            }
        }

        type ResolveFuture = Pin<
            Box<dyn Future<Output = Result<Vec<ObjectMeta>, object_store::Error>> + Send + 'static>,
        >;

        let tasks: FuturesUnordered<ResolveFuture> = FuturesUnordered::new();

        for (listing_prefix, prefix) in minute_resolve {
            let client = Arc::clone(&client);
            tasks.push(Box::pin(async move {
                let mut list = client
                    .list(Some(&object_store::path::Path::from(listing_prefix)))
                    .await?
                    .try_collect::<Vec<_>>()
                    .await?;

                list.retain(|object| {
                    prefix.iter().any(|prefix| {
                        object
                            .location
                            .prefix_matches(&object_store::path::Path::from(prefix.as_ref()))
                    })
                });

                Ok(list)
            }));
        }

        for prefix in all_resolve {
            let client = Arc::clone(&client);
            tasks.push(Box::pin(async move {
                client
                    .list(Some(&object_store::path::Path::from(prefix)))
                    .await?
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(Into::into)
            }));
        }

        let res: Vec<Vec<String>> = tasks
            .and_then(|res| {
                future::ok(
                    res.into_iter()
                        .map(|res| res.location.to_string())
                        .collect_vec(),
                )
            })
            .try_collect()
            .await?;

        let mut res = res.into_iter().flatten().collect_vec();
        res.sort();
        res.reverse();

        Ok(Self {
            stream: self.stream,
            listing: res,
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
        let file_sort_order: Vec<Vec<Expr>>;
        let file_format = ParquetFormat::default().with_enable_pruning(Some(true));
        if let Some(time_partition) = time_partition {
            file_sort_order = vec![vec![col(time_partition).sort(true, false)]];
        } else {
            file_sort_order = vec![vec![col(DEFAULT_TIMESTAMP_KEY).sort(true, false)]];
        }

        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(".parquet")
            .with_file_sort_order(file_sort_order)
            .with_collect_stat(true)
            .with_target_partitions(1);

        let config = ListingTableConfig::new_with_multi_paths(map(self.listing))
            .with_listing_options(listing_options)
            .with_schema(schema);

        let listing_table = Arc::new(ListingTable::try_new(config)?);
        Ok(Some(listing_table))
    }
}
