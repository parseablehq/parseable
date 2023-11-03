/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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

mod filter_optimizer;
mod table_provider;

use chrono::{DateTime, Utc};
use chrono::{TimeZone, Timelike};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::execution::context::SessionState;
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::*;
use itertools::Itertools;
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use sysinfo::{System, SystemExt};

use crate::event::DEFAULT_TIMESTAMP_KEY;
use crate::option::CONFIG;
use crate::storage::{ObjectStorage, OBJECT_STORE_DATA_GRANULARITY};
use crate::storage::{ObjectStorageError, StorageDir};
use crate::utils::TimePeriod;
use crate::validator;

use self::error::{ExecuteError, ParseError};
use self::filter_optimizer::FilterOptimizerRule;
use self::table_provider::QueryTableProvider;

type Key = &'static str;
fn get_value(value: &Value, key: Key) -> Result<&str, Key> {
    value.get(key).and_then(|value| value.as_str()).ok_or(key)
}

// Query holds all values relevant to a query for a single log stream
pub struct Query {
    pub query: String,
    pub stream_name: String,
    pub schema: Arc<Schema>,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub filter_tag: Option<Vec<String>>,
    pub fill_null: bool,
}

impl Query {
    // parse_query parses the SQL query and returns the log stream name on which
    // this query is supposed to be executed
    pub fn parse(query_json: Value) -> Result<Query, ParseError> {
        // retrieve query, start and end time information from payload.
        let query = get_value(&query_json, "query")?;
        let start_time = get_value(&query_json, "startTime")?;
        let end_time = get_value(&query_json, "endTime")?;

        Ok(validator::query(query, start_time, end_time)?)
    }

    /// Return prefixes, each per day/hour/minutes as necessary
    fn _get_prefixes(&self) -> Vec<String> {
        TimePeriod::new(self.start, self.end, OBJECT_STORE_DATA_GRANULARITY).generate_prefixes()
    }

    pub fn get_prefixes(&self) -> Vec<String> {
        self._get_prefixes()
            .into_iter()
            .map(|key| format!("{}/{}", self.stream_name, key))
            // latest first
            .rev()
            .collect()
    }

    // create session context for this query
    fn create_session_context(&self) -> SessionContext {
        let config = SessionConfig::default();
        let runtime_config = CONFIG
            .storage()
            .get_datafusion_runtime()
            .with_disk_manager(DiskManagerConfig::NewOs);

        let (pool_size, fraction) = match CONFIG.parseable.query_memory_pool_size {
            Some(size) => (size, 1.),
            None => {
                let mut system = System::new();
                system.refresh_memory();
                let available_mem = system.available_memory();
                (available_mem as usize, 0.85)
            }
        };

        let runtime_config = runtime_config.with_memory_limit(pool_size, fraction);
        let runtime = Arc::new(RuntimeEnv::new(runtime_config).unwrap());
        let mut state = SessionState::new_with_config_rt(config, runtime);

        if let Some(tag) = &self.filter_tag {
            let filter = FilterOptimizerRule {
                column: crate::event::DEFAULT_TAGS_KEY.to_string(),
                literals: tag.clone(),
            };
            state = state.add_optimizer_rule(Arc::new(filter))
        }

        SessionContext::new_with_state(state)
    }

    /// Execute query on object storage(and if necessary on cache as well) with given stream information
    /// TODO: find a way to query all selected parquet files together in a single context.
    pub async fn execute(
        &self,
        storage: Arc<dyn ObjectStorage + Send>,
    ) -> Result<(Vec<RecordBatch>, Vec<String>), ExecuteError> {
        let ctx = self.create_session_context();
        let remote_listing_table = self._remote_query(storage)?;

        let current_minute = Utc::now()
            .with_second(0)
            .and_then(|x| x.with_nanosecond(0))
            .expect("zeroed value is valid");

        let memtable = if self.end > current_minute {
            crate::event::STREAM_WRITERS.recordbatches_cloned(&self.stream_name, &self.schema)
        } else {
            None
        };

        let table =
            QueryTableProvider::try_new(memtable, remote_listing_table, self.schema.clone())?;

        ctx.register_table(&*self.stream_name, Arc::new(table))
            .map_err(ObjectStorageError::DataFusionError)?;
        // execute the query and collect results
        let df = ctx.sql(self.query.as_str()).await?;
        // dataframe qualifies name by adding table name before columns. \
        // For now this is just actual names
        let fields = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name())
            .cloned()
            .collect_vec();

        let results = df.collect().await?;

        Ok((results, fields))
    }

    fn _remote_query(
        &self,
        storage: Arc<dyn ObjectStorage + Send>,
    ) -> Result<Option<Arc<ListingTable>>, ExecuteError> {
        let prefixes = storage.query_prefixes(self.get_prefixes());
        if prefixes.is_empty() {
            return Ok(None);
        }
        let file_format = ParquetFormat::default().with_enable_pruning(Some(true));
        let file_sort_order = vec![vec![col(DEFAULT_TIMESTAMP_KEY).sort(false, true)]];
        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(".parquet")
            .with_file_sort_order(file_sort_order)
            .with_collect_stat(true)
            // enforce distribution will take care of parallelization
            .with_target_partitions(1);

        let config = ListingTableConfig::new_with_multi_paths(prefixes)
            .with_listing_options(listing_options)
            .with_schema(self.schema.clone());

        let listing_table = Arc::new(ListingTable::try_new(config)?);
        Ok(Some(listing_table))
    }
}

#[allow(dead_code)]
fn get_staging_prefixes(
    stream_name: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> HashMap<PathBuf, Vec<PathBuf>> {
    let dir = StorageDir::new(stream_name);
    let mut files = dir.arrow_files_grouped_by_time();
    files.retain(|k, _| path_intersects_query(k, start, end));
    files
}

fn path_intersects_query(path: &Path, starttime: DateTime<Utc>, endtime: DateTime<Utc>) -> bool {
    let time = time_from_path(path);
    starttime <= time && time <= endtime
}

fn time_from_path(path: &Path) -> DateTime<Utc> {
    let prefix = path
        .file_name()
        .expect("all given path are file")
        .to_str()
        .expect("filename is valid");

    // Next three in order will be date, hour and minute
    let mut components = prefix.splitn(3, '.');

    let date = components.next().expect("date=xxxx-xx-xx");
    let hour = components.next().expect("hour=xx");
    let minute = components.next().expect("minute=xx");

    let year = date[5..9].parse().unwrap();
    let month = date[10..12].parse().unwrap();
    let day = date[13..15].parse().unwrap();
    let hour = hour[5..7].parse().unwrap();
    let minute = minute[7..9].parse().unwrap();

    Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
        .unwrap()
}

pub mod error {
    use datafusion::error::DataFusionError;

    use crate::{storage::ObjectStorageError, validator::error::QueryValidationError};

    use super::Key;

    #[derive(Debug, thiserror::Error)]
    pub enum ParseError {
        #[error("Key not found: {0}")]
        Key(String),
        #[error("Error parsing query: {0}")]
        Validation(#[from] QueryValidationError),
    }

    impl From<Key> for ParseError {
        fn from(key: Key) -> Self {
            ParseError::Key(key.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum ExecuteError {
        #[error("Query Execution failed due to error in object storage: {0}")]
        ObjectStorage(#[from] ObjectStorageError),
        #[error("Query Execution failed due to error in datafusion: {0}")]
        Datafusion(#[from] DataFusionError),
    }
}

#[cfg(test)]
mod tests {
    use super::time_from_path;
    use std::path::PathBuf;

    #[test]
    fn test_time_from_parquet_path() {
        let path = PathBuf::from("date=2022-01-01.hour=00.minute=00.hostname.data.parquet");
        let time = time_from_path(path.as_path());
        assert_eq!(time.timestamp(), 1640995200);
    }
}
