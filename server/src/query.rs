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

mod table_provider;

use chrono::TimeZone;
use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use itertools::Itertools;
use serde_json::Value;
use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::option::CONFIG;
use crate::storage::ObjectStorageError;
use crate::storage::StorageDir;
use crate::storage::{ObjectStorage, OBJECT_STORE_DATA_GRANULARITY};
use crate::utils::TimePeriod;
use crate::validator;

use self::error::{ExecuteError, ParseError};
use table_provider::QueryTableProvider;

type Key = &'static str;
fn get_value(value: &Value, key: Key) -> Result<&str, Key> {
    value.get(key).and_then(|value| value.as_str()).ok_or(key)
}

// Query holds all values relevant to a query for a single log stream
pub struct Query {
    pub query: String,
    pub stream_name: String,
    pub merged_schema: Arc<Schema>,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
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
            .collect()
    }

    pub fn get_schema(&self) -> &Schema {
        &self.merged_schema
    }

    /// Execute query on object storage(and if necessary on cache as well) with given stream information
    /// TODO: find a way to query all selected parquet files together in a single context.
    pub async fn execute(
        &self,
        storage: Arc<dyn ObjectStorage + Send>,
    ) -> Result<(Vec<RecordBatch>, Vec<String>), ExecuteError> {
        let dir = StorageDir::new(&self.stream_name);
        // take a look at local dir and figure out what local cache we could use for this query
        let staging_arrows = dir
            .arrow_files_grouped_by_time()
            .into_iter()
            .filter(|(path, _)| path_intersects_query(path, self.start, self.end))
            .sorted_by(|(a, _), (b, _)| Ord::cmp(a, b))
            .collect_vec();

        let staging_parquet_set: HashSet<&PathBuf, RandomState> =
            HashSet::from_iter(staging_arrows.iter().map(|(p, _)| p));

        let other_staging_parquet = dir
            .parquet_files()
            .into_iter()
            .filter(|path| path_intersects_query(path, self.start, self.end))
            .filter(|path| !staging_parquet_set.contains(path))
            .collect_vec();

        let ctx = SessionContext::with_config_rt(
            SessionConfig::default(),
            CONFIG.storage().get_datafusion_runtime(),
        );

        let table = Arc::new(QueryTableProvider::new(
            staging_arrows,
            other_staging_parquet,
            self.get_prefixes(),
            storage,
            Arc::new(self.get_schema().clone()),
        ));

        ctx.register_table(
            &*self.stream_name,
            Arc::clone(&table) as Arc<dyn TableProvider>,
        )
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
