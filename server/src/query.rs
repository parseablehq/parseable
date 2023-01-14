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
use serde_json::Value;
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;
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
    pub schema: Arc<Schema>,
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
    pub fn get_prefixes(&self) -> Vec<String> {
        TimePeriod::new(self.start, self.end, OBJECT_STORE_DATA_GRANULARITY)
            .generate_prefixes(&self.stream_name)
    }

    /// Execute query on object storage(and if necessary on cache as well) with given stream information
    /// TODO: find a way to query all selected parquet files together in a single context.
    pub async fn execute(
        &self,
        storage: &(impl ObjectStorage + ?Sized),
    ) -> Result<Vec<RecordBatch>, ExecuteError> {
        let dir = StorageDir::new(&self.stream_name);

        // take a look at local dir and figure out what local cache we could use for this query
        let arrow_files: Vec<PathBuf> = dir
            .arrow_files()
            .into_iter()
            .filter(|path| path_intersects_query(path, self.start, self.end))
            .collect();

        let possible_parquet_files = arrow_files.iter().cloned().map(|mut path| {
            path.set_extension("parquet");
            path
        });

        let parquet_files = dir
            .parquet_files()
            .into_iter()
            .filter(|path| path_intersects_query(path, self.start, self.end));

        let parquet_files: HashSet<PathBuf> = possible_parquet_files.chain(parquet_files).collect();
        let parquet_files = Vec::from_iter(parquet_files.into_iter());

        let ctx = SessionContext::with_config_rt(
            SessionConfig::default(),
            CONFIG.storage().get_datafusion_runtime(),
        );

        let table = Arc::new(QueryTableProvider::new(
            arrow_files,
            parquet_files,
            storage.query_table(self)?,
            Arc::clone(&self.schema),
        ));
        ctx.register_table(
            &*self.stream_name,
            Arc::clone(&table) as Arc<dyn TableProvider>,
        )
        .map_err(ObjectStorageError::DataFusionError)?;
        // execute the query and collect results
        let df = ctx.sql(self.query.as_str()).await?;
        let results = df.collect().await?;
        Ok(results)
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

    // substring of filename i.e date=xxxx.hour=xx.minute=xx
    let prefix = &prefix[..33];
    Utc.datetime_from_str(prefix, "date=%F.hour=%H.minute=%M")
        .expect("valid prefix is parsed")
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
    use super::{time_from_path, Query};
    use crate::{alerts::Alerts, metadata::STREAM_INFO};
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::datatypes::{DataType, Field};
    use rstest::*;
    use serde_json::Value;
    use std::path::PathBuf;
    use std::str::FromStr;

    #[test]
    fn test_time_from_parquet_path() {
        let path = PathBuf::from("date=2022-01-01.hour=00.minute=00.hostname.data.parquet");
        let time = time_from_path(path.as_path());
        assert_eq!(time.timestamp(), 1640995200);
    }

    // Query prefix generation tests
    #[fixture]
    fn schema() -> Schema {
        let field_a = Field::new("a", DataType::Int64, false);
        let field_b = Field::new("b", DataType::Boolean, false);
        Schema::new(vec![field_a, field_b])
    }

    fn clear_map() {
        STREAM_INFO.write().unwrap().clear();
    }

    // A query can only be performed on streams with a valid schema
    #[rstest]
    #[case(
        r#"{
            "query": "SELECT * FROM stream_name",
            "startTime": "2022-10-15T10:00:00+00:00",
            "endTime": "2022-10-15T10:01:00+00:00"
        }"#,
        &["stream_name/date=2022-10-15/hour=10/minute=00/"]
    )]
    #[case(
        r#"{
            "query": "SELECT * FROM stream_name",
            "startTime": "2022-10-15T10:00:00+00:00",
            "endTime": "2022-10-15T10:02:00+00:00"
        }"#,
        &["stream_name/date=2022-10-15/hour=10/minute=00/", "stream_name/date=2022-10-15/hour=10/minute=01/"]
    )]
    #[serial_test::serial]
    fn query_parse_prefix_with_some_schema(#[case] prefix: &str, #[case] right: &[&str]) {
        clear_map();
        STREAM_INFO.add_stream("stream_name".to_string(), Some(schema()), Alerts::default());

        let query = Value::from_str(prefix).unwrap();
        let query = Query::parse(query).unwrap();
        assert_eq!(&query.stream_name, "stream_name");
        let prefixes = query.get_prefixes();
        let left = prefixes.iter().map(String::as_str).collect::<Vec<&str>>();
        assert_eq!(left.as_slice(), right);
    }

    // If there is no schema for this stream then parsing a Query should fail
    #[rstest]
    #[case(
        r#"{
            "query": "SELECT * FROM stream_name",
            "startTime": "2022-10-15T10:00:00+00:00",
            "endTime": "2022-10-15T10:01:00+00:00"
        }"#
    )]
    #[serial_test::serial]
    fn query_parse_prefix_with_no_schema(#[case] prefix: &str) {
        clear_map();
        STREAM_INFO.add_stream("stream_name".to_string(), None, Alerts::default());

        let query = Value::from_str(prefix).unwrap();
        assert!(Query::parse(query).is_err());
    }
}
