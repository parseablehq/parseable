/*
 * Parseable Server (C) 2022 Parseable, Inc.
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

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use chrono::TimeZone;
use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::datasource::listing::ListingTable;
use datafusion::datasource::listing::ListingTableConfig;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use serde_json::Value;
use std::any::Any;
use std::fs::File;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use crate::storage;
use crate::storage::ObjectStorage;
use crate::storage::ObjectStorageError;
use crate::storage::StorageDir;
use crate::utils::TimePeriod;
use crate::validator;

use self::error::{ExecuteError, ParseError};

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
        TimePeriod::new(self.start, self.end, storage::OBJECT_STORE_DATA_GRANULARITY)
            .generate_prefixes(&self.stream_name)
    }

    /// Execute query on object storage(and if necessary on cache as well) with given stream information
    /// TODO: find a way to query all selected parquet files together in a single context.
    pub async fn execute(
        &self,
        storage: &impl ObjectStorage,
    ) -> Result<Vec<RecordBatch>, ExecuteError> {
        let dir = StorageDir::new(&self.stream_name);

        // take a look at local dir and figure out what local cache we could use for this query
        let arrow_files: Vec<PathBuf> = dir
            .arrow_files()
            .into_iter()
            .filter(|path| path_intersects_query(path, self.start, self.end))
            .collect();

        let possible_parquet_files = arrow_files.clone().into_iter().map(|mut path| {
            path.set_extension("parquet");
            path
        });

        let parquet_files = dir
            .parquet_files()
            .into_iter()
            .filter(|path| path_intersects_query(path, self.start, self.end));

        let parquet_files: Vec<PathBuf> = possible_parquet_files.chain(parquet_files).collect();

        let mut results = vec![];

        if !(arrow_files.is_empty() && parquet_files.is_empty()) {
            self.execute_on_cache(
                arrow_files,
                parquet_files,
                self.schema.clone(),
                &mut results,
            )
            .await?;
        }

        storage.query(self, &mut results).await?;
        Ok(results)
    }

    async fn execute_on_cache(
        &self,
        arrow_files: Vec<PathBuf>,
        parquet_files: Vec<PathBuf>,
        schema: Arc<Schema>,
        results: &mut Vec<RecordBatch>,
    ) -> Result<(), ExecuteError> {
        let ctx = SessionContext::new();
        let table = Arc::new(QueryTableProvider::new(arrow_files, parquet_files, schema));
        ctx.register_table(
            &*self.stream_name,
            Arc::clone(&table) as Arc<dyn TableProvider>,
        )
        .map_err(ObjectStorageError::DataFusionError)?;
        // execute the query and collect results
        let df = ctx.sql(self.query.as_str()).await?;
        results.extend(df.collect().await?);
        table.remove_preserve();
        Ok(())
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

#[derive(Debug)]
struct QueryTableProvider {
    arrow_files: Vec<PathBuf>,
    parquet_files: Vec<PathBuf>,
    schema: Arc<Schema>,
}

impl QueryTableProvider {
    fn new(arrow_files: Vec<PathBuf>, parquet_files: Vec<PathBuf>, schema: Arc<Schema>) -> Self {
        // By the time this query executes the arrow files could be converted to parquet files
        // we want to preserve these files as well in case

        let mut parquet_cached = crate::storage::CACHED_FILES.lock().expect("no poisoning");
        for file in &parquet_files {
            parquet_cached.upsert(file)
        }

        Self {
            arrow_files,
            parquet_files,
            schema,
        }
    }

    pub fn remove_preserve(&self) {
        let mut parquet_cached = crate::storage::CACHED_FILES.lock().expect("no poisoning");
        for file in &self.parquet_files {
            parquet_cached.remove(file)
        }
    }

    pub async fn create_physical_plan(
        &self,
        ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut mem_records: Vec<Vec<RecordBatch>> = Vec::new();
        let mut parquet_files = self.parquet_files.clone();
        for file in &self.arrow_files {
            let Ok(arrow_file) = File::open(file) else { continue; };
            let reader = StreamReader::try_new(arrow_file, None)?;
            let records = reader
                .filter_map(|record| match record {
                    Ok(record) => Some(record),
                    Err(e) => {
                        log::warn!("warning from arrow stream {:?}", e);
                        None
                    }
                })
                .collect();
            mem_records.push(records);

            let mut file = file.clone();
            file.set_extension("parquet");

            parquet_files.retain(|p| p != &file)
        }

        let memtable = MemTable::try_new(Arc::clone(&self.schema), mem_records)?;
        let memexec = memtable.scan(ctx, projection, filters, limit).await?;

        if parquet_files.is_empty() {
            Ok(memexec)
        } else {
            let listing_options = ListingOptions {
                file_extension: ".parquet".to_owned(),
                format: Arc::new(ParquetFormat::default().with_enable_pruning(true)),
                table_partition_cols: vec![],
                collect_stat: true,
                target_partitions: 1,
            };

            let paths = parquet_files
                .clone()
                .into_iter()
                .map(|path| {
                    ListingTableUrl::parse(path.to_str().expect("path should is valid unicode"))
                        .expect("path is valid for filesystem listing")
                })
                .collect();

            let config = ListingTableConfig::new_with_multi_paths(paths)
                .with_listing_options(listing_options)
                .with_schema(Arc::clone(&self.schema));

            let listtable = ListingTable::try_new(config).unwrap();
            let listexec = listtable.scan(ctx, projection, filters, limit).await?;

            Ok(Arc::new(UnionExec::new(vec![memexec, listexec])))
        }
    }
}

#[async_trait]
impl TableProvider for QueryTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.create_physical_plan(ctx, projection, filters, limit)
            .await
    }
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
        assert_eq!(time.timestamp(), 100);
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
