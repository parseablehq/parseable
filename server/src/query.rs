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

use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::datasource::listing::ListingTable;
use datafusion::datasource::listing::ListingTableConfig;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::prelude::*;
use serde_json::Value;
use std::sync::Arc;

use crate::metadata::STREAM_INFO;
use crate::option::CONFIG;
use crate::storage;
use crate::storage::ObjectStorage;
use crate::storage::ObjectStorageError;
use crate::utils::TimePeriod;
use crate::validator;
use crate::Error;

fn get_value<'a>(value: &'a Value, key: &'static str) -> Result<&'a str, Error> {
    value
        .get(key)
        .ok_or(Error::JsonQuery(key))?
        .as_str()
        .ok_or(Error::JsonQuery(key))
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
    pub fn parse(query_json: Value) -> Result<Query, Error> {
        // retrieve query, start and end time information from payload.
        let query = get_value(&query_json, "query")?;
        let start_time = get_value(&query_json, "startTime")?;
        let end_time = get_value(&query_json, "endTime")?;

        validator::query(query, start_time, end_time)
    }

    /// Return prefixes, each per day/hour/minutes as necessary
    pub fn get_prefixes(&self) -> Vec<String> {
        TimePeriod::new(self.start, self.end, storage::OBJECT_STORE_DATA_GRANULARITY)
            .generate_prefixes(&self.stream_name)
    }

    /// Execute query on object storage(and if necessary on cache as well) with given stream information
    /// TODO: Query local and remote S3 parquet files in a single context
    pub async fn execute(&self, storage: &impl ObjectStorage) -> Result<Vec<RecordBatch>, Error> {
        let mut results = vec![];
        storage.query(self, &mut results).await?;

        // query cache only if end_time coulld have been after last sync.
        let duration_since = Utc::now() - self.end;
        if duration_since.num_seconds() < CONFIG.parseable.upload_interval as i64 {
            self.execute_on_cache(&mut results).await?;
        }

        Ok(results)
    }

    async fn execute_on_cache(&self, results: &mut Vec<RecordBatch>) -> Result<(), Error> {
        let ctx = SessionContext::new();
        let file_format = ParquetFormat::default().with_enable_pruning(true);

        let listing_options = ListingOptions {
            file_extension: ".parquet".to_owned(),
            format: Arc::new(file_format),
            table_partition_cols: vec![],
            collect_stat: true,
            target_partitions: 1,
        };

        let schema = STREAM_INFO.schema(&self.stream_name)?;

        let schema = match schema {
            Some(schema) => Arc::new(schema),
            None => return Ok(()),
        };

        let cache_path = CONFIG.parseable.get_cache_path(&self.stream_name);

        let table_path =
            ListingTableUrl::parse(cache_path.to_str().expect("path should is valid unicode"))
                .expect("path should parse into valid listing url for local filesystem");

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(schema);

        let table = ListingTable::try_new(config)?;

        ctx.register_table(&*self.stream_name, Arc::new(table))
            .map_err(ObjectStorageError::DataFusionError)?;

        // execute the query and collect results
        let df = ctx.sql(self.query.as_str()).await?;
        results.extend(df.collect().await.map_err(Error::DataFusion)?);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Query;
    use crate::{alerts::Alerts, metadata::STREAM_INFO};
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::datatypes::{DataType, Field};
    use rstest::*;
    use serde_json::Value;
    use std::str::FromStr;

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
        STREAM_INFO
            .add_stream("stream_name".to_string(), Some(schema()), Alerts::default())
            .unwrap();

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
        STREAM_INFO
            .add_stream("stream_name".to_string(), None, Alerts::default())
            .unwrap();

        let query = Value::from_str(prefix).unwrap();
        assert!(Query::parse(query).is_err());
    }
}
