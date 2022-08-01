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
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::prelude::*;
use serde_json::Value;
use std::sync::Arc;

use crate::option::CONFIG;
use crate::storage;
use crate::storage::ObjectStorage;
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
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

impl Query {
    // parse_query parses the SQL query and returns the log stream name on which
    // this query is supposed to be executed
    pub fn parse(query_json: Value) -> Result<Query, Error> {
        // retrieve query, start and end time information from payload.
        // Convert query to lowercase.
        let query = get_value(&query_json, "query")?.to_lowercase();
        let start_time = get_value(&query_json, "startTime")?;
        let end_time = get_value(&query_json, "endTime")?;

        validator::query(&query, start_time, end_time)
    }

    /// Return prefixes, each per day/hour/minutes as necessary
    pub fn get_prefixes(&self) -> Vec<String> {
        TimePeriod::new(self.start, self.end, storage::BLOCK_DURATION)
            .generate_prefixes(&self.stream_name)
    }

    /// Execute query on object storage(and if necessary on cache as well) with given stream information
    /// TODO: find a way to query all selected parquet files together in a single context.
    pub async fn execute(&self, storage: &impl ObjectStorage) -> Result<Vec<RecordBatch>, Error> {
        let mut results = vec![];
        storage.query(self, &mut results).await?;

        // query cache only if end_time coulld have been after last sync.
        let duration_since = Utc::now() - self.end;
        if duration_since.num_seconds() < CONFIG.parseable.sync_duration as i64 {
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

        ctx.register_listing_table(
            &self.stream_name,
            CONFIG.parseable.get_cache_path(&self.stream_name).as_str(),
            listing_options,
            None,
        )
        .await?;

        // execute the query and collect results
        let df = ctx.sql(self.query.as_str()).await?;
        results.extend(df.collect().await.map_err(Error::DataFusion)?);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use serde_json::Value;

    use super::Query;

    #[test]
    fn query_parse_prefix() {
        let query = Value::from_str(
            r#"{
    "query": "SELECT * FROM stream_name",
    "startTime": "2022-10-15T10:00:00+00:00",
    "endTime": "2022-10-15T10:01:00+00:00"
}"#,
        )
        .unwrap();

        let query = Query::parse(query).unwrap();

        assert_eq!(&query.stream_name, "stream_name");
        assert_eq!(
            query.get_prefixes(),
            vec!["stream_name/date=2022-10-15/hour=10/minute=00/".to_string()]
        );
    }
}
