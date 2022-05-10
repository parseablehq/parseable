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

use arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::prelude::*;
use serde::Deserialize;
use std::sync::Arc;

use crate::utils;

// Query holds all values relevant to a query for a single stream
#[derive(Deserialize)]
pub struct Query {
    pub query: String,
}

impl Query {
    // parse_query parses the SQL query and returns the stream name on which
    // this query is supposed to be executed
    pub fn parse(&self) -> Result<String, String> {
        // convert to lowercase before parsing
        let query_lower = self.query.to_lowercase();
        let tokens = query_lower.split(' ').collect::<Vec<&str>>();
        match Self::validate(&tokens) {
            Ok(_) => {
                // stream name is located after the `from` keyword
                let stream_name_index = tokens.iter().position(|&x| x == "from").unwrap() + 1;
                // we currently don't support queries like "select name, address from stream1 and stream2"
                // so if there is an `and` after the first stream name, we return an error.
                if tokens.len() > stream_name_index + 1 && tokens[stream_name_index + 1] == "and" {
                    return Err(String::from(
                        "queries across multiple streams are not supported currently",
                    ));
                }
                Ok(tokens[stream_name_index].to_string())
            }
            Err(e) => Err(e),
        }
    }

    fn validate(tokens: &[&str]) -> Result<(), String> {
        if tokens.contains(&"") {
            return Err(String::from("query cannot be empty"));
        }
        if tokens.contains(&"join") {
            return Err(String::from("joins are not supported currently"));
        }
        Ok(())
    }

    #[tokio::main]
    pub async fn execute(&self, stream: &str) -> Result<Vec<RecordBatch>, String> {
        let mut ctx = ExecutionContext::new();
        let file_format = ParquetFormat::default().with_enable_pruning(true);

        let listing_options = ListingOptions {
            file_extension: ".parquet".to_owned(),
            format: Arc::new(file_format),
            table_partition_cols: vec![],
            collect_stat: true,
            target_partitions: 1,
        };

        match ctx
            .register_listing_table(
                stream,
                utils::get_cache_path(stream).as_str(),
                listing_options,
                None,
            )
            .await
        {
            Ok(_) => {
                // execute the query
                match ctx.sql(self.query.as_str()).await {
                    Ok(df) => match df.collect().await {
                        Ok(results) => Ok(results),
                        Err(e) => Err(e.to_string()),
                    },
                    Err(e) => Err(e.to_string()),
                }
            }
            Err(e) => Err(e.to_string()),
        }
    }
}
