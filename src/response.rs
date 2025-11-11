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

use crate::{handlers::http::query::QueryError, utils::arrow::record_batches_to_json};
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::{Value, json};
use tracing::info;

pub struct QueryResponse {
    pub records: Vec<RecordBatch>,
    pub fields: Vec<String>,
    pub fill_null: bool,
    pub with_fields: bool,
}

impl QueryResponse {
    pub fn to_json(&self) -> Result<Value, QueryError> {
        info!("{}", "Returning query results");

        // Process in batches to avoid massive allocations
        const BATCH_SIZE: usize = 100; // Process 100 record batches at a time
        let mut all_values = Vec::new();

        for chunk in self.records.chunks(BATCH_SIZE) {
            let mut json_records = record_batches_to_json(chunk)?;

            if self.fill_null {
                for map in &mut json_records {
                    for field in &self.fields {
                        if !map.contains_key(field) {
                            map.insert(field.clone(), Value::Null);
                        }
                    }
                }
            }

            // Convert this batch to values and add to collection
            let batch_values: Vec<Value> = json_records.into_iter().map(Value::Object).collect();
            all_values.extend(batch_values);
        }

        let response = if self.with_fields {
            json!({
                "fields": self.fields,
                "records": all_values,
            })
        } else {
            Value::Array(all_values)
        };

        Ok(response)
    }
}
