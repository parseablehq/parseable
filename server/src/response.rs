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

use actix_web::{web, Responder};
use datafusion::arrow::json::writer::record_batches_to_json_rows;
use datafusion::arrow::record_batch::RecordBatch;
use itertools::Itertools;
use serde_json::{json, Value};

pub struct QueryResponse {
    pub records: Vec<RecordBatch>,
    pub fields: Vec<String>,
    pub fill_null: bool,
    pub with_fields: bool,
}

impl QueryResponse {
    pub fn to_http(&self, imem: Option<Vec<Value>>) -> impl Responder {
        log::info!("{}", "Returning query results");
        let records: Vec<&RecordBatch> = self.records.iter().collect();
        let mut json_records = record_batches_to_json_rows(&records).unwrap();
        if self.fill_null {
            for map in &mut json_records {
                for field in &self.fields {
                    if !map.contains_key(field) {
                        map.insert(field.clone(), Value::Null);
                    }
                }
            }
        }
        let mut values = json_records.into_iter().map(Value::Object).collect_vec();

        if let Some(mut imem) = imem {
            values.append(&mut imem);
        }

        let response = if self.with_fields {
            json!({
                "fields": self.fields,
                "records": values
            })
        } else {
            Value::Array(values)
        };

        web::Json(response)
    }
}
