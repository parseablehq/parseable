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

use actix_web::http::StatusCode;
use actix_web::{web, Responder};
use datafusion::arrow::json::writer::record_batches_to_json_rows;
use datafusion::arrow::record_batch::RecordBatch;
use itertools::Itertools;
use serde_json::Value;

pub struct QueryResponse {
    pub code: StatusCode,
    pub records: Vec<RecordBatch>,
    pub fields: Vec<String>,
}

impl QueryResponse {
    pub fn to_http(&self) -> impl Responder {
        log::info!("{}", "Returning query results");
        let mut json = record_batches_to_json_rows(&self.records).unwrap();
        for map in &mut json {
            for field in &self.fields {
                if !map.contains_key(field) {
                    map.insert(field.clone(), Value::Null);
                }
            }
        }
        let values = json.into_iter().map(|map| Value::Object(map)).collect_vec();
        web::Json(values)
    }
}

impl From<(Vec<RecordBatch>, Vec<String>)> for QueryResponse {
    fn from(body: (Vec<RecordBatch>, Vec<String>)) -> Self {
        let (records, fields) = body;

        Self {
            code: StatusCode::OK,
            records,
            fields,
        }
    }
}
