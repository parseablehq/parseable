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

use crate::{
    handlers::http::query::QueryError,
    utils::arrow::{
        flight::{into_flight_data, DoGetStream},
        record_batches_to_json,
    },
};
use actix_web::HttpResponse;
use datafusion::arrow::record_batch::RecordBatch;
use itertools::Itertools;
use serde_json::{json, Value};
use tonic::{Response, Status};
use tracing::info;

pub struct QueryResponse {
    pub records: Vec<RecordBatch>,
    pub fields: Vec<String>,
    pub fill_null: bool,
    pub with_fields: bool,
}

impl QueryResponse {
    pub fn to_http(&self) -> Result<HttpResponse, QueryError> {
        info!("{}", "Returning query results");
        let records: Vec<&RecordBatch> = self.records.iter().collect();
        let mut json_records = record_batches_to_json(&records)?;

        if self.fill_null {
            for map in &mut json_records {
                for field in &self.fields {
                    if !map.contains_key(field) {
                        map.insert(field.clone(), Value::Null);
                    }
                }
            }
        }
        let values = json_records.into_iter().map(Value::Object).collect_vec();

        let response = if self.with_fields {
            json!({
                "fields": self.fields,
                "records": values
            })
        } else {
            Value::Array(values)
        };

        Ok(HttpResponse::Ok().json(response))
    }

    #[allow(dead_code)]
    pub fn into_flight(self) -> Result<Response<DoGetStream>, Status> {
        into_flight_data(self.records)
    }
}
