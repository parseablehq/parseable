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
use actix_web::{error, web, HttpResponse, HttpResponseBuilder, Responder};
use datafusion::arrow::json;
use datafusion::arrow::record_batch::RecordBatch;
use derive_more::{Display, Error};

use crate::storage;

pub struct ServerResponse {
    pub code: StatusCode,
    pub msg: String,
}

impl ServerResponse {
    pub fn to_http(&self) -> HttpResponse {
        log::info!("{}", self.msg);

        HttpResponseBuilder::new(self.code)
            .content_type("text")
            .body(self.msg.to_string())
    }
}

pub fn list_response(body: Vec<storage::LogStream>) -> impl Responder {
    web::Json(body)
}

pub struct QueryResponse {
    pub code: StatusCode,
    pub body: Vec<RecordBatch>,
}

impl QueryResponse {
    pub fn to_http(&self) -> HttpResponse {
        log::info!("{}", "Returning query results");
        let buf = Vec::new();
        let mut writer = json::ArrayWriter::new(buf);
        writer.write_batches(&self.body).unwrap();
        writer.finish().unwrap();

        HttpResponseBuilder::new(self.code)
            .content_type("json")
            .body(writer.into_inner())
    }
}

impl From<Vec<RecordBatch>> for QueryResponse {
    fn from(body: Vec<RecordBatch>) -> Self {
        Self {
            code: StatusCode::OK,
            body,
        }
    }
}

#[derive(Debug, Display, Error)]
pub struct EventError {
    pub msg: String,
}

impl error::ResponseError for EventError {}
