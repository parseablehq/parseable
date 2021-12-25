/*
 * Parseable Server (C) 2022 Parseable, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use actix_web::dev::HttpResponseBuilder;
use actix_web::HttpResponse;
use arrow::record_batch::RecordBatch;

pub struct ServerResponse {
    pub http_response: HttpResponseBuilder,
    pub msg: String,
    pub rb: Option<RecordBatch>,
    pub schema: Option<arrow::datatypes::Schema>,
}

impl ServerResponse {
    pub fn success_server_response(&self) -> HttpResponse {
        log::info!("{}", self.msg);
        HttpResponse::Ok().body(format!("{}", self.msg))
    }
    pub fn error_server_response(&self) -> HttpResponse {
        log::error!("{}", self.msg);
        HttpResponse::Ok().body(format!("{}", self.msg))
    }
}
