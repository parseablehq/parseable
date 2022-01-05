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
use actix_web::http::StatusCode;
use actix_web::{error, HttpResponse};
use derive_more::{Display, Error};

pub struct ServerResponse {
    pub code: StatusCode,
    pub msg: String,
}

impl ServerResponse {
    pub fn to_http(&self) -> HttpResponse {
        log::info!("{}", self.msg);
        HttpResponseBuilder::new(self.code).body(format!("{}", self.msg))
    }
}

pub struct EventResponse {
    pub msg: String,
}

#[derive(Debug, Display, Error)]
pub struct EventError {
    pub msg: String,
}

impl error::ResponseError for EventError {}
