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

use std::{collections::HashMap, future::Future, pin::Pin};

use actix_web::{web, FromRequest, HttpRequest, Responder};
use serde::{Deserialize, Serialize};

use crate::handlers::http::modal::{coordinator_server::QUERY_COORDINATION, QuerierMetadata};

#[derive(Serialize, Debug, Deserialize, Default, Clone, Eq, PartialEq)]
pub struct RegisterQuerier {
    pub querier_metadata: QuerierMetadata
}

pub async fn register_querier(req: HttpRequest, body: RegisterQuerier) -> impl Responder {

    QUERY_COORDINATION.lock().await.append_querier_info(body.querier_metadata);
    
    web::Json("registered")
}

impl FromRequest for RegisterQuerier {
    type Error = actix_web::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, payload: &mut actix_web::dev::Payload) -> Self::Future {
        let metadata = web::Json::<RegisterQuerier>::from_request(req, payload);
        let fut = async move {
            let metadata = metadata.await?.into_inner();
            Ok(metadata)
        };

        Box::pin(fut)
    }
}