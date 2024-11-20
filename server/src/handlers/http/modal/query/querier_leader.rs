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

use actix_web::{HttpResponse, Responder};

use crate::handlers::http::{
    logstream::error::StreamError,
    modal::{
        query_server::{QUERY_COORDINATION, QUERY_ROUTING},
        LEADER,
    },
};

pub async fn make_leader() -> impl Responder {
    LEADER.lock().await.make_leader();

    // also modify QUERY_ROUTING for this query node
    QUERY_ROUTING.lock().await.reset().await;

    LEADER.lock().await.remove_other_leaders().await;

    // send request to other nodes for remove leader
    HttpResponse::Ok().finish()
}

pub async fn remove_leader() -> anyhow::Result<impl Responder, StreamError> {
    // if this node is leader, remove
    LEADER.lock().await.remove_leader();

    // reset this node's leader
    QUERY_COORDINATION.lock().await.reset_leader().await?;
    Ok(HttpResponse::Ok().finish())
}

pub async fn is_leader() -> anyhow::Result<impl Responder, StreamError> {
    // if this node is leader, send ok
    if LEADER.lock().await.is_leader() {
        Ok("leader")
    } else {
        Err(StreamError::Anyhow(anyhow::Error::msg("not a leader")))
    }
}