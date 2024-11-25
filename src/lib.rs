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

mod about;
mod alerts;
mod analytics;
mod banner;
mod catalog;
mod cli;
mod event;
mod handlers;
mod hottier;
mod livetail;
mod localcache;
mod metadata;
mod metrics;
mod migration;
mod oidc;
pub mod option;
mod query;
mod querycache;
mod rbac;
mod response;
mod static_schema;
mod stats;
mod storage;
mod sync;
mod users;
mod utils;
mod validator;

pub use handlers::http::modal::{
    ingest_server::IngestServer, query_server::QueryServer, server::Server, ParseableServer,
};

pub const STORAGE_UPLOAD_INTERVAL: u32 = 60;
