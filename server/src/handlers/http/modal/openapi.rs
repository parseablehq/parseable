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
    alerts::Alerts,
    handlers::http::{
        about,
        cluster::utils::{IngestionStats, QueriedStats, StorageStats},
        health_check, ingest, logstream, query, rbac, role,
    },
    hottier::StreamHotTier,
    response,
    storage::{retention::Retention, StreamInfo},
};
use utoipa::{
    openapi::security::{HttpBuilder, SecurityScheme},
    Modify, OpenApi,
};

#[derive(OpenApi)]
#[openapi(
paths(
    query::query,
    health_check::liveness,
    health_check::readiness,
    about::about,
    role::list,
    role::put_default,
    role::delete,
    role::get_default,
    role::get,
    role::put,
    rbac::post_user,
    rbac::list_users,
    rbac::delete_user,
    rbac::get_role,
    rbac::put_role,
    rbac::post_gen_password,
    logstream::put_stream,
    ingest::post_event,
    logstream::get_stream_info,
    logstream::put_alert,
    logstream::get_alert,
    logstream::schema,
    logstream::get_stats,
    logstream::get_retention,
    logstream::put_retention,
    logstream::put_enable_cache,
    logstream::get_cache_enabled,
    logstream::put_stream_hot_tier,
    logstream::get_stream_hot_tier,
    logstream::delete_stream_hot_tier,
),
components(
    schemas(
        response::QueryResponse,
        query::Query,
        StreamInfo,
        Alerts,
        QueriedStats,
        IngestionStats,
        StorageStats,
        Retention,
        StreamHotTier
    )
),
info(
    description = "Parseable API documents [https://www.parseable.com/docs/](https://www.parseable.com/docs/)",
    contact(name = "Parseable", email = "hi@parseable.com", url = "https://www.parseable.com/"),
),
modifiers(&SecurityAddon),
tags(
    (name = "role", description = "Roles retrieval and management"),
    (name = "user", description = "Users retrieval and management"),
    (name = "health check", description = "Health of services"),
    (name = "about", description = "Details about this Parseable executable"),
    (name = "query", description = "Running queries against the server"),
    (name = "logstream", description = "Upsert, delete, and ingest data to streams")
)
)]
pub struct ApiDoc;

pub struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.as_mut().unwrap();
        // components.add_security_scheme(
        //     "Authorization",
        //     SecurityScheme::ApiKey(utoipa::openapi::security::ApiKey::Header(
        //         utoipa::openapi::security::ApiKeyValue::new("Authorization"),
        //     )),
        // );
        components.add_security_scheme(
            "basic_auth",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(utoipa::openapi::security::HttpAuthScheme::Basic)
                    .build(),
            ),
        )
    }
}
