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
    alerts::{Alert, AlertVersion, Alerts},
    handlers::http::{
        // about,
        cluster::utils::{IngestionStats, QueriedStats, StorageStats},
        health_check,
        ingest::{self},
        logstream::{self},
        query::{self},
        rbac::{self},
        role,
    },
    hottier::StreamHotTier,
    response,
    storage::{
        retention::{Retention, Task},
        StreamInfo,
    },
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
    // about::about,
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
    ingest::ingest,
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
        StreamHotTier,
        AlertVersion,
        Alert,
        Task,
    )
),
info(
    description = "Parseable API documents [https://www.parseable.com/docs/](https://www.parseable.com/docs/)",
    contact(name = "Parseable", email = "hi@parseable.com", url = "https://www.parseable.com/"),
),
modifiers(&SecurityAddon),
tags(
    (name = "About", description = "Details about this Parseable executable"),
    (name = "Health Status", description = "Health of Parseable server"),
    (name = "Log Stream Ingestion", description = "Sending data to log streams"),
    (name = "Log Stream Alerts", description = "Manipulation of alerts for log streams"),
    (name = "Log Stream Management", description = "Create, List, Delete, log streams"),
    (name = "Log Stream Query", description = "Query log streams"),
    (name = "Log Stream Retention", description = "Get and Set retention policies for log streams"),
    (name = "Log Stream Hottier", description = "Hottier related actions for a log stream"),
    (name = "User Management", description = "Actions pertaining to Users"),
    (name = "Role Management", description = "Actions pertaining to Roles"),
    (name = "RBAC", description = "Add a role to a user"),
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
