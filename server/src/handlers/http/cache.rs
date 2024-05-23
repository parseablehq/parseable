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

use actix_web::{web, HttpRequest, HttpResponse, Responder};
use anyhow::anyhow;
use bytes::Bytes;
use http::StatusCode;
use serde_json::json;

use crate::{
    option::CONFIG,
    querycache::{CacheMetadata, QueryCacheManager},
};

use super::ingest::PostError;

pub async fn list(req: HttpRequest) -> Result<impl Responder, PostError> {
    let stream = req
        .match_info()
        .get("stream")
        .ok_or_else(|| PostError::Invalid(anyhow!("Invalid Stream Name in resource path")))?;

    let user_id = req
        .match_info()
        .get("user_id")
        .ok_or_else(|| PostError::Invalid(anyhow!("Invalid User ID not in Resource path")))?;

    let query_cache_manager = QueryCacheManager::global(CONFIG.parseable.query_cache_size)
        .await
        .unwrap_or(None);

    if let Some(query_cache_manager) = query_cache_manager {
        let cache = query_cache_manager
            .get_cache(stream, user_id)
            .await
            .map_err(PostError::CacheError)?;

        let size = cache.used_cache_size();
        let queries = cache.queries();

        let out = json!({
            "used_capacity": size,
            "cache": queries
        });

        Ok((web::Json(out), StatusCode::OK))
    } else {
        Err(PostError::Invalid(anyhow!(
            "Query Caching is not active on server "
        )))
    }
}

pub async fn remove(req: HttpRequest, body: Bytes) -> Result<impl Responder, PostError> {
    let stream = req
        .match_info()
        .get("stream")
        .ok_or_else(|| PostError::Invalid(anyhow!("Invalid Stream Name in resource path")))?;

    let user_id = req
        .match_info()
        .get("user_id")
        .ok_or_else(|| PostError::Invalid(anyhow!("Invalid User ID not in Resource path")))?;

    let query = serde_json::from_slice::<CacheMetadata>(&body)?;

    let query_cache_manager = QueryCacheManager::global(CONFIG.parseable.query_cache_size)
        .await
        .unwrap_or(None);

    if let Some(query_cache_manager) = query_cache_manager {
        query_cache_manager
            .remove_from_cache(query, stream, user_id)
            .await?;

        Ok(HttpResponse::Ok().finish())
    } else {
        Err(PostError::Invalid(anyhow!("Query Caching is not enabled")))
    }
}
