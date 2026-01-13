/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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

use std::fs;

use actix_web::http::StatusCode;
use actix_web::{
    HttpRequest, Responder,
    web::{Json, Path},
};
use bytes::Bytes;
use tracing::warn;

use crate::{
    catalog::remove_manifest_from_snapshot,
    handlers::http::logstream::error::StreamError,
    parseable::{PARSEABLE, StreamNotFound},
    stats, utils::get_tenant_id_from_request,
};

pub async fn retention_cleanup(
    req: HttpRequest,
    stream_name: Path<String>,
    Json(date_list): Json<Vec<String>>,
) -> Result<impl Responder, StreamError> {
    let stream_name = stream_name.into_inner();
    let storage = PARSEABLE.storage().get_object_store();
    let tenant_id = get_tenant_id_from_request(&req);
    // if the stream not found in memory map,
    //check if it exists in the storage
    //create stream and schema from storage
    if !PARSEABLE.streams.contains(&stream_name, &tenant_id)
        && !PARSEABLE
            .create_stream_and_schema_from_storage(&stream_name, &tenant_id)
            .await
            .unwrap_or(false)
    {
        return Err(StreamNotFound(stream_name.clone()).into());
    }

    if let Err(err) = remove_manifest_from_snapshot(&storage, &stream_name, date_list, &tenant_id).await
    {
        return Err(StreamError::Custom {
            msg: format!(
                "failed to update snapshot during retention cleanup for stream {}: {}",
                stream_name, err
            ),
            status: StatusCode::INTERNAL_SERVER_ERROR,
        });
    }

    Ok(actix_web::HttpResponse::NoContent().finish())
}

pub async fn delete(req: HttpRequest, stream_name: Path<String>) -> Result<impl Responder, StreamError> {
    let stream_name = stream_name.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    // Delete from staging
    let stream_dir = PARSEABLE.get_stream(&stream_name, &tenant_id)?;
    if let Err(err) = fs::remove_dir_all(&stream_dir.data_path) {
        warn!(
            "failed to delete local data for stream {} with error {err}. Clean {} manually",
            stream_name,
            stream_dir.data_path.to_string_lossy()
        )
    }

    // Delete from memory
    PARSEABLE.streams.delete(&stream_name, &tenant_id);
    stats::delete_stats(&stream_name, "json", &tenant_id)
        .unwrap_or_else(|e| warn!("failed to delete stats for stream {}: {:?}", stream_name, e));

    Ok((format!("log stream {stream_name} deleted"), StatusCode::OK))
}

pub async fn put_stream(
    req: HttpRequest,
    stream_name: Path<String>,
    body: Bytes,
) -> Result<impl Responder, StreamError> {
    let stream_name = stream_name.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    PARSEABLE
        .create_update_stream(req.headers(), &body, &stream_name, &tenant_id)
        .await?;

    Ok(("Log stream created", StatusCode::OK))
}
