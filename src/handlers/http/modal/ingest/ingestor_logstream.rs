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

use actix_web::http::StatusCode;
use actix_web::{
    HttpRequest, Responder,
    web::{Json, Path},
};
use bytes::Bytes;
use tokio::sync::Mutex;
use tracing::warn;

use crate::option::Mode;
use crate::{
    catalog::remove_manifest_from_snapshot,
    handlers::http::logstream::error::StreamError,
    parseable::{PARSEABLE, StreamNotFound},
    utils::get_tenant_id_from_request,
};

// Serializes creation/update and local deletion cleanup on this ingestor.
// Reconciliation uses the same lock so old cleanup cannot remove files from
// a dataset recreated under the same name.
pub(crate) static CREATE_STREAM_LOCK: Mutex<()> = Mutex::const_new(());

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

    if let Err(err) =
        remove_manifest_from_snapshot(&storage, &stream_name, date_list, &tenant_id).await
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

pub async fn delete(
    req: HttpRequest,
    stream_name: Path<String>,
) -> Result<impl Responder, StreamError> {
    let stream_name = stream_name.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    // The querier owns the tombstone and object-storage deletion. This handler
    // handles its notification locally, holding off recreation through cleanup.
    let _creation_guard = CREATE_STREAM_LOCK.lock().await;

    let stream_dir = {
        // Reject new ingestion before waiting for active local operations.
        let stream_dir = PARSEABLE.get_stream(&stream_name, &tenant_id)?;
        stream_dir.mark_deleting();
        stream_dir
    };

    // On ingest/standalone nodes, wait for participating ingestion and local
    // sync, finalize writers, then remove staging and reset stats. Cleanup is
    // synchronous here; reconciliation retries it before evicting a stale entry.
    if (PARSEABLE.options.mode == Mode::Ingest || PARSEABLE.options.mode == Mode::All)
        && let Err(err) = stream_dir.remove_staging(&tenant_id).await
    {
        warn!(
            "failed to delete local data for stream {} with error {err}. Clean {} manually",
            stream_name,
            stream_dir.data_path.to_string_lossy()
        )
    }
    if let Err(err) = crate::stats::delete_stats(&stream_name, "json", &tenant_id) {
        warn!("failed to clear stats for deleted stream {stream_name}: {err}");
    }
    // Keep the old entry marked deleting until the querier finishes deleting
    // object storage. Once the tombstone is gone, reconciliation or recreation
    // self-healing cleans any remaining staging and evicts the entry. Evicting
    // here would let requests lose the local guard while deletion is ongoing.
    Ok((
        format!("log stream {stream_name} deletion started"),
        StatusCode::ACCEPTED,
    ))
}

pub async fn put_stream(
    req: HttpRequest,
    stream_name: Path<String>,
    body: Bytes,
) -> Result<impl Responder, StreamError> {
    let stream_name = stream_name.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    let _guard = CREATE_STREAM_LOCK.lock().await;
    PARSEABLE
        .create_update_stream(req.headers(), &body, &stream_name, &tenant_id)
        .await?;

    Ok(("Log stream created", StatusCode::OK))
}
