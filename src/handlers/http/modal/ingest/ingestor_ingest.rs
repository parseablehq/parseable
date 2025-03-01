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

use actix_web::{HttpRequest, HttpResponse};
use bytes::Bytes;

use crate::{handlers::http::{ingest::PostError, modal::utils::ingest_utils::push_logs}, metadata::PARSEABLE.streams};


// Handler for POST /api/v1/logstream/{logstream}
// only ingests events into the specified logstream
// fails if the logstream does not exist
pub async fn post_event(req: HttpRequest, body: Bytes) -> Result<HttpResponse, PostError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    let internal_stream_names = PARSEABLE.streams.list_internal_streams();
    if internal_stream_names.contains(&stream_name) {
        return Err(PostError::Invalid(anyhow::anyhow!(
            "Stream {} is an internal stream and cannot be ingested into",
            stream_name
        )));
    }
    if !PARSEABLE.streams.stream_exists(&stream_name) {
        return Err(PostError::StreamNotFound(stream_name));
    }

    push_logs(req, body, stream_name).await?;
    Ok(HttpResponse::Ok().finish())
}