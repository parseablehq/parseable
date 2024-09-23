use actix_web::{HttpRequest, HttpResponse};
use bytes::Bytes;

use crate::{handlers::http::{ingest::PostError, modal::utils::ingest_utils::flatten_and_push_logs}, metadata::STREAM_INFO};


// Handler for POST /api/v1/logstream/{logstream}
// only ingests events into the specified logstream
// fails if the logstream does not exist
pub async fn post_event(req: HttpRequest, body: Bytes) -> Result<HttpResponse, PostError> {
    let stream_name: String = req.match_info().get("logstream").unwrap().parse().unwrap();
    let internal_stream_names = STREAM_INFO.list_internal_streams();
    if internal_stream_names.contains(&stream_name) {
        return Err(PostError::Invalid(anyhow::anyhow!(
            "Stream {} is an internal stream and cannot be ingested into",
            stream_name
        )));
    }
    if !STREAM_INFO.stream_exists(&stream_name) {
        return Err(PostError::StreamNotFound(stream_name));
    }

    flatten_and_push_logs(req, body, stream_name).await?;
    Ok(HttpResponse::Ok().finish())
}