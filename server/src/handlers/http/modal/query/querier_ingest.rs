use actix_web::{HttpRequest, HttpResponse};
use bytes::Bytes;
use crate::handlers::http::ingest::PostError;

// Handler for POST /api/v1/logstream/{logstream}
// only ingests events into the specified logstream
// fails if the logstream does not exist
#[allow(unused)]
pub async fn post_event(req: HttpRequest, body: Bytes) -> Result<HttpResponse, PostError> {
    Err(PostError::Invalid(anyhow::anyhow!(
        "Ingestion is not allowed in Query mode"
    )))
}