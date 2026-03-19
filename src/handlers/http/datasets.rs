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

use std::collections::HashSet;

use actix_web::http::StatusCode;
use actix_web::{HttpRequest, HttpResponse, web};
use serde::{Deserialize, Serialize};

use crate::event::format::LogSource;
use crate::rbac::{self, Users, role::Action};
use crate::utils::actix::extract_session_key_from_req;
use crate::utils::get_tenant_id_from_request;
use crate::{
    handlers::DatasetTag,
    parseable::PARSEABLE,
    storage::{ObjectStorageError, StreamType},
};

/// Check if the caller is authorized to read a specific stream.
fn can_access_stream(req: &HttpRequest, stream_name: &str) -> bool {
    let Ok(key) = extract_session_key_from_req(req) else {
        return false;
    };
    Users.authorize(key, Action::GetStreamInfo, Some(stream_name), None)
        == rbac::Response::Authorized
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CorrelatedDataset {
    name: String,
    log_source: LogSource,
    shared_tags: Vec<DatasetTag>,
    shared_labels: Vec<String>,
}

/// GET /api/v1/datasets/correlated/{name}
/// Returns all datasets sharing at least one tag or label with the named dataset.
/// Results are filtered to only include datasets the caller is authorized to read.
pub async fn get_correlated_datasets(
    req: HttpRequest,
    path: web::Path<String>,
) -> Result<HttpResponse, DatasetsError> {
    let dataset_name = path.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);

    // Authorize caller for the seed dataset
    if !can_access_stream(&req, &dataset_name) {
        return Err(DatasetsError::DatasetNotFound(dataset_name));
    }

    if !PARSEABLE
        .check_or_load_stream(&dataset_name, &tenant_id)
        .await
    {
        return Err(DatasetsError::DatasetNotFound(dataset_name));
    }

    let stream = PARSEABLE
        .get_stream(&dataset_name, &tenant_id)
        .map_err(|_| DatasetsError::DatasetNotFound(dataset_name.clone()))?;

    let target_tags: HashSet<DatasetTag> = stream.get_dataset_tags().into_iter().collect();
    let target_labels: HashSet<String> = stream.get_dataset_labels().into_iter().collect();

    if target_tags.is_empty() && target_labels.is_empty() {
        return Ok(HttpResponse::Ok().json(Vec::<CorrelatedDataset>::new()));
    }

    let all_streams = PARSEABLE.streams.list(&tenant_id);
    let mut correlated = Vec::new();

    for name in all_streams {
        if name == dataset_name {
            continue;
        }
        // Filter out datasets the caller cannot read
        if !can_access_stream(&req, &name) {
            continue;
        }
        if let Ok(s) = PARSEABLE.get_stream(&name, &tenant_id) {
            // Skip internal streams
            if s.get_stream_type() == StreamType::Internal {
                continue;
            }

            let s_tags: HashSet<DatasetTag> = s.get_dataset_tags().into_iter().collect();
            let s_labels: HashSet<String> = s.get_dataset_labels().into_iter().collect();

            let shared_tags: Vec<DatasetTag> = target_tags.intersection(&s_tags).copied().collect();
            let shared_labels: Vec<String> =
                target_labels.intersection(&s_labels).cloned().collect();

            if !shared_tags.is_empty() || !shared_labels.is_empty() {
                let log_source = s
                    .get_log_source()
                    .first()
                    .map(|entry| entry.log_source_format.clone())
                    .unwrap_or_default();
                correlated.push(CorrelatedDataset {
                    name,
                    log_source,
                    shared_tags,
                    shared_labels,
                });
            }
        }
    }

    Ok(HttpResponse::Ok().json(correlated))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TaggedDataset {
    name: String,
    log_source: LogSource,
}

/// GET /api/v1/datasets/tags/{tag}
/// Returns all datasets that have the specified tag.
/// Results are filtered to only include datasets the caller is authorized to read.
pub async fn get_datasets_by_tag(
    req: HttpRequest,
    path: web::Path<String>,
) -> Result<HttpResponse, DatasetsError> {
    let tenant_id = get_tenant_id_from_request(&req);
    let tag_str = path.into_inner();
    let tag =
        DatasetTag::try_from(tag_str.as_str()).map_err(|_| DatasetsError::InvalidTag(tag_str))?;

    let all_streams = PARSEABLE.streams.list(&tenant_id);
    let mut matching = Vec::new();

    for name in all_streams {
        // Filter out datasets the caller cannot read
        if !can_access_stream(&req, &name) {
            continue;
        }
        if let Ok(s) = PARSEABLE.get_stream(&name, &tenant_id) {
            if s.get_stream_type() == StreamType::Internal {
                continue;
            }
            if s.get_dataset_tags().contains(&tag) {
                let log_source = s
                    .get_log_source()
                    .first()
                    .map(|entry| entry.log_source_format.clone())
                    .unwrap_or_default();
                matching.push(TaggedDataset { name, log_source });
            }
        }
    }

    Ok(HttpResponse::Ok().json(matching))
}

#[derive(Debug, Deserialize)]
pub struct PutDatasetMetadataBody {
    pub tags: Option<Vec<DatasetTag>>,
    pub labels: Option<Vec<String>>,
}

/// PUT /api/v1/datasets/{name}
/// Replaces the dataset's tags and/or labels.
/// Only fields present in the body are updated; absent fields are left unchanged.
pub async fn put_dataset_metadata(
    req: HttpRequest,
    path: web::Path<String>,
    body: web::Json<PutDatasetMetadataBody>,
) -> Result<HttpResponse, DatasetsError> {
    let dataset_name = path.into_inner();
    let body = body.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);

    if !PARSEABLE
        .check_or_load_stream(&dataset_name, &tenant_id)
        .await
    {
        return Err(DatasetsError::DatasetNotFound(dataset_name));
    }

    let stream = PARSEABLE
        .get_stream(&dataset_name, &tenant_id)
        .map_err(|_| DatasetsError::DatasetNotFound(dataset_name.clone()))?;

    let final_tags = match body.tags {
        Some(tags) => tags
            .into_iter()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect(),
        None => stream.get_dataset_tags(),
    };
    let final_labels = match body.labels {
        Some(labels) => labels
            .into_iter()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect(),
        None => stream.get_dataset_labels(),
    };

    // Update storage first, then in-memory
    let storage = PARSEABLE.storage.get_object_store();
    storage
        .update_dataset_tags_and_labels_in_stream(
            &dataset_name,
            &final_tags,
            &final_labels,
            &tenant_id,
        )
        .await
        .map_err(DatasetsError::Storage)?;

    stream.set_dataset_tags(final_tags.clone());
    stream.set_dataset_labels(final_labels.clone());

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "tags": final_tags,
        "labels": final_labels,
    })))
}

#[derive(Debug, thiserror::Error)]
pub enum DatasetsError {
    #[error("Dataset not found: {0}")]
    DatasetNotFound(String),
    #[error("Invalid tag: {0}")]
    InvalidTag(String),
    #[error("Storage error: {0}")]
    Storage(ObjectStorageError),
}

impl actix_web::ResponseError for DatasetsError {
    fn status_code(&self) -> StatusCode {
        match self {
            DatasetsError::DatasetNotFound(_) => StatusCode::NOT_FOUND,
            DatasetsError::InvalidTag(_) => StatusCode::BAD_REQUEST,
            DatasetsError::Storage(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).json(serde_json::json!({
            "error": self.to_string()
        }))
    }
}
