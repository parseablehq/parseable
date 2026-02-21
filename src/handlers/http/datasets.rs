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
use actix_web::{HttpResponse, web};
use serde::{Deserialize, Serialize};

use crate::{
    handlers::DatasetTag,
    parseable::PARSEABLE,
    storage::{ObjectStorageError, StreamType},
};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CorrelatedDataset {
    name: String,
    shared_tags: Vec<DatasetTag>,
    shared_labels: Vec<String>,
}

/// GET /api/v1/datasets/{name}/correlated
/// Returns all datasets sharing at least one tag or label with the named dataset.
pub async fn get_correlated_datasets(
    path: web::Path<String>,
) -> Result<HttpResponse, DatasetsError> {
    let dataset_name = path.into_inner();

    let stream = PARSEABLE
        .get_stream(&dataset_name)
        .map_err(|_| DatasetsError::DatasetNotFound(dataset_name.clone()))?;

    let target_tags: HashSet<DatasetTag> = stream.get_dataset_tags().into_iter().collect();
    let target_labels: HashSet<String> = stream.get_dataset_labels().into_iter().collect();

    if target_tags.is_empty() && target_labels.is_empty() {
        return Ok(HttpResponse::Ok().json(Vec::<CorrelatedDataset>::new()));
    }

    let all_streams = PARSEABLE.streams.list();
    let mut correlated = Vec::new();

    for name in all_streams {
        if name == dataset_name {
            continue;
        }
        if let Ok(s) = PARSEABLE.get_stream(&name) {
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
                correlated.push(CorrelatedDataset {
                    name,
                    shared_tags,
                    shared_labels,
                });
            }
        }
    }

    Ok(HttpResponse::Ok().json(correlated))
}

/// GET /api/v1/datasets/tags/{tag}
/// Returns all datasets that have the specified tag.
pub async fn get_datasets_by_tag(path: web::Path<String>) -> Result<HttpResponse, DatasetsError> {
    let tag_str = path.into_inner();
    let tag =
        DatasetTag::try_from(tag_str.as_str()).map_err(|_| DatasetsError::InvalidTag(tag_str))?;

    let all_streams = PARSEABLE.streams.list();
    let mut matching = Vec::new();

    for name in all_streams {
        if let Ok(s) = PARSEABLE.get_stream(&name) {
            if s.get_stream_type() == StreamType::Internal {
                continue;
            }
            if s.get_dataset_tags().contains(&tag) {
                matching.push(name);
            }
        }
    }

    Ok(HttpResponse::Ok().json(matching))
}

#[derive(Debug, Deserialize)]
pub struct PutTagsBody {
    pub tags: Vec<DatasetTag>,
}

#[derive(Debug, Deserialize)]
pub struct PutLabelsBody {
    pub labels: Vec<String>,
}

/// PUT /api/v1/datasets/{name}/tags
/// Replaces the dataset's tags with the provided list.
pub async fn put_dataset_tags(
    path: web::Path<String>,
    body: web::Json<PutTagsBody>,
) -> Result<HttpResponse, DatasetsError> {
    let dataset_name = path.into_inner();
    let new_tags: Vec<DatasetTag> = body
        .into_inner()
        .tags
        .into_iter()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    let stream = PARSEABLE
        .get_stream(&dataset_name)
        .map_err(|_| DatasetsError::DatasetNotFound(dataset_name.clone()))?;

    // Update storage first, then in-memory
    let storage = PARSEABLE.storage.get_object_store();
    let existing_labels = stream.get_dataset_labels();
    storage
        .update_dataset_tags_and_labels_in_stream(&dataset_name, &new_tags, &existing_labels)
        .await
        .map_err(DatasetsError::Storage)?;

    stream.set_dataset_tags(new_tags.clone());

    Ok(HttpResponse::Ok().json(serde_json::json!({ "tags": new_tags })))
}

/// PUT /api/v1/datasets/{name}/labels
/// Replaces the dataset's labels with the provided list.
pub async fn put_dataset_labels(
    path: web::Path<String>,
    body: web::Json<PutLabelsBody>,
) -> Result<HttpResponse, DatasetsError> {
    let dataset_name = path.into_inner();
    let new_labels: Vec<String> = body
        .into_inner()
        .labels
        .into_iter()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    let stream = PARSEABLE
        .get_stream(&dataset_name)
        .map_err(|_| DatasetsError::DatasetNotFound(dataset_name.clone()))?;

    // Update storage first, then in-memory
    let storage = PARSEABLE.storage.get_object_store();
    let existing_tags = stream.get_dataset_tags();
    storage
        .update_dataset_tags_and_labels_in_stream(&dataset_name, &existing_tags, &new_labels)
        .await
        .map_err(DatasetsError::Storage)?;

    stream.set_dataset_labels(new_labels.clone());

    Ok(HttpResponse::Ok().json(serde_json::json!({ "labels": new_labels })))
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
