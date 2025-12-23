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

use http::StatusCode;
use serde::Serialize;

use crate::storage::ObjectStorageError;

pub mod metastore_traits;
pub mod metastores;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MetastoreErrorDetail {
    pub operation: String,
    pub message: String,
    pub stream_name: Option<String>,
    pub file_path: Option<String>,
    pub timestamp: Option<chrono::DateTime<chrono::Utc>>,
    pub metadata: std::collections::HashMap<String, String>,
    pub status_code: u16,
}

#[derive(Debug, thiserror::Error)]
pub enum MetastoreError {
    #[error("ObjectStorageError: {0}")]
    ObjectStorageError(#[from] ObjectStorageError),

    #[error("JSON parsing error: {0}")]
    JsonParseError(#[from] serde_json::Error),

    #[error("JSON schema validation error: {message}")]
    JsonSchemaError { message: String },

    #[error("Invalid JSON structure: expected {expected}, found {found}")]
    InvalidJsonStructure { expected: String, found: String },

    #[error("Missing required JSON field: {field}")]
    MissingJsonField { field: String },

    #[error("Invalid JSON value for field '{field}': {reason}")]
    InvalidJsonValue { field: String, reason: String },

    #[error("{self:?}")]
    Error {
        status_code: StatusCode,
        message: String,
        flow: String,
    },
}

impl MetastoreError {
    pub fn to_detail(&self) -> MetastoreErrorDetail {
        match self {
            MetastoreError::Error {
                status_code,
                message,
                flow,
            } => MetastoreErrorDetail {
                operation: flow.clone(),
                message: message.clone(),
                stream_name: None,
                file_path: None,
                timestamp: Some(chrono::Utc::now()),
                metadata: std::collections::HashMap::new(),
                status_code: status_code.as_u16(),
            },
            MetastoreError::ObjectStorageError(e) => MetastoreErrorDetail {
                operation: "ObjectStorageError".to_string(),
                message: e.to_string(),
                stream_name: None,
                file_path: None,
                timestamp: Some(chrono::Utc::now()),
                metadata: std::collections::HashMap::new(),
                status_code: 500,
            },
            MetastoreError::JsonParseError(e) => MetastoreErrorDetail {
                operation: "JsonParseError".to_string(),
                message: e.to_string(),
                stream_name: None,
                file_path: None,
                timestamp: Some(chrono::Utc::now()),
                metadata: std::collections::HashMap::new(),
                status_code: 400,
            },
            MetastoreError::JsonSchemaError { message } => MetastoreErrorDetail {
                operation: "JsonSchemaError".to_string(),
                message: message.clone(),
                stream_name: None,
                file_path: None,
                timestamp: Some(chrono::Utc::now()),
                metadata: std::collections::HashMap::new(),
                status_code: 400,
            },
            MetastoreError::InvalidJsonStructure { expected, found } => MetastoreErrorDetail {
                operation: "InvalidJsonStructure".to_string(),
                message: format!("Expected {}, found {}", expected, found),
                stream_name: None,
                file_path: None,
                timestamp: Some(chrono::Utc::now()),
                metadata: [
                    ("expected".to_string(), expected.clone()),
                    ("found".to_string(), found.clone()),
                ]
                .into_iter()
                .collect(),
                status_code: 400,
            },
            MetastoreError::MissingJsonField { field } => MetastoreErrorDetail {
                operation: "MissingJsonField".to_string(),
                message: format!("Missing required field: {}", field),
                stream_name: None,
                file_path: None,
                timestamp: Some(chrono::Utc::now()),
                metadata: [("field".to_string(), field.clone())].into_iter().collect(),
                status_code: 400,
            },
            MetastoreError::InvalidJsonValue { field, reason } => MetastoreErrorDetail {
                operation: "InvalidJsonValue".to_string(),
                message: format!("Invalid value for field '{}': {}", field, reason),
                stream_name: None,
                file_path: None,
                timestamp: Some(chrono::Utc::now()),
                metadata: [
                    ("field".to_string(), field.clone()),
                    ("reason".to_string(), reason.clone()),
                ]
                .into_iter()
                .collect(),
                status_code: 400,
            },
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            MetastoreError::ObjectStorageError(..) => StatusCode::INTERNAL_SERVER_ERROR,
            MetastoreError::JsonParseError(..) => StatusCode::INTERNAL_SERVER_ERROR,
            MetastoreError::JsonSchemaError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            MetastoreError::InvalidJsonStructure { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            MetastoreError::MissingJsonField { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            MetastoreError::InvalidJsonValue { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            MetastoreError::Error { status_code, .. } => *status_code,
        }
    }
}
