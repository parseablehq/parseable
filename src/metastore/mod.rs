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

use crate::storage::ObjectStorageError;

pub mod metastore_traits;
pub mod metastores;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MetastoreErrorDetail {
    pub error_type: String,
    pub message: String,
    pub operation: Option<String>,
    pub stream_name: Option<String>,
    pub file_path: Option<String>,
    pub timestamp: Option<chrono::DateTime<chrono::Utc>>,
    pub metadata: std::collections::HashMap<String, String>,
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
}

impl MetastoreError {
    pub fn to_detail(&self) -> MetastoreErrorDetail {
        match self {
            MetastoreError::ObjectStorageError(e) => MetastoreErrorDetail {
                error_type: "ObjectStorageError".to_string(),
                message: e.to_string(),
                operation: None,
                stream_name: None,
                file_path: None,
                timestamp: Some(chrono::Utc::now()),
                metadata: std::collections::HashMap::new(),
            },
            MetastoreError::JsonParseError(e) => MetastoreErrorDetail {
                error_type: "JsonParseError".to_string(),
                message: e.to_string(),
                operation: None,
                stream_name: None,
                file_path: None,
                timestamp: Some(chrono::Utc::now()),
                metadata: std::collections::HashMap::new(),
            },
            MetastoreError::JsonSchemaError { message } => MetastoreErrorDetail {
                error_type: "JsonSchemaError".to_string(),
                message: message.clone(),
                operation: None,
                stream_name: None,
                file_path: None,
                timestamp: Some(chrono::Utc::now()),
                metadata: std::collections::HashMap::new(),
            },
            MetastoreError::InvalidJsonStructure { expected, found } => MetastoreErrorDetail {
                error_type: "InvalidJsonStructure".to_string(),
                message: format!("Expected {}, found {}", expected, found),
                operation: None,
                stream_name: None,
                file_path: None,
                timestamp: Some(chrono::Utc::now()),
                metadata: [
                    ("expected".to_string(), expected.clone()),
                    ("found".to_string(), found.clone()),
                ]
                .into_iter()
                .collect(),
            },
            MetastoreError::MissingJsonField { field } => MetastoreErrorDetail {
                error_type: "MissingJsonField".to_string(),
                message: format!("Missing required field: {}", field),
                operation: None,
                stream_name: None,
                file_path: None,
                timestamp: Some(chrono::Utc::now()),
                metadata: [("field".to_string(), field.clone())].into_iter().collect(),
            },
            MetastoreError::InvalidJsonValue { field, reason } => MetastoreErrorDetail {
                error_type: "InvalidJsonValue".to_string(),
                message: format!("Invalid value for field '{}': {}", field, reason),
                operation: None,
                stream_name: None,
                file_path: None,
                timestamp: Some(chrono::Utc::now()),
                metadata: [
                    ("field".to_string(), field.clone()),
                    ("reason".to_string(), reason.clone()),
                ]
                .into_iter()
                .collect(),
            },
        }
    }
}
