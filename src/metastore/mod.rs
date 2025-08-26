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

use http::StatusCode;

use crate::storage::ObjectStorageError;

pub mod metastore_traits;
pub mod metastores;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetastoreErrorDetail {
    pub flow: String,
    pub message: String,
    pub operation: Option<String>,
    pub stream_name: Option<String>,
    pub file_path: Option<String>,
    pub timestamp: Option<chrono::DateTime<chrono::Utc>>,
    pub metadata: std::collections::HashMap<String, String>,
    pub status_code: StatusCode,
}

#[derive(Debug, thiserror::Error)]
pub enum MetastoreError {
    #[error("ObjectStorageError: {0}")]
    ObjectStorageError(#[from] ObjectStorageError),

    #[error("{self:?}")]
    ObjectStoreError {
        status_code: StatusCode,
        message: String,
        flow: String,
    },
}

impl MetastoreError {
    pub fn to_detail(self) -> MetastoreErrorDetail {
        match self {
            MetastoreError::ObjectStoreError {
                status_code,
                message,
                flow,
            } => MetastoreErrorDetail {
                flow,
                message,
                operation: None,
                stream_name: None,
                file_path: None,
                timestamp: Some(chrono::Utc::now()),
                metadata: std::collections::HashMap::new(),
                status_code,
            },
            MetastoreError::ObjectStorageError(e) => MetastoreErrorDetail {
                flow: "ObjectStorageError".to_string(),
                message: e.to_string(),
                operation: None,
                stream_name: None,
                file_path: None,
                timestamp: Some(chrono::Utc::now()),
                metadata: std::collections::HashMap::new(),
                status_code: StatusCode::INTERNAL_SERVER_ERROR,
            },
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            MetastoreError::ObjectStorageError(_object_storage_error) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            MetastoreError::ObjectStoreError {
                status_code,
                message: _,
                flow: _,
            } => *status_code,
        }
    }
}
