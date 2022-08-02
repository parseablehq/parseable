/*
 * Parseable Server (C) 2022 Parseable, Inc.
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

use arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use parquet::errors::ParquetError;

use crate::{response::EventError, storage::ObjectStorageError};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde_json error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("error parsing time: {0}")]
    TimeParse(#[from] chrono::ParseError),
    #[error("JSON provided to query api doesn't contain {0}")]
    JsonQuery(&'static str),
    #[error("Storage error: {0}")]
    Storage(Box<dyn ObjectStorageError>),
    #[error("Event error: {0}")]
    Event(#[from] EventError),
    #[error("Parquet error: {0}")]
    Parquet(#[from] ParquetError),
    #[error("Arrow error: {0}")]
    Arrow(#[from] ArrowError),
    #[error("Data Fusion error: {0}")]
    DataFusion(#[from] DataFusionError),
    #[error("UTF8 parsing error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("log stream name cannot be empty")]
    EmptyName,
    #[error("log stream name cannot contain spaces: {0}")]
    NameWhiteSpace(String),
    #[error("log stream name cannot contain special characters: {0}")]
    NameSpecialChar(String),
    #[error("log stream name cannot contain uppercase characters: {0}")]
    NameUpperCase(String),
    #[error("log stream name cannot be numeric only: {0}")]
    NameNumericOnly(String),
    #[error("log stream name cannot start with a number: {0}")]
    NameCantStartWithNumber(String),
    #[error("log stream name cannot be a sql keyword: {0}")]
    SQLKeyword(String),
    #[error("queries across multiple streams are not supported currently: {0}")]
    MultipleStreams(String),
    #[error("start time can not be later than end time")]
    StartTimeAfterEndTime(),
    #[error("query is incomplete")]
    IncompleteQuery(),
    #[error("query cannot be empty")]
    EmptyQuery,
    #[error("start time cannot be empty in query")]
    EmptyStartTime,
    #[error("end time cannot be empty in query")]
    EmptyEndTime,
    #[error("joins are not supported currently: {0}")]
    Join(String),
    #[error("missing record batch")]
    MissingRecord,
    #[error("metadata not found for log stream: {0}")]
    StreamMetaNotFound(String),
    #[error("invalid alert config: {0}")]
    InvalidAlert(String),
    #[error("this event schema doesn't match with stream schema. please ensure event data is in same format as previous events sent to the stream: {0}")]
    SchemaMismatch(String),
}
