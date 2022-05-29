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
use aws_sdk_s3::Error as AWSS3Error;
use datafusion::error::DataFusionError;
use parquet::errors::ParquetError;

use crate::response::EventError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(std::io::Error),
    #[error("serde_json error: {0}")]
    Serde(serde_json::Error),
    #[error("S3 error: {0}")]
    S3(AWSS3Error),
    #[error("Event error: {0}")]
    Event(EventError),
    #[error("Parquet error: {0}")]
    Parquet(ParquetError),
    #[error("Arrow error: {0}")]
    Arrow(ArrowError),
    #[error("Data Fusion error: {0}")]
    DataFusion(DataFusionError),
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
    #[error("query cannot be empty")]
    Empty,
    #[error("joins are not supported currently: {0}")]
    Join(String),
    #[error("Missing record batch")]
    MissingRecord,
    #[error("Couldn't get lock on STREAM_INFO")]
    StreamLock,
    #[error("Metadata not found for log stream: {0}")]
    StreamMetaNotFound(String),
    #[error("Schema not found for log stream: {0}")]
    SchemaNotFound(String),
    #[error("Alert config not found for log stream: {0}")]
    AlertConfigNotFound(String),
    #[error("Invalid alert config: {0}")]
    InvalidAlert(String),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::Io(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Error {
        Error::Serde(e)
    }
}

impl From<EventError> for Error {
    fn from(e: EventError) -> Error {
        Error::Event(e)
    }
}

impl From<ParquetError> for Error {
    fn from(e: ParquetError) -> Error {
        Error::Parquet(e)
    }
}

impl From<ArrowError> for Error {
    fn from(e: ArrowError) -> Error {
        Error::Arrow(e)
    }
}

impl From<AWSS3Error> for Error {
    fn from(e: AWSS3Error) -> Error {
        Error::S3(e)
    }
}
