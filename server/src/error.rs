use arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use parquet::errors::ParquetError;

use crate::response::EventError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(std::io::Error),
    #[error("serde_json error: {0}")]
    Serde(serde_json::Error),
    #[error("Event error: {0}")]
    Event(EventError),
    #[error("Parquet error: {0}")]
    Parquet(ParquetError),
    #[error("Arrow error: {0}")]
    Arrow(ArrowError),
    #[error("Data Fusion error: {0}")]
    DataFusion(DataFusionError),
    #[error("logstream name cannot be empty")]
    EmptyName,
    #[error("logstream name cannot contain spaces: {0}")]
    NameWhiteSpace(String),
    #[error("logstream name cannot contain special characters: {0}")]
    NameSpecialChar(String),
    #[error("logstream name cannot contain uppercase characters: {0}")]
    NameUpperCase(String),
    #[error("logstream name cannot be numeric only: {0}")]
    NameNumericOnly(String),
    #[error("logstream name cannot be a sql keyword: {0}")]
    SQLKeyword(String),
    #[error("queries across multiple streams are not supported currently: {0}")]
    MultipleStreams(String),
    #[error("query cannot be empty")]
    Empty,
    #[error("joins are not supported currently: {0}")]
    Join(String),
    #[error("Missing record batch")]
    MissingRecord,
    #[error("Missing path information")]
    MissingPath,
    #[error("Couldn't get lock on MEM_STREAMS")]
    StreamLock,
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
