use std::backtrace::Backtrace;
use thiserror::Error;
use std::convert::{Into, From};

#[derive(Debug,)]
pub struct BackendClientError {
    location: std::panic::Location,
    backtrace: std::backtrace::Backtrace,
    error: ServerErrors,
}

impl from<E> for BackendClientError 
where E: Into<ServerErrors> 
{
    #[track_caller]
    fn from(value: E) -> Self {

        let location = std::panic::Location::caller();
        let backtrace = Backtrace::capture();

        Self {
            location,
            backtrace,
            error: value.into()
        }
    }
}

impl std::fmt::Display for BackendClientError  {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let err = self.error.to_str();
        write!(f, "BackendServerError: {err} \n location: {}", self.location)
    }
}

#[derive(Error, Debug)]
enum ClientError  {
    #[error("I/O Error found")]
    IoErr(#[from] std::io::Error),
    /*
    we can add our types accordingly! 

    */
    #[error("Undefined Error")]
    UndefinedError(Box<dyn std::error::Error + Send + Sync>)
}