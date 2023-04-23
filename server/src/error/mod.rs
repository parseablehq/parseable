

mod client;
mod server;

// A type for working with server side errors
pub type BackendServerResult<T> = std::result::Result<T, BackendServerError>;

//A type for working with Client side errors
pub type BackendClientResult<T> = std::result::Result<T, BackendClientError>;
