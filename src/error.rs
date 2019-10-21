use std::io;

/// result type for kvs crate
pub type Result<T> = std::result::Result<T, KvsError>;

/// Error type for kvs crate
#[derive(Debug, Fail)]
pub enum KvsError {
    /// Io Error
    #[fail(display = "io error: {}", _0)]
    Io(io::Error),
    /// SerdeJson Error
    #[fail(display = "serde_json error: {}", _0)]
    SerdeJson(serde_json::error::Error),
    /// Key not found
    #[fail(display = "key not found")]
    KeyNotFound,
    /// Unknown server error
    #[fail(display = "server error")]
    ServerError,
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> Self {
        KvsError::Io(err)
    }
}

impl From<serde_json::error::Error> for KvsError {
    fn from(err: serde_json::error::Error) -> Self {
        KvsError::SerdeJson(err)
    }
}
