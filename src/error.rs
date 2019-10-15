use std::io;

/// result type for kvs crate
pub type Result<T> = std::result::Result<T, KvsError>;

/// Error type for kvs crate
#[derive(Debug, Fail)]
pub enum KvsError {
    /// Io Error
    #[fail(display = "io error: {}", _0)]
    Io(io::Error),
    /// Key not found
    #[fail(display = "key not found")]
    KeyNotFound,
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> Self {
        KvsError::Io(err)
    }
}
