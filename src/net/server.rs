use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};

use crate::net::{Query, Response};
use crate::{KvsEngine, KvsError, Result};

/// A TCP Server to handle queries from client
pub struct KvsServer<T> {
    listener: TcpListener,
    engine: T,
}

impl<T: KvsEngine> KvsServer<T> {
    /// initialize the key-value server
    pub fn init(engine: T, addr: &SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind(addr)?;

        Ok(Self { listener, engine })
    }
}
