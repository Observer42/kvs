use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::net::{Query, Response};
use crate::{KvsError, Result};

/// A TCP client to interact with key-value server
pub struct KvsClient {
    stream: TcpStream,
}

impl KvsClient {
    /// initiate a connection to remote socket
    pub async fn init(addr: &SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self { stream })
    }

    /// query value from server for the given key
    pub async fn get(&mut self, key: String) -> Result<Option<String>> {
        let query = Query::Get(key);
        self.send(query).await?;

        match self.receive().await? {
            Response::Ok(val) => Ok(val),
            Response::KeyNotFound => Err(KvsError::KeyNotFound),
            Response::Err => Err(KvsError::ServerError),
            _ => unreachable!(),
        }
    }

    /// set key value pair to server
    pub async fn set(&mut self, key: String, val: String) -> Result<()> {
        let query = Query::Set(key, val);
        self.send(query).await?;
        match self.receive().await? {
            Response::Success => Ok(()),
            Response::Err => Err(KvsError::ServerError),
            _ => unreachable!(),
        }
    }

    /// remove key-value pair from server for the given key
    pub async fn remove(&mut self, key: String) -> Result<()> {
        let query = Query::Rm(key);
        self.send(query).await?;
        match self.receive().await? {
            Response::Success => Ok(()),
            Response::KeyNotFound => Err(KvsError::KeyNotFound),
            Response::Err => Err(KvsError::ServerError),
            _ => unreachable!(),
        }
    }

    async fn send(&mut self, query: Query) -> Result<()> {
        let serialized_query = serde_json::to_vec(&query)?;
        self.stream
            .write_all(&(serialized_query.len() as u32).to_be_bytes())
            .await?;
        self.stream.write_all(&serialized_query).await?;
        Ok(())
    }

    async fn receive(&mut self) -> Result<Response> {
        let mut msg_len = [0; 4];
        self.stream.read_exact(&mut msg_len).await?;
        let len = u32::from_be_bytes(msg_len) as usize;
        let mut msg = vec![0; len];
        self.stream.read_exact(&mut msg).await?;
        serde_json::from_slice::<Response>(&msg).map_err(|e| e.into())
    }
}
