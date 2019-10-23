use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};

use log::info;

use crate::net::{Query, Response};
use crate::{KvsEngine, Result};

/// A TCP Server to handle queries from client
pub struct KvsServer<T> {
    listener: TcpListener,
    engine: T,
}

impl<T: KvsEngine> KvsServer<T> {
    /// Initialize the key-value server
    pub fn init(engine: T, addr: &SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind(addr)?;

        Ok(Self { listener, engine })
    }

    /// Start the server to serve client queries
    pub fn serve(&self) -> Result<()> {
        for stream in self.listener.incoming() {
            if let Ok(mut stream) = stream {
                info!("serving: {:?}", stream.peer_addr()?);
                let _ = Self::handle(&mut stream, &self.engine);
            }
        }
        Ok(())
    }

    fn handle(stream: &mut TcpStream, engine: &T) -> Result<()> {
        let query = receive(stream)?;
        let response = match query {
            Query::Set(key, val) => match engine.set(key, val) {
                Ok(_) => Response::Success,
                Err(_) => Response::Err,
            },
            Query::Get(key) => match engine.get(key) {
                Ok(val) => Response::Ok(val),
                Err(_) => Response::Err,
            },
            Query::Rm(key) => match engine.remove(key) {
                Ok(_) => Response::Success,
                Err(_) => Response::Err,
            },
        };
        send(stream, response)?;
        Ok(())
    }
}

fn receive(stream: &mut TcpStream) -> Result<Query> {
    let mut msg_len = [0; 4];
    stream.read_exact(&mut msg_len)?;
    let len = u32::from_be_bytes(msg_len) as usize;
    let mut msg = vec![0; len];
    stream.read_exact(&mut msg)?;
    serde_json::from_slice::<Query>(&msg).map_err(|e| e.into())
}

fn send(stream: &mut TcpStream, response: Response) -> Result<()> {
    let serialized_query = serde_json::to_vec(&response)?;
    stream.write_all(&(serialized_query.len() as u32).to_be_bytes())?;
    stream.write_all(&serialized_query)?;
    Ok(())
}
