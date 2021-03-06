use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;

use log::info;

use crate::net::{Query, Response};
use crate::thread_pool::ThreadPool;
use crate::{KvsEngine, Result};

/// A TCP Server to handle queries from client
#[derive(Clone)]
pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    addr: SocketAddr,
    engine: E,
    thread_pool: Arc<Mutex<P>>,
    stop: Arc<AtomicBool>,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    /// Initialize the key-value server
    pub fn init(engine: E, addr: SocketAddr, thread_pool: P) -> Result<Self> {
        Ok(Self {
            addr,
            engine,
            thread_pool: Arc::new(Mutex::new(thread_pool)),
            stop: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Start the server to serve client queries
    pub fn start(&self) -> JoinHandle<Result<()>> {
        let addr = self.addr;
        let thread_pool = self.thread_pool.clone();
        let engine = self.engine.clone();
        let stop_sign = self.stop.clone();

        thread::spawn(move || {
            let pool_lock = thread_pool.lock().unwrap();
            let listener = TcpListener::bind(addr)?;
            for stream in listener.incoming() {
                if stop_sign.load(Ordering::Acquire) {
                    break;
                }
                if let Ok(stream) = stream {
                    info!("serving: {:?}", stream.peer_addr()?);
                    let engine = engine.clone();

                    pool_lock.spawn(move || {
                        handle(stream, engine).unwrap();
                    });
                }
            }
            Ok(())
        })
    }

    /// Stop the server
    pub fn stop_server(&self) {
        self.stop.store(true, Ordering::Release);
        let _stream = TcpStream::connect(&self.addr);
    }
}

impl<E: KvsEngine, P: ThreadPool> Drop for KvsServer<E, P> {
    fn drop(&mut self) {
        self.stop_server();
    }
}

fn handle<E: KvsEngine>(mut stream: TcpStream, engine: E) -> Result<()> {
    let query = receive(&mut stream)?;
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
    send(&mut stream, response)?;
    Ok(())
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
