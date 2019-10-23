#![deny(missing_docs)]
//! a simple key-value store

#[macro_use]
extern crate failure;

mod engine;
mod error;
mod net;
pub mod thread_pool;

pub use engine::{EngineType, KvStore, KvsEngine, SledKvsEngine};
pub use error::{KvsError, Result};
pub use net::{KvsClient, KvsServer};
