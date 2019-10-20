#![deny(missing_docs)]
//! a simple key-value store

#[macro_use]
extern crate failure;

mod error;
mod kv;

pub use error::{KvsError, Result};
pub use kv::{KvStore, KvsEngine};
