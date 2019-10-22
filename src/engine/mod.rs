pub mod kv_store;
pub mod sled_engine;

pub use kv_store::KvStore;
pub use sled_engine::SledKvsEngine;

use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;

use crate::{KvsError, Result};
use std::str::FromStr;

const ENGINE_TYPE_SLED: &str = "sled";
const ENGINE_TYPE_KVSTORE: &str = "kvs";

/// Trait for key-value store
pub trait KvsEngine {
    /// get the value from the store for a given key.
    ///
    /// return `Ok(None)` if the key does not exist.
    fn get(&mut self, key: String) -> Result<Option<String>>;
    /// set a key-value pair into the store.
    ///
    /// if the key already exists, the value will be updated.
    fn set(&mut self, key: String, value: String) -> Result<()>;
    /// remove the key from the store.
    fn remove(&mut self, key: String) -> Result<()>;
}

/// Engine Type: sled or kv_store
#[derive(PartialEq, Eq, Copy, Clone, Debug)]
pub enum EngineType {
    /// sled engine
    Sled,
    /// kv_store engine
    KvStore,
}

impl FromStr for EngineType {
    type Err = KvsError;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            ENGINE_TYPE_KVSTORE => Ok(EngineType::KvStore),
            ENGINE_TYPE_SLED => Ok(EngineType::Sled),
            _ => Err(KvsError::WrongEngine),
        }
    }
}

impl Display for EngineType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineType::Sled => write!(f, "{}", ENGINE_TYPE_SLED),
            EngineType::KvStore => write!(f, "{}", ENGINE_TYPE_KVSTORE),
        }
    }
}

fn try_add_engine_type(log_dir: &PathBuf, engine_type: EngineType) -> Result<()> {
    let engine_file = log_dir.join(".engine");
    if engine_file.exists() {
        let mut file = File::open(engine_file)?;
        let mut engine_str = String::new();
        file.read_to_string(&mut engine_str)?;
        let actual_type = EngineType::from_str(&engine_str)?;
        if actual_type == engine_type {
            Ok(())
        } else {
            Err(KvsError::WrongEngine)
        }
    } else {
        let mut file = File::create(engine_file)?;
        file.write_all(engine_type.to_string().as_bytes())?;
        Ok(())
    }
}
