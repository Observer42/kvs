use crate::Result;

pub mod kv_store;

pub use kv_store::KvStore;

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
