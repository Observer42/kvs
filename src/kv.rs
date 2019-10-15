use std::collections::HashMap;

use crate::Result;
use std::path::Path;

/// A simple key-value store implementation
///
/// Examples:
/// ```rust
/// use kvs::KvStore;
///
/// let mut store = KvStore::new();
/// store.set("abc".to_string(), "def".to_string());
/// assert_eq!(store.get("abc".to_string()), Some("def".to_string()));
/// assert_eq!(store.get("ijk".to_string()), None);
/// store.remove("abc".to_string());
/// assert_eq!(store.get("abc".to_string()), None);
/// ```
#[derive(Default)]
pub struct KvStore {
    store: HashMap<String, String>,
}

impl KvStore {
    /// create a `KvStore`.
    pub fn new() -> Self {
        Default::default()
    }

    /// load the kv store from disk
    pub fn open<T: AsRef<Path>>(_dir: T) -> Result<Self> {
        Ok(Default::default())
    }

    /// set a key-value pair into the store.
    ///
    /// if the key already exists, the value will be updated.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.store.insert(key, value);
        Ok(())
    }

    /// get the value from the store for a given key.
    ///
    /// return `None` if the key does not exist.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self.store.get(&key).cloned())
    }

    /// remove the key from the store.
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.store.remove(&key);
        Ok(())
    }
}
