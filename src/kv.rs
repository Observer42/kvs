use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::Result;

#[derive(Serialize, Deserialize)]
enum Op {
    Set(String, String),
    Rm(String),
}

struct LogIndex {
    file_index: usize,
    offset: usize,
}

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
pub struct KvStore {
    path: PathBuf,
    key_index: HashMap<String, LogIndex>,
    index_scanned: usize,
    commands: Vec<Op>,
    mutable_file: File,
    wal_logs: Vec<File>,
}

impl Drop for KvStore {
    fn drop(&mut self) {
        // todo: compact mutable file
    }
}

impl KvStore {
    /// load the kv store from disk
    pub fn open<T: AsRef<Path>>(dir: T) -> Result<Self> {
        let mut log_dir = PathBuf::new();
        log_dir.push(dir);

        let mut files = vec![];
        for entry_result in log_dir.read_dir()? {
            let entry = entry_result?;
            if entry.metadata()?.is_file() {
                files.push(entry);
            }
        }
        files.sort_by_key(|item| item.file_name());

        let mutable_file =
            if files.is_empty() || files.last().unwrap().metadata()?.len() > 1_000_000 {
                let mut path = log_dir.clone();
                let name = format!("wal_{}.log", files.len());
                path.join(name);
                File::create(path)?
            } else {
                OpenOptions::new()
                    .append(true)
                    .open(files.last().unwrap().path())?
            };

        let mut wal_logs = vec![];
        for entry in files {
            let file = File::open(entry.path())?;
            wal_logs.push(file);
        }

        let key_index = Self::generate_index(&mut wal_logs)?;

        Ok(Self {
            path: log_dir,
            key_index,
            index_scanned: wal_logs.len(),
            commands: vec![],
            mutable_file,
            wal_logs,
        })
    }

    fn generate_index(logs: &mut Vec<File>) -> Result<HashMap<String, LogIndex>> {
        unimplemented!()
    }

    /// set a key-value pair into the store.
    ///
    /// if the key already exists, the value will be updated.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        unimplemented!()
    }

    /// get the value from the store for a given key.
    ///
    /// return `None` if the key does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        unimplemented!();
    }

    /// remove the key from the store.
    pub fn remove(&mut self, key: String) -> Result<()> {
        unimplemented!();
    }
}
