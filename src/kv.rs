use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::Result;
use std::io::SeekFrom;

const BUFFER_SIZE: usize = 2048;
const USIZE_LEN: usize = std::mem::size_of::<usize>();

#[derive(Serialize, Deserialize)]
enum Cmd {
    Set(String, String),
    Rm(String),
}

struct LogIndex {
    file_index: usize,
    offset: u64,
}

impl LogIndex {
    fn new(file_index: usize, offset: u64) -> Self {
        Self { file_index, offset }
    }
}

/// A simple key-value store implementation
///
/// Examples:
/// ```rust
/// use kvs::KvStore;
///
/// let mut store = KvStore::open(std::env::current_dir().unwrap())?;
/// store.set("abc".to_string(), "def".to_string())?;
/// assert_eq!(store.get("abc".to_string())?, Some("def".to_string()));
/// assert_eq!(store.get("ijk".to_string())?, None);
/// store.remove("abc".to_string())?;
/// assert_eq!(store.get("abc".to_string())?, None);
/// ```
pub struct KvStore {
    path: PathBuf,
    key_index: HashMap<String, LogIndex>,
    index_scanned: usize,
    commands: Vec<Cmd>,
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

        let mut wal_logs = vec![];
        for entry in &files {
            let file = File::open(entry.path())?;
            if entry.file_name().to_str().unwrap().contains(".log") {
                wal_logs.push(file);
            }
        }

        let mutable_file = if files.is_empty()
            || !files
                .last()
                .unwrap()
                .file_name()
                .to_str()
                .unwrap()
                .contains("active")
        {
            let path = log_dir.join("wal_active".to_string());
            File::create(path)?
        } else {
            OpenOptions::new()
                .append(true)
                .open(files.last().unwrap().path())?
        };

        let key_index = HashMap::new();

        Ok(Self {
            path: log_dir,
            key_index,
            index_scanned: wal_logs.len() + 1,
            commands: vec![],
            mutable_file,
            wal_logs,
        })
    }

    /// set a key-value pair into the store.
    ///
    /// if the key already exists, the value will be updated.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let offset = self.mutable_file.seek(SeekFrom::Current(0))?;

        let cmd = Cmd::Set(key.clone(), value);
        let serialized_cmd = serde_json::to_string(&cmd)?;

        self.mutable_file
            .write_all(&serialized_cmd.len().to_le_bytes())?;
        self.mutable_file.write_all(serialized_cmd.as_bytes())?;
        self.mutable_file.flush()?;

        let file_index = self.wal_logs.len();
        self.key_index
            .insert(key, LogIndex::new(file_index, offset));

        Ok(())
    }

    /// get the value from the store for a given key.
    ///
    /// return `None` if the key does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        unimplemented!();
    }

    /// remove the key from the store.
    pub fn remove(&mut self, key: String) -> Result<()> {
        let offset = self.mutable_file.seek(SeekFrom::Current(0))?;

        let cmd = Cmd::Rm(key.clone());
        let serialized_cmd = serde_json::to_string(&cmd)?;

        if self.key_index.contains_key(&key) {
            self.mutable_file
                .write_all(&(serialized_cmd.len() as u32).to_le_bytes())?;
            self.mutable_file.write_all(serialized_cmd.as_bytes())?;
            self.mutable_file.flush()?;

            let file_index = self.wal_logs.len();
            self.key_index
                .entry(key)
                .and_modify(|val| *val = LogIndex::new(file_index, offset));
        } else {
            while self.index_scanned != 0 {
                self.import_next_log_index()?;
                //todo: check index again
            }
        }

        Ok(())
    }

    fn import_next_log_index(&mut self) -> Result<()> {
        if self.index_scanned == 0 {
            return Ok(());
        }
        self.index_scanned -= 1;
        let mut file = if self.index_scanned == self.wal_logs.len() {
            let path = self.path.join("wal_active".to_string());
            File::open(path)?
        } else {
            self.wal_logs[self.index_scanned].try_clone()?
        };
        let len = file.metadata()?.len() as usize;
        let mut cur = 0;
        while cur < len {
            let mut size_array = [0; USIZE_LEN];
            let mut buffer = vec![0; BUFFER_SIZE];

            file.read_exact(&mut size_array[..])?;
            let cmd_size = usize::from_le_bytes(size_array);
            if buffer.len() < cmd_size {
                buffer.resize(cmd_size, 0);
            }
            file.read_exact(&mut buffer[..]);
            let cmd = serde_json::from_slice::<Cmd>(&buffer)?;
            match cmd {
                Cmd::Set(key, _) => {
                    // todo: whether to insert
                }
                Cmd::Rm(key) => {
                    // todo: whether to insert
                }
            };

            cur += USIZE_LEN + cmd_size;
        }
        Ok(())
    }
}
