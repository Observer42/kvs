use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{KvsError, Result};

#[derive(Serialize, Deserialize)]
enum Cmd {
    Set(String, String),
    Rm(String),
}

#[derive(Copy, Clone)]
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
/// let mut dir = std::env::temp_dir();
/// dir.push("wal_log");
/// std::fs::create_dir(&dir);
///
/// let mut store = KvStore::open(&dir).unwrap();
/// store.set("abc".to_string(), "def".to_string());
/// assert_eq!(store.get("abc".to_string()).unwrap(), Some("def".to_string()));
/// assert_eq!(store.get("ijk".to_string()).unwrap(), None);
/// store.remove("abc".to_string());
/// assert_eq!(store.get("abc".to_string()).unwrap(), None);
///
/// std::fs::remove_dir_all(&dir);
/// ```
pub struct KvStore {
    path: PathBuf,
    key_index: HashMap<String, LogIndex>,
    index_scanned: usize,
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

        let mutable_file =
            if files.is_empty() || !files.last().unwrap().file_name().to_str().unwrap().contains("active") {
                let path = log_dir.join("wal_active".to_string());
                File::create(path)?
            } else {
                OpenOptions::new().append(true).open(files.last().unwrap().path())?
            };

        let key_index = HashMap::new();

        Ok(Self {
            path: log_dir,
            key_index,
            index_scanned: wal_logs.len() + 1,
            mutable_file,
            wal_logs,
        })
    }

    /// set a key-value pair into the store.
    ///
    /// if the key already exists, the value will be updated.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.append_log(Cmd::Set(key.clone(), value), key)
    }

    /// get the value from the store for a given key.
    ///
    /// return `Ok(None)` if the key does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(&log_index) = self.key_index.get(&key) {
            let cmd = self.read_from_log(log_index)?;
            match cmd {
                Cmd::Set(_, val) => Ok(Some(val)),
                Cmd::Rm(_) => Ok(None),
            }
        } else {
            while self.index_scanned != 0 {
                self.import_next_log()?;
                if let Some(&log_index) = self.key_index.get(&key) {
                    let cmd = self.read_from_log(log_index)?;
                    match cmd {
                        Cmd::Set(_, val) => return Ok(Some(val)),
                        Cmd::Rm(_) => return Ok(None),
                    }
                }
            }
            Ok(None)
        }
    }

    /// remove the key from the store.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.key_index.contains_key(&key) {
            self.append_log(Cmd::Rm(key.clone()), key)
        } else {
            while self.index_scanned != 0 {
                self.import_next_log()?;
                if let Some(&log_index) = self.key_index.get(&key) {
                    let cmd = self.read_from_log(log_index)?;
                    if let Cmd::Rm(_) = cmd {
                        return Err(KvsError::KeyNotFound);
                    } else {
                        return self.append_log(Cmd::Rm(key.clone()), key);
                    }
                }
            }
            Err(KvsError::KeyNotFound)
        }
    }

    fn append_log(&mut self, cmd: Cmd, key: String) -> Result<()> {
        let serialized_cmd = serde_json::to_vec(&cmd)?;
        let offset = self.mutable_file.seek(SeekFrom::End(0))?;
        self.mutable_file
            .write_all(&(serialized_cmd.len() as u32).to_le_bytes())?;
        self.mutable_file.write_all(&serialized_cmd)?;
        self.mutable_file.flush()?;

        let file_index = self.wal_logs.len();
        let log_index = LogIndex::new(file_index, offset);
        self.key_index.insert(key, log_index);
        Ok(())
    }

    fn read_from_log(&mut self, log_index: LogIndex) -> Result<Cmd> {
        let mut file = if log_index.file_index == self.wal_logs.len() {
            let path = self.path.join("wal_active".to_string());
            File::open(path)?
        } else {
            self.wal_logs[self.index_scanned].try_clone()?
        };
        file.seek(SeekFrom::Start(log_index.offset))?;

        let mut size_array = [0; 4];
        file.read_exact(&mut size_array[..])?;
        let cmd_size = u32::from_le_bytes(size_array);

        let mut buffer = vec![0; cmd_size as usize];
        file.read_exact(&mut buffer[..cmd_size as usize])?;
        serde_json::from_slice::<Cmd>(&buffer[..cmd_size as usize]).map_err(|e| e.into())
    }

    fn import_next_log(&mut self) -> Result<()> {
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

        let mut local_index = HashMap::new();

        let mut size_array = [0; 4];
        let mut buffer = vec![];
        while cur < len {
            file.read_exact(&mut size_array[..])?;
            let cmd_size = u32::from_le_bytes(size_array);
            if buffer.len() < cmd_size as usize {
                buffer.resize(cmd_size as usize, 0);
            }
            file.read_exact(&mut buffer[..cmd_size as usize])?;
            let cmd = serde_json::from_slice::<Cmd>(&buffer[..cmd_size as usize])?;
            let key = match cmd {
                Cmd::Set(key, _) => key.clone(),
                Cmd::Rm(key) => key.clone(),
            };
            local_index.insert(key, LogIndex::new(self.index_scanned, cur as u64));

            cur += 4 + cmd_size as usize;
        }

        local_index.into_iter().for_each(|(key, index)| {
            self.key_index.entry(key).or_insert(index);
        });

        Ok(())
    }
}
