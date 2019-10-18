use std::collections::BTreeMap;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::prelude::*;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{KvsError, Result};

const COMPACTION_THRESHOLD: u32 = 10_000;
const FILE_SIZE_LIMIT: usize = 1_048_576;

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
    key_index: BTreeMap<String, LogIndex>,
    index_scanned: usize,
    mutable_file: File,
    wal_logs: Vec<File>,
    active_redundant: u32,
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
        create_dir_all(&log_dir)?;

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

        let key_index = BTreeMap::new();

        Ok(Self {
            path: log_dir,
            key_index,
            index_scanned: wal_logs.len() + 1,
            mutable_file,
            wal_logs,
            active_redundant: 0,
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

        //trigger compaction if necessary: too much redundant records or active_file too large
        if let Some(old_index) = self.key_index.insert(key, log_index) {
            if old_index.file_index == self.wal_logs.len() {
                self.active_redundant += 1;
                if self.active_redundant > COMPACTION_THRESHOLD {
                    self.minor_compact()?
                }
            }
        }
        if offset as usize > 10 * FILE_SIZE_LIMIT {
            self.minor_compact()?
        }
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

        let mut local_index = BTreeMap::new();

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

    fn minor_compact(&mut self) -> Result<()> {
        let active_path = self.path.join("wal_active".to_string());
        let mut new_key_index = self.key_index.clone();

        let new_active_path = self.path.join("new_active");
        let mut new_active_file = File::create(&new_active_path)?;

        let prev_active_index = self.wal_logs.len();
        let mut cur_index = self.wal_logs.len();
        let mut offset = 0;
        for (_, log_index) in new_key_index.iter_mut() {
            if log_index.file_index == prev_active_index {
                let cmd = self.read_from_log(*log_index)?;
                let serialized_cmd = serde_json::to_vec(&cmd)?;

                if offset + serialized_cmd.len() + 4 >= FILE_SIZE_LIMIT {
                    // save file as wal_##.log and create new active_file
                    new_active_file.flush()?;
                    drop(new_active_file);
                    let next_log_path = self.path.join(format!("wal_{:05}.log", cur_index));
                    std::fs::rename(&new_active_path, &next_log_path)?;
                    self.wal_logs.push(File::open(next_log_path)?);

                    new_active_file = File::open(&new_active_path)?;
                    cur_index += 1;
                    offset = 0;
                }

                new_active_file.write_all(&(serialized_cmd.len() as u32).to_le_bytes())?;
                new_active_file.write_all(&serialized_cmd)?;

                log_index.file_index = cur_index;
                log_index.offset = offset as u64;
                offset += serialized_cmd.len() + 4;
            }
        }
        new_active_file.flush()?;
        drop(new_active_file);
        std::fs::rename(new_active_path, &active_path)?;

        self.active_redundant = 0;
        self.key_index = new_key_index;
        self.mutable_file = OpenOptions::new().append(true).open(active_path)?;

        Ok(())
    }
}
