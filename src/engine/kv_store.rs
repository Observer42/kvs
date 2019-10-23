use std::collections::BTreeMap;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::engine::{try_add_engine_type, EngineType};
use crate::{KvsEngine, KvsError, Result};

const COMPACTION_THRESHOLD: u32 = 10_000;
const FILE_SIZE_LIMIT: usize = 1_048_576;

#[derive(Serialize, Deserialize)]
pub(crate) enum Cmd {
    Set(String, String),
    Rm(String),
}

#[derive(Copy, Clone)]
struct LogIndex {
    file_index: usize,
    offset: u64,
    len: u64,
}

impl LogIndex {
    fn new(file_index: usize, offset: u64, len: u64) -> Self {
        Self {
            file_index,
            offset,
            len,
        }
    }
}

/// A simple key-value store implementation
///
/// Examples:
/// ```rust
/// use kvs::{KvStore, KvsEngine};
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
#[derive(Clone)]
pub struct KvStore {
    inner: Arc<Mutex<KvStoreInner>>,
}

impl KvStore {
    /// load the kv store from disk
    pub fn open<T: AsRef<Path>>(dir: T) -> Result<Self> {
        let inner = Arc::new(Mutex::new(KvStoreInner::open(dir)?));
        Ok(Self { inner })
    }
}

impl KvsEngine for KvStore {
    fn get(&self, key: String) -> Result<Option<String>> {
        let mut inner = self.inner.lock().unwrap();
        inner.get(key)
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.set(key, value)
    }

    fn remove(&self, key: String) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.remove(key)
    }
}

struct KvStoreInner {
    path: PathBuf,
    key_index: BTreeMap<String, LogIndex>,
    index_scanned: usize,
    active_reader: BufReader<File>,
    active_writer: BufWriter<File>,
    wal_logs: Vec<BufReader<File>>,
    active_redundant: u32,
}

impl KvStoreInner {
    fn open<T: AsRef<Path>>(dir: T) -> Result<Self> {
        let mut log_dir = PathBuf::new();
        log_dir.push(dir);
        create_dir_all(&log_dir)?;

        try_add_engine_type(&log_dir, EngineType::KvStore)?;

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
                wal_logs.push(BufReader::new(file));
            }
        }

        let (active_reader, active_writer) = if files.is_empty()
            || !files
                .last()
                .unwrap()
                .file_name()
                .to_str()
                .unwrap()
                .contains("wal_active")
        {
            let path = log_dir.join("wal_active".to_string());
            let writer = File::create(&path)?;
            let reader = File::open(path)?;
            (BufReader::new(reader), BufWriter::new(writer))
        } else {
            let path = files.last().unwrap().path();
            let writer = OpenOptions::new().append(true).open(&path)?;
            let reader = File::open(path)?;
            (BufReader::new(reader), BufWriter::new(writer))
        };

        let key_index = BTreeMap::new();

        Ok(Self {
            path: log_dir,
            key_index,
            index_scanned: wal_logs.len() + 1,
            active_reader,
            active_writer,
            wal_logs,
            active_redundant: 0,
        })
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.append_log(Cmd::Set(key.clone(), value), key)
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
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

    fn remove(&mut self, key: String) -> Result<()> {
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
        let offset = self.active_writer.seek(SeekFrom::End(0))?;
        serde_json::to_writer(&mut self.active_writer, &cmd)?;
        self.active_writer.flush()?;
        let new_offset = self.active_writer.seek(SeekFrom::End(0))?;

        let file_index = self.wal_logs.len();
        let log_index = LogIndex::new(file_index, offset, new_offset - offset);

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
        let reader = if log_index.file_index == self.wal_logs.len() {
            &mut self.active_reader
        } else {
            &mut self.wal_logs[self.index_scanned]
        };
        reader.seek(SeekFrom::Start(log_index.offset))?;

        let take = reader.take(log_index.len);
        serde_json::from_reader(take).map_err(|e| e.into())
    }

    fn import_next_log(&mut self) -> Result<()> {
        if self.index_scanned == 0 {
            return Ok(());
        }
        self.index_scanned -= 1;
        let reader = if self.index_scanned == self.wal_logs.len() {
            &mut self.active_reader
        } else {
            &mut self.wal_logs[self.index_scanned]
        };

        reader.seek(SeekFrom::Start(0))?;
        let mut cur_pos = 0;
        let mut local_index = BTreeMap::new();
        let mut stream = serde_json::Deserializer::from_reader(reader).into_iter::<Cmd>();

        while let Some(cmd) = stream.next() {
            let key = match cmd? {
                Cmd::Set(key, _) => key.clone(),
                Cmd::Rm(key) => key.clone(),
            };
            let new_pos = stream.byte_offset() as u64;
            local_index.insert(key, LogIndex::new(self.index_scanned, cur_pos, new_pos - cur_pos));
            cur_pos = new_pos;
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
        let mut new_active_writer = BufWriter::new(File::create(&new_active_path)?);

        let prev_active_index = self.wal_logs.len();
        let mut cur_index = self.wal_logs.len();
        let mut offset = 0;
        for (_, log_index) in new_key_index.iter_mut() {
            if log_index.file_index == prev_active_index {
                if offset + log_index.len as usize >= FILE_SIZE_LIMIT {
                    // save file as wal_##.log and create new active_file
                    drop(new_active_writer);
                    let next_log_path = self.path.join(format!("wal_{:05}.log", cur_index));
                    std::fs::rename(&new_active_path, &next_log_path)?;
                    self.wal_logs.push(BufReader::new(File::open(next_log_path)?));

                    new_active_writer = BufWriter::new(File::create(&new_active_path)?);
                    cur_index += 1;
                    offset = 0;
                }

                let cmd = self.read_from_log(*log_index)?;
                serde_json::to_writer(&mut new_active_writer, &cmd)?;

                log_index.file_index = cur_index;
                log_index.offset = offset as u64;
                offset += log_index.len as usize;
            }
        }
        new_active_writer.flush()?;
        drop(new_active_writer);
        std::fs::rename(new_active_path, &active_path)?;

        self.active_redundant = 0;
        self.key_index = new_key_index;
        self.active_reader = BufReader::new(File::open(&active_path)?);
        self.active_writer = BufWriter::new(OpenOptions::new().append(true).open(active_path)?);

        Ok(())
    }
}
