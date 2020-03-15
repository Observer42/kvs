use std::collections::BTreeMap;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::engine::{try_add_engine_type, EngineType};
use crate::{KvsEngine, KvsError, Result};

const COMPACTION_THRESHOLD: u32 = 10_000;

#[derive(Serialize, Deserialize)]
pub(crate) enum Cmd {
    Set(String, String),
    Rm(String),
}

#[derive(Copy, Clone)]
struct LogIndex {
    offset: u64,
    len: u64,
}

impl LogIndex {
    fn new(offset: u64, len: u64) -> Self {
        Self { offset, len }
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
    epoch: u64,
    key_index: BTreeMap<String, LogIndex>,
    reader: BufReader<File>,
    writer: BufWriter<File>,
    redundant: u32,
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

        let mut log_files = files
            .iter()
            .filter_map(|entry| {
                let file_name = entry.file_name().to_str().unwrap().to_owned();
                if file_name.ends_with(".log") {
                    let prefix = file_name.trim_end_matches(".log");
                    let epoch = u64::from_str(prefix).ok()?;
                    Some((entry, epoch))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        log_files.sort_by_key(|&(_, epoch)| epoch);

        let (reader, writer, epoch) = if let Some((entry, epoch)) = log_files.last() {
            let path = entry.path();
            let writer = OpenOptions::new().append(true).open(&path)?;
            let reader = File::open(path)?;
            (BufReader::new(reader), BufWriter::new(writer), *epoch)
        } else {
            let path = log_dir.join("0.log".to_string());
            let writer = File::create(&path)?;
            let reader = File::open(path)?;
            (BufReader::new(reader), BufWriter::new(writer), 0)
        };

        let key_index = BTreeMap::new();

        let mut store = Self {
            path: log_dir,
            epoch,
            key_index,
            reader,
            writer,
            redundant: 0,
        };

        store.import_log()?;
        Ok(store)
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
            Ok(None)
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if self.key_index.contains_key(&key) {
            self.append_log(Cmd::Rm(key.clone()), key)
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    fn append_log(&mut self, cmd: Cmd, key: String) -> Result<()> {
        let offset = self.writer.seek(SeekFrom::End(0))?;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        let new_offset = self.writer.seek(SeekFrom::End(0))?;

        let log_index = LogIndex::new(offset, new_offset - offset);

        //trigger compaction if necessary: too much redundant records or active_file too large
        if let Some(_) = self.key_index.insert(key, log_index) {
            self.redundant += 1;
            if self.redundant > COMPACTION_THRESHOLD {
                self.compact()?
            }
        }
        Ok(())
    }

    fn read_from_log(&mut self, log_index: LogIndex) -> Result<Cmd> {
        let reader = &mut self.reader;
        reader.seek(SeekFrom::Start(log_index.offset))?;

        let take = reader.take(log_index.len);
        serde_json::from_reader(take).map_err(|e| e.into())
    }

    fn import_log(&mut self) -> Result<()> {
        self.reader.seek(SeekFrom::Start(0))?;
        let mut cur_pos = 0;
        let mut local_index = BTreeMap::new();
        let mut stream = serde_json::Deserializer::from_reader(&mut self.reader).into_iter::<Cmd>();

        while let Some(cmd) = stream.next() {
            let key = match cmd? {
                Cmd::Set(key, _) => key.clone(),
                Cmd::Rm(key) => key.clone(),
            };
            let new_pos = stream.byte_offset() as u64;
            local_index.insert(key, LogIndex::new(cur_pos, new_pos - cur_pos));
            cur_pos = new_pos;
        }

        local_index.into_iter().for_each(|(key, index)| {
            self.key_index.entry(key).or_insert(index);
        });

        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        let temp_path = self.path.join("temp");
        let mut new_writer = BufWriter::new(File::create(&temp_path)?);
        let mut new_key_index = self.key_index.clone();

        let mut offset = 0;
        for (_, log_index) in new_key_index.iter_mut() {
            let cmd = self.read_from_log(*log_index)?;
            serde_json::to_writer(&mut new_writer, &cmd)?;
            log_index.offset = offset as u64;
            offset += log_index.len as usize;
        }
        new_writer.flush()?;
        drop(new_writer);

        let old_path = self.path.join(format!("{}.log", self.epoch));
        let _ = std::fs::remove_file(old_path);
        self.epoch += 1;
        let new_path = self.path.join(format!("{}.log", self.epoch));
        std::fs::rename(temp_path, &new_path)?;

        self.redundant = 0;
        self.key_index = new_key_index;
        self.reader = BufReader::new(File::open(&new_path)?);
        self.writer = BufWriter::new(OpenOptions::new().append(true).open(new_path)?);

        Ok(())
    }
}
