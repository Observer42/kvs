use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chashmap::CHashMap;
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
    epoch: usize,
    offset: u64,
    len: u64,
}

impl LogIndex {
    fn new(epoch: usize, offset: u64, len: u64) -> Self {
        Self { epoch, offset, len }
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
    reader: KvStoreReader,
    writer: Arc<Mutex<KvStoreWriter>>,
}

impl KvStore {
    /// load the kv store from disk
    pub fn open<T: AsRef<Path>>(dir: T) -> Result<Self> {
        let mut log_dir = PathBuf::new();
        log_dir.push(dir);
        create_dir_all(&log_dir)?;

        try_add_engine_type(&log_dir, EngineType::KvStore)?;

        let log_file = log_dir
            .read_dir()?
            .into_iter()
            .filter_map(|entry_result| entry_result.ok())
            .filter(|entry| entry.metadata().unwrap().is_file())
            .filter_map(|entry| {
                let file_name = entry.file_name().to_str().unwrap().to_owned();
                if file_name.ends_with(".log") {
                    let prefix = file_name.trim_end_matches(".log");
                    let epoch = usize::from_str(prefix).ok()?;
                    Some((entry, epoch))
                } else {
                    None
                }
            })
            .max_by_key(|(_, epoch)| *epoch);

        let (mut reader, writer, epoch) = match log_file {
            Some((entry, epoch)) => {
                let path = entry.path();
                let writer = OpenOptions::new().append(true).open(&path)?;
                let reader = File::open(path)?;
                (BufReader::new(reader), BufWriter::new(writer), epoch)
            }
            None => {
                let path = log_dir.join("0.log".to_string());
                let writer = File::create(&path)?;
                let reader = File::open(path)?;
                (BufReader::new(reader), BufWriter::new(writer), 0)
            }
        };

        let latest = Arc::new(AtomicUsize::from(epoch));

        let key_index = Self::import_log(&mut reader, epoch)?;
        let path = Arc::new(log_dir);

        let mut buf_readers = [None, None];
        buf_readers[epoch % 2] = Some(reader);

        let reader = KvStoreReader {
            path: path.clone(),
            epoch: latest.clone(),
            key_index: key_index.clone(),
            reader_epoch: AtomicUsize::from(epoch),
            readers: RefCell::new(buf_readers),
        };

        let writer = KvStoreWriter {
            path: path.clone(),
            epoch: latest.clone(),
            key_index,
            redundant: 0,
            reader: reader.clone(),
            writer: writer,
        };
        //import_log()?;

        Ok(Self {
            reader,
            writer: Arc::new(Mutex::new(writer)),
        })
    }

    fn import_log(reader: &mut BufReader<File>, epoch: usize) -> Result<Arc<CHashMap<String, LogIndex>>> {
        reader.seek(SeekFrom::Start(0))?;
        let mut cur_pos = 0;
        let mut stream = serde_json::Deserializer::from_reader(reader).into_iter::<Cmd>();
        let key_index = CHashMap::new();

        while let Some(cmd) = stream.next() {
            let key = match cmd? {
                Cmd::Set(key, _) => key.clone(),
                Cmd::Rm(key) => key.clone(),
            };
            let new_pos = stream.byte_offset() as u64;
            key_index.insert(key, LogIndex::new(epoch, cur_pos, new_pos - cur_pos));
            cur_pos = new_pos;
        }
        Ok(Arc::new(key_index))
    }
}

impl KvsEngine for KvStore {
    fn get(&self, key: String) -> Result<Option<String>> {
        self.reader.get(key)
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)
    }

    fn remove(&self, key: String) -> Result<()> {
        self.writer.lock().unwrap().remove(key)
    }
}

struct KvStoreReader {
    path: Arc<PathBuf>,
    epoch: Arc<AtomicUsize>,
    key_index: Arc<CHashMap<String, LogIndex>>,
    readers: RefCell<[Option<BufReader<File>>; 2]>,
    reader_epoch: AtomicUsize,
}

impl Clone for KvStoreReader {
    fn clone(&self) -> Self {
        let reader = Self {
            path: self.path.clone(),
            epoch: self.epoch.clone(),
            key_index: self.key_index.clone(),
            readers: RefCell::new([None, None]),
            reader_epoch: AtomicUsize::new(0),
        };

        let _ = reader.update_reader(true);

        reader
    }
}

impl KvStoreReader {
    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(log_index) = self.key_index.get(&key) {
            self.update_reader(false)?;
            let cmd = self.read_from_log(*log_index)?;
            match cmd {
                Cmd::Set(_, val) => Ok(Some(val)),
                Cmd::Rm(_) => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    fn read_from_log(&self, log_index: LogIndex) -> Result<Cmd> {
        let mut readers = self.readers.borrow_mut();
        let reader = readers[log_index.epoch % 2].as_mut().unwrap();
        reader.seek(SeekFrom::Start(log_index.offset))?;
        let take = reader.take(log_index.len);
        serde_json::from_reader(take).map_err(|e| e.into())
    }

    fn update_reader(&self, initial: bool) -> Result<()> {
        let latest = self.epoch.load(Ordering::SeqCst);
        let self_epoch = self.reader_epoch.load(Ordering::SeqCst);
        if self_epoch < latest || initial {
            let cur_reader = BufReader::new(File::open(self.path.join(format!("{}.log", latest)))?);
            self.readers.borrow_mut()[latest % 2] = Some(cur_reader);
            if self_epoch + 1 < latest {
                let prev_file = File::open(self.path.join(format!("{}.log", latest - 1))).ok();
                self.readers.borrow_mut()[(latest - 1) % 2] = prev_file.map(|file| BufReader::new(file));
            }
            self.reader_epoch.store(latest, Ordering::SeqCst);
        }
        Ok(())
    }
}

struct KvStoreWriter {
    path: Arc<PathBuf>,
    epoch: Arc<AtomicUsize>,
    key_index: Arc<CHashMap<String, LogIndex>>,
    writer: BufWriter<File>,
    redundant: u32,
    reader: KvStoreReader,
}

impl KvStoreWriter {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.append_log(Cmd::Set(key.clone(), value), key)
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

        let epoch = self.epoch.load(Ordering::Acquire);
        let log_index = LogIndex::new(epoch, offset, new_offset - offset);

        //trigger compaction if necessary: too much redundant records or active_file too large
        if let Some(_) = self.key_index.insert(key, log_index) {
            self.redundant += 1;
            if self.redundant > COMPACTION_THRESHOLD {
                self.compact()?
            }
        }
        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        let temp_path = self.path.join("temp");
        let mut new_writer = BufWriter::new(File::create(&temp_path)?);
        let cur_key_index = (*self.key_index).clone();
        let mut new_key_index = HashMap::new();
        let new_epoch = self.epoch.load(Ordering::SeqCst) + 1;

        let mut offset = 0;
        for (key, log_index) in cur_key_index.into_iter() {
            let cmd = self.reader.read_from_log(log_index)?;
            serde_json::to_writer(&mut new_writer, &cmd)?;
            new_key_index.insert(key, LogIndex::new(new_epoch, offset, log_index.len));
            offset += log_index.len;
        }
        new_writer.flush()?;
        drop(new_writer);

        let old_path = self.path.join(format!("{}.log", new_epoch - 2));
        let _ = std::fs::remove_file(old_path);

        let new_path = self.path.join(format!("{}.log", new_epoch));
        std::fs::rename(temp_path, &new_path)?;

        self.epoch.fetch_add(1, Ordering::SeqCst);
        for (key, index) in new_key_index {
            self.key_index.insert(key, index);
        }
        self.redundant = 0;

        self.writer = BufWriter::new(File::create(&new_path)?);

        Ok(())
    }
}
