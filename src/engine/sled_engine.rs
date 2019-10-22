use std::path::{Path, PathBuf};

use sled::Db;

use crate::engine::{try_add_engine_type, EngineType};
use crate::{KvsEngine, KvsError, Result};

/// Sled implementation of `KvsEngine`
pub struct SledKvsEngine {
    db: Db,
}

impl SledKvsEngine {
    /// load the sled db from disk
    pub fn open<T: AsRef<Path>>(dir: T) -> Result<Self> {
        let mut log_dir = PathBuf::new();
        log_dir.push(dir);
        std::fs::create_dir_all(&log_dir)?;

        try_add_engine_type(&log_dir, EngineType::Sled)?;

        let db = Db::open(log_dir)?;
        Ok(Self { db })
    }
}

impl KvsEngine for SledKvsEngine {
    fn get(&mut self, key: String) -> Result<Option<String>> {
        self.db
            .get(key)
            .map(|option| option.map(|vec| unsafe { String::from_utf8_unchecked(vec.to_vec()) }))
            .map_err(|e| e.into())
    }
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.as_bytes()).map(|_| ())?;
        self.db.flush()?;
        Ok(())
    }
    fn remove(&mut self, key: String) -> Result<()> {
        let res = match self.db.remove(key) {
            Ok(Some(_)) => Ok(()),
            Ok(None) => Err(KvsError::KeyNotFound),
            Err(e) => Err(e.into()),
        };
        self.db.flush()?;
        res
    }
}
