use super::ThreadPool;
use crate::{KvsError, Result};

/// Rayon thread pool wrapper
pub struct RayonThreadPool {
    pool: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads as usize)
            .build()
            .map_err(|_e| KvsError::ThreadPoolError)?;
        Ok(Self { pool })
    }

    /// Send a closure to thread pool
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.pool.spawn(job);
    }
}
