use std::thread;

use super::ThreadPool;
use crate::Result;

/// Naive thread pool
///
/// This is not even a thread pool. It just spawn one thread for each closure
pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(_threads: u32) -> Result<Self> {
        Ok(NaiveThreadPool)
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}
