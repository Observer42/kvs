//! Thread pool module
//!
//! This module contains ?? `ThreadPool` implementation

mod naive;
mod rayon;
mod shared_queue;

pub use self::rayon::RayonThreadPool;
pub use naive::NaiveThreadPool;
pub use shared_queue::SharedQueueThreadPool;

use crate::Result;

/// Interface for thread pool implementation
pub trait ThreadPool {
    /// Creates a thread pool
    ///
    /// return error if failed to create any thread
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;

    /// Send a closure to thread pool
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}
