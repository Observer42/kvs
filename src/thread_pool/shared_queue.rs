use std::thread;

use crossbeam::{Receiver, Sender};

use super::ThreadPool;
use crate::Result;

/// Shared queue thread pool
///
/// The thread pool create dispatch tasks by crossbeam channel
pub struct SharedQueueThreadPool {
    sender: Sender<Box<dyn FnOnce() + Send + 'static>>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let (tx, rx) = crossbeam::unbounded();
        for _ in 0..threads {
            let receiver = ReceiverWrapper(rx.clone());
            thread::spawn(move || {
                while let Ok(f) = receiver.0.recv() {
                    f();
                }
            });
        }
        Ok(Self { sender: tx })
    }

    /// Send a closure to thread pool
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender.send(Box::new(job)).unwrap();
    }
}

struct ReceiverWrapper(Receiver<Box<dyn FnOnce() + Send + 'static>>);

impl Drop for ReceiverWrapper {
    fn drop(&mut self) {
        if thread::panicking() {
            let receiver = ReceiverWrapper(self.0.clone());
            thread::spawn(move || {
                while let Ok(f) = receiver.0.recv() {
                    f();
                }
            });
        }
    }
}
