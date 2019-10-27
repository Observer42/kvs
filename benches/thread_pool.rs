use std::collections::HashMap;

use criterion::{criterion_group, criterion_main, Criterion};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use tempfile::TempDir;

use kvs::{KvStore, KvsEngine, SledKvsEngine};

fn kvs_read(_c: &mut Criterion) {}

criterion_group!(benches, kvs_read);
criterion_main!(benches);
