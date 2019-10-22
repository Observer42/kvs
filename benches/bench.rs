use std::collections::HashMap;

use criterion::{criterion_group, criterion_main, Criterion};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use tempfile::TempDir;

use kvs::{KvStore, KvsEngine, SledKvsEngine};

fn bench_write(c: &mut Criterion) {
    let mut rng = thread_rng();
    let mut map = HashMap::new();
    for _ in 0..100 {
        let key_len = rng.gen_range(1, 100_001);
        let key = rng.sample_iter(&Alphanumeric).take(key_len).collect::<String>();
        let val_len = rng.gen_range(1, 100_001);
        let val = rng.sample_iter(&Alphanumeric).take(val_len).collect::<String>();
        map.insert(key, val);
    }

    let map_clone = map.clone();
    c.bench_function("sled write", move |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            let mut store = SledKvsEngine::open(temp_dir.path()).unwrap();
            map_clone.iter().for_each(|(k, v)| {
                store.set(k.clone(), v.clone()).unwrap();
            });
        })
    });
    c.bench_function("kvs write", move |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            let mut store = KvStore::open(temp_dir.path()).unwrap();
            map.iter().for_each(|(k, v)| {
                store.set(k.clone(), v.clone()).unwrap();
            });
        })
    });
}

fn bench_read(c: &mut Criterion) {
    let mut rng = thread_rng();
    let mut map = HashMap::new();
    for _ in 0..100 {
        let key_len = rng.gen_range(1, 100_001);
        let key = rng.sample_iter(&Alphanumeric).take(key_len).collect::<String>();
        let val_len = rng.gen_range(1, 100_001);
        let val = rng.sample_iter(&Alphanumeric).take(val_len).collect::<String>();
        map.insert(key, val);
    }

    let map_clone = map.clone();
    c.bench_function("sled read", move |b| {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = SledKvsEngine::open(temp_dir.path()).unwrap();
        map_clone.iter().for_each(|(k, v)| {
            store.set(k.clone(), v.clone()).unwrap();
        });
        b.iter(|| {
            for _ in 0..10 {
                map_clone.keys().for_each(|k| {
                    store.get(k.clone()).unwrap();
                });
            }
        });
    });

    c.bench_function("kvs read", move |b| {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = KvStore::open(temp_dir.path()).unwrap();
        map.iter().for_each(|(k, v)| {
            store.set(k.clone(), v.clone()).unwrap();
        });
        b.iter(|| {
            for _ in 0..10 {
                map.keys().for_each(|k| {
                    store.get(k.clone()).unwrap();
                });
            }
        });
    });
}

criterion_group!(benches, bench_read, bench_write);
criterion_main!(benches);
