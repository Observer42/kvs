use std::net::SocketAddr;
use std::thread;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use tempfile::TempDir;

use kvs::thread_pool::{RayonThreadPool, SharedQueueThreadPool, ThreadPool};
use kvs::{KvStore, KvsClient, KvsEngine, KvsServer, SledKvsEngine};

fn read_queued_kvstore(c: &mut Criterion) {
    read_general_bench::<KvStore, SharedQueueThreadPool, Box<dyn Fn() -> KvStore>>(
        c,
        Box::new(generate_kvstore),
        "read_queued_kvstore",
    );
}

fn write_queued_kvstore(c: &mut Criterion) {
    write_general_bench::<KvStore, SharedQueueThreadPool, Box<dyn Fn() -> KvStore>>(
        c,
        Box::new(generate_kvstore),
        "write_queued_kvstore",
    );
}

fn read_rayon_kvstore(c: &mut Criterion) {
    read_general_bench::<KvStore, RayonThreadPool, Box<dyn Fn() -> KvStore>>(
        c,
        Box::new(generate_kvstore),
        "read_rayon_kvstore",
    );
}

fn write_rayon_kvstore(c: &mut Criterion) {
    write_general_bench::<KvStore, RayonThreadPool, Box<dyn Fn() -> KvStore>>(
        c,
        Box::new(generate_kvstore),
        "write_rayon_kvstore",
    );
}

fn read_rayon_sledkvengine(c: &mut Criterion) {
    read_general_bench::<SledKvsEngine, RayonThreadPool, Box<dyn Fn() -> SledKvsEngine>>(
        c,
        Box::new(generate_sled_engine),
        "read_rayon_sledkvengine",
    );
}

fn write_rayon_sledkvengine(c: &mut Criterion) {
    write_general_bench::<SledKvsEngine, RayonThreadPool, Box<dyn Fn() -> SledKvsEngine>>(
        c,
        Box::new(generate_sled_engine),
        "write_rayon_sledkvengine",
    );
}

fn read_general_bench<E: KvsEngine, T: ThreadPool, F: Fn() -> E>(
    c: &mut Criterion,
    generate_kv: F,
    group_name: &'static str,
) {
    let threads = [1, 2, 4, 6, 8];
    let mut group = c.benchmark_group(group_name);
    for size in threads.iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let engine = generate_kv();
            for i in 1000..2000 {
                engine.set(i.to_string(), "value".to_string()).unwrap();
            }
            let addr: SocketAddr = "127.0.0.1:4000".parse().unwrap();
            let thread_pool: T = ThreadPool::new(*size).unwrap();

            let server = KvsServer::init(engine, addr, thread_pool).unwrap();
            server.start();

            let client_thread_pool: T = ThreadPool::new(1000).unwrap();
            b.iter(|| {
                for num in 1000..2000 {
                    client_thread_pool.spawn(move || {
                        let mut client = KvsClient::init(&addr).unwrap();
                        let key = num.to_string();
                        let res = client.get(key);
                        assert!(res.is_ok());
                        assert_eq!(res.unwrap(), Some("value".to_string()));
                    });
                }
            });
            thread::sleep(Duration::from_secs(1));
            server.stop_server();
        });
    }
}

fn write_general_bench<E: KvsEngine, T: ThreadPool, F: Fn() -> E>(
    c: &mut Criterion,
    generate_kv: F,
    group_name: &'static str,
) {
    let threads = [1, 2, 4, 6, 8];
    let mut group = c.benchmark_group(group_name);
    for size in threads.iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let engine = generate_kv();
            let addr: SocketAddr = "127.0.0.1:4000".parse().unwrap();
            let thread_pool: T = ThreadPool::new(*size).unwrap();

            let server = KvsServer::init(engine, addr, thread_pool).unwrap();
            server.start();

            let client_thread_pool: T = ThreadPool::new(*size).unwrap();
            b.iter(|| {
                for num in 1000..2000 {
                    client_thread_pool.spawn(move || {
                        let mut client = KvsClient::init(&addr).unwrap();
                        let key = num.to_string();
                        assert!(client.set(key, "value".to_string()).is_ok());
                    });
                }
            });
            thread::sleep(Duration::from_secs(1));
            server.stop_server();
        });
    }
}

fn generate_kvstore() -> KvStore {
    let temp_dir = TempDir::new().unwrap();
    KvStore::open(temp_dir).unwrap()
}

fn generate_sled_engine() -> SledKvsEngine {
    let temp_dir = TempDir::new().unwrap();
    SledKvsEngine::open(temp_dir).unwrap()
}

criterion_group!(
    benches,
    write_queued_kvstore,
    read_queued_kvstore,
    read_rayon_kvstore,
    write_rayon_kvstore,
    read_rayon_sledkvengine,
    write_rayon_sledkvengine
);
criterion_main!(benches);
