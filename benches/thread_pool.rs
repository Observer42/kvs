use std::net::SocketAddr;
use std::thread;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use tempfile::TempDir;

use kvs::thread_pool::{SharedQueueThreadPool, ThreadPool};
use kvs::{KvStore, KvsClient, KvsEngine, KvsServer, SledKvsEngine};

fn read_queued_kvstore(c: &mut Criterion) {
    let threads = [1, 2, 4, 6, 8];
    let mut group = c.benchmark_group("read_queued_kvstore");
    for size in threads.iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let temp_dir = TempDir::new().unwrap();
            let kv_store = KvStore::open(temp_dir).unwrap();
            for i in 1000..2000 {
                kv_store.set(i.to_string(), "value".to_string()).unwrap();
            }
            let addr: SocketAddr = "127.0.0.1:4000".parse().unwrap();
            let thread_pool = SharedQueueThreadPool::new(*size).unwrap();

            let server = KvsServer::init(kv_store, addr, thread_pool).unwrap();
            server.start();
            thread::sleep(Duration::from_secs(1));

            let client_thread_pool = SharedQueueThreadPool::new(1000).unwrap();
            b.iter(|| {
                for num in 1000..2000 {
                    client_thread_pool.spawn(move || {
                        let mut client = KvsClient::init(&addr).unwrap();
                        let key = num.to_string();
                        let res = client.get(key.clone());
                        assert!(res.is_ok());
                        assert_eq!(res.unwrap(), Some("value".to_string()));
                    });
                }
            });
            server.stop_server();
        });
    }
}

fn write_queued_kvstore(c: &mut Criterion) {
    let threads = [1, 2, 4, 6, 8];
    let mut group = c.benchmark_group("write_queued_kvstore");
    for size in threads.iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let temp_dir = TempDir::new().unwrap();
            let kv_store = KvStore::open(temp_dir).unwrap();
            let addr: SocketAddr = "127.0.0.1:4000".parse().unwrap();
            let thread_pool = SharedQueueThreadPool::new(*size).unwrap();

            let server = KvsServer::init(kv_store, addr, thread_pool).unwrap();
            server.start();
            thread::sleep(Duration::from_secs(1));

            let client_thread_pool = SharedQueueThreadPool::new(*size).unwrap();
            b.iter(|| {
                for num in 1000..2000 {
                    client_thread_pool.spawn(move || {
                        let mut client = KvsClient::init(&addr).unwrap();
                        let key = num.to_string();
                        assert!(client.set(key.clone(), "value".to_string()).is_ok());
                    });
                }
            });
            server.stop_server();
        });
    }
}

criterion_group!(benches, write_queued_kvstore, read_queued_kvstore);
criterion_main!(benches);
