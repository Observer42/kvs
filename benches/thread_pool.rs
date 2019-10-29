use std::net::{SocketAddr, IpAddr, Ipv4Addr};

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use tempfile::TempDir;

use kvs::{KvStore, KvsEngine, SledKvsEngine, KvsServer, KvsClient};
use kvs::thread_pool::SharedQueueThreadPool;
use kvs::thread_pool::ThreadPool;
use std::thread;
use failure::_core::time::Duration;

fn kvs_read(c: &mut Criterion) {
    let threads = vec![1, 2, 4, 8];
    let mut group = c.benchmark_group("thread_pool_kvs_read");
    for size in threads {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let temp_dir = TempDir::new().unwrap();
            let kv_store = KvStore::open(temp_dir).unwrap();
            for i in 0..1000 {
                kv_store.set(i.to_string(), i.to_string()).unwrap();
            }

            let addr: SocketAddr = "127.0.0.1:4000".parse().unwrap();

            thread::spawn(move || {
                let thread_pool = SharedQueueThreadPool::new(size).unwrap();
                let server = KvsServer::init(kv_store, addr, thread_pool).unwrap();
                server.serve().unwrap();
            });
            b.iter(|| {
                for i in 0..10000 {
                    thread::spawn(move|| {
                        let mut client = KvsClient::init(&addr).unwrap();
                        let key = (i % 1000).to_string();
                        match client.get(key.clone()) {
                            Ok(Some(v)) => {
                                if v != key {
                                    panic!("wrong result");
                                }
                            }
                            Ok(None) => {
                                if i <= 1000 {
                                    panic!("wrong result");
                                }
                            }
                            Err(_e) => (),
                        };
                    });
                }
            });
            thread::sleep(Duration::from_secs(10));
            server.stop();
        });
    }
}


criterion_group!(benches, kvs_read);
criterion_main!(benches);
