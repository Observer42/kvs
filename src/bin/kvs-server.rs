use std::env::current_dir;
use std::net::SocketAddr;

use log::info;
use structopt::StructOpt;

use kvs::thread_pool::{NaiveThreadPool, ThreadPool};
use kvs::{EngineType, KvStore, KvsEngine, KvsServer, SledKvsEngine};

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs-server", about = env!("CARGO_PKG_DESCRIPTION"))]
struct Opt {
    #[structopt(long, parse(try_from_str), default_value = "127.0.0.1:4000")]
    addr: SocketAddr,
    #[structopt(long, parse(try_from_str), default_value = "kvs")]
    engine: EngineType,
}

fn main() -> kvs::Result<()> {
    env_logger::builder().filter_level(log::LevelFilter::Info).init();
    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));

    let opt: Opt = Opt::from_args();
    info!("server addr: {}, engine: {}", opt.addr, opt.engine);

    let dir = current_dir()?;

    let thread_pool = NaiveThreadPool::new(0)?;
    match opt.engine {
        EngineType::KvStore => start_server(KvStore::open(dir)?, opt.addr, thread_pool),
        EngineType::Sled => start_server(SledKvsEngine::open(dir)?, opt.addr, thread_pool),
    }
}

fn start_server<T: KvsEngine, U: ThreadPool>(engine: T, addr: SocketAddr, thread_pool: U) -> kvs::Result<()> {
    let server = KvsServer::init(engine, &addr, thread_pool)?;
    server.serve()
}
