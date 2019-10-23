use std::net::SocketAddr;

use log::info;
use structopt::StructOpt;

use kvs::{EngineType, KvStore, KvsEngine, KvsServer, SledKvsEngine};
use std::env::current_dir;

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

    match opt.engine {
        EngineType::KvStore => start_server(KvStore::open(dir)?, opt.addr),
        EngineType::Sled => start_server(SledKvsEngine::open(dir)?, opt.addr),
    }
}

fn start_server<T: KvsEngine>(engine: T, addr: SocketAddr) -> kvs::Result<()> {
    let server = KvsServer::init(engine, &addr)?;
    server.serve()
}
