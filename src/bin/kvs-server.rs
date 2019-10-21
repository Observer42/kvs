use std::net::SocketAddr;
use std::str::FromStr;

use log::info;
use structopt::StructOpt;

use kvs::{KvStore, KvsEngine, KvsServer};
use std::env::current_dir;
use std::fmt::{Display, Formatter};

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs-server", about = env!("CARGO_PKG_DESCRIPTION"))]
struct Opt {
    #[structopt(long, parse(try_from_str), default_value = "127.0.0.1:4000")]
    addr: SocketAddr,
    #[structopt(long, parse(try_from_str), default_value = "kvs")]
    engine: Engine,
}

#[derive(Debug, Clone, Copy)]
enum Engine {
    Kvs,
    Sled,
}

impl FromStr for Engine {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "kvs" => Ok(Engine::Kvs),
            "sled" => Ok(Engine::Sled),
            s => Err(format!("wrong engine: {}", s)),
        }
    }
}

impl Display for Engine {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Engine::Kvs => write!(f, "kvs"),
            Engine::Sled => write!(f, "sled"),
        }
    }
}

fn main() -> kvs::Result<()> {
    env_logger::builder().filter_level(log::LevelFilter::Info).init();
    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));

    let opt: Opt = Opt::from_args();
    info!("server addr: {}, engine: {}", opt.addr, opt.engine);

    let dir = current_dir()?;

    match opt.engine {
        Engine::Kvs => start_server(KvStore::open(dir)?, opt.addr),
        // todo: change to sled
        Engine::Sled => start_server(KvStore::open(dir)?, opt.addr),
    }
}

fn start_server<T: KvsEngine>(engine: T, addr: SocketAddr) -> kvs::Result<()> {
    let mut server = KvsServer::init(engine, &addr)?;
    server.serve()
}
