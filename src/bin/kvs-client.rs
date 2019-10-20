use std::process::exit;

use structopt::StructOpt;

use kvs::{KvStore, KvsEngine};
use std::net::SocketAddr;

#[derive(Debug, StructOpt)]
enum Command {
    #[structopt(name = "set")]
    Set {
        key: String,
        val: String,
        #[structopt(long, parse(try_from_str), default_value = "127.0.0.1:4000")]
        addr: SocketAddr,
    },
    #[structopt(name = "get")]
    Get {
        key: String,
        #[structopt(long, parse(try_from_str), default_value = "127.0.0.1:4000")]
        addr: SocketAddr,
    },
    #[structopt(name = "rm")]
    Remove {
        key: String,
        #[structopt(long, parse(try_from_str), default_value = "127.0.0.1:4000")]
        addr: SocketAddr,
    },
}

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs", about = env!("CARGO_PKG_DESCRIPTION"))]
struct Opt {
    #[structopt(subcommand)]
    command: Command,
}

fn main() {
    let opt: Opt = Opt::from_args();
    let mut store = KvStore::open(std::env::current_dir().unwrap()).unwrap_or_else(|_| exit(1));
    match opt.command {
        Command::Set { key, val, addr } => {
            if let Err(err) = store.set(key, val) {
                println!("{}", err);
                exit(1);
            }
        }
        Command::Get { key, addr } => match store.get(key) {
            Ok(Some(val)) => println!("{}", val),
            Ok(None) => println!("Key not found"),
            _ => exit(1),
        },
        Command::Remove { key, addr } => match store.remove(key) {
            Ok(_) => (),
            _ => {
                println!("Key not found");
                exit(1);
            }
        },
    };
}
