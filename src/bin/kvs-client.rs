use std::net::SocketAddr;
use std::process::exit;

use log::error;
use structopt::StructOpt;

use kvs::KvsClient;

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

impl Command {
    fn get_addr(&self) -> &SocketAddr {
        match self {
            Command::Set { addr, .. } => addr,
            Command::Get { addr, .. } => addr,
            Command::Remove { addr, .. } => addr,
        }
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs", about = env!("CARGO_PKG_DESCRIPTION"))]
struct Opt {
    #[structopt(subcommand)]
    command: Command,
}

fn main() {
    let opt: Opt = Opt::from_args();
    let mut client = KvsClient::init(opt.command.get_addr()).unwrap();
    match opt.command {
        Command::Set { key, val, .. } => match client.set(key, val) {
            Ok(_) => (),
            Err(err) => {
                error!("{}", err);
                exit(1);
            }
        },
        Command::Get { key, .. } => match client.get(key) {
            Ok(Some(val)) => println!("{}", val),
            Ok(None) => println!("Key not found"),
            _ => exit(1),
        },
        Command::Remove { key, .. } => match client.remove(key) {
            Ok(_) => (),
            _ => {
                eprintln!("Key not found");
                exit(1);
            }
        },
    };
}
