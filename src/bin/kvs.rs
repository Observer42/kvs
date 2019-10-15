use kvs::KvStore;
use std::process::exit;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
enum Command {
    #[structopt(name = "set")]
    Set { key: String, val: String },
    #[structopt(name = "get")]
    Get { key: String },
    #[structopt(name = "rm")]
    Remove { key: String },
}

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs", about = env!("CARGO_PKG_DESCRIPTION"))]
struct Opt {
    #[structopt(subcommand)]
    command: Command,
}

fn main() {
    let opt: Opt = Opt::from_args();
    match opt.command {
        Command::Set { key, val } => {
            let mut kv = KvStore::new();
            match kv.set(key, val) {
                Err(err) => {
                    println!("{}", err);
                    exit(1);
                }
                Ok(_) => (),
            }
        }
        Command::Get { key } => {
            let kv = KvStore::new();
            match kv.get(key) {
                Ok(Some(val)) => {
                    println!("{}", val);
                }
                _ => {
                    println!("Key not found");
                    exit(1);
                }
            }
        }
        Command::Remove { key } => {
            let mut kv = KvStore::new();
            match kv.remove(key) {
                Ok(_) => (),
                _ => {
                    println!("Key not found");
                    exit(1);
                }
            }
        }
    }
}
