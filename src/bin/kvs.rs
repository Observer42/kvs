use std::process::exit;

use structopt::StructOpt;

use kvs::KvStore;

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
    let mut store = KvStore::open(std::env::current_dir().unwrap()).unwrap_or_else(|_| exit(1));
    match opt.command {
        Command::Set { key, val } => {
            if let Err(err) = store.set(key, val) {
                println!("{}", err);
                exit(1);
            }
        }
        Command::Get { key } => match store.get(key) {
            Ok(Some(val)) => {
                println!("{}", val);
            }
            _ => {
                println!("Key not found");
                exit(1);
            }
        },
        Command::Remove { key } => match store.remove(key) {
            Ok(_) => (),
            _ => {
                println!("Key not found");
                exit(1);
            }
        },
    }
}
