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
        Command::Set { .. } => {
            eprintln!("unimplemented");
            exit(1);
        }
        Command::Get { .. } => {
            eprintln!("unimplemented");
            exit(1);
        }
        Command::Remove { .. } => {
            eprintln!("unimplemented");
            exit(1);
        }
    }
}
