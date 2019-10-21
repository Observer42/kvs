mod client;
mod server;

pub use client::KvsClient;
pub use server::KvsServer;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
enum Query {
    Get(String),
    Set(String, String),
    Rm(String),
}

#[derive(Serialize, Deserialize)]
enum Response {
    Success,
    KeyNotFound,
    Ok(Option<String>),
    Err,
}
