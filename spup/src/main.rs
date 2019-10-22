use std::fs::File;
use std::path::{Path, PathBuf};
use std::io::BufReader;
use std::io::prelude::*;
use serde_derive::Deserialize;
use sp_tokio::client::connect;
use sp_pack_core::unpack;

#[derive(Debug, Deserialize)]
struct Config {
    host: String,
    addr: String,
    access_key: String
}

fn main() {
    let config_path = std::env::args().nth(1)
        .expect("path to config file not passed as argument");

    let file = File::open(config_path)
        .expect("failed to open config");

    let mut buf_reader = BufReader::new(file);

    let mut config = String::new();

    buf_reader.read_to_string(&mut config)
        .expect("failed to read config");

    let config: Config = toml::from_str(&config)
        .expect("failed to deserialize config");

    connect(&config.host, &config.addr, &config.access_key);
    /*
    let path = std::env::args().nth(1)
        .expect("path to file not passed as argument");
unpack(&path);
    
    */
}
