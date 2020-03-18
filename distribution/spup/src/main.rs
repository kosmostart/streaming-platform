use std::thread;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::io::BufReader;
use std::io::prelude::*;
use serde_derive::Deserialize;
use serde_json::json;
use streaming_platform::{magic_ball, ClientMsg};
use sp_pack_core::unpack;

#[derive(Debug, Deserialize)]
struct Config {
    host: String,
    addr: String,
    access_key: String,
    path: String
}

fn main() {
    let config_path = std::env::args().nth(1)
        .expect("path to config file not passed as argument");
    let file = std::fs::File::open(config_path)
        .expect("failed to open config");

    let mut buf_reader = BufReader::new(file);
    let mut config = String::new();

    buf_reader.read_to_string(&mut config)
        .expect("failed to read config");
    let config: Config = toml::from_str(&config)
        .expect("failed to deserialize config");    

    magic_ball(&config.host, &config.addr, &config.access_key, &config.path, process_msg);
}

fn process_msg(msg: ClientMsg, save_path: String) {
    match msg {
        ClientMsg::FileReceiveComplete(name) => unpack(save_path, name),
        _ => {}
    }
}
