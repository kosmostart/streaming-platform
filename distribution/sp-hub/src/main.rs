use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use streaming_platform::{ServerConfig, start};

pub fn get_config_from_arg() -> ServerConfig {
    let config_path = std::env::args().nth(1)
        .expect("path to config file not passed as argument");

    let file = File::open(config_path)
        .expect("failed to open config");

    let mut buf_reader = BufReader::new(file);

    let mut config = String::new();

    buf_reader.read_to_string(&mut config)
        .expect("failed to read config");

    toml::from_str(&config)
        .expect("failed to deserialize config")
}

fn main() {
    env_logger::init();
    
    let config = get_config_from_arg();

    start(config);
}
