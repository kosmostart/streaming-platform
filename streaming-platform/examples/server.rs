use std::{fmt::Debug, collections::HashMap};
use std::io::BufReader;
use std::io::prelude::*;
use streaming_platform::ServerConfig;
pub use streaming_platform;

pub fn get_config_from_file() -> ServerConfig {
    let config_path = std::env::args().nth(1)
    .expect("path to config file not passed as argument");

    let file = std::fs::File::open(config_path)
        .expect("failed to open config");

    let mut buf_reader = BufReader::new(file);
    let mut config = String::new();

    buf_reader.read_to_string(&mut config)
        .expect("failed to read config");

     toml::from_str(&config)
        .expect("failed to deserialize config")
}

pub fn get_config_from_arg() -> ServerConfig {
    let config = std::env::args().nth(1)
    .expect("config not passed as argument");    

     toml::from_str(&config)
        .expect("failed to deserialize config")
}

pub fn get_config_from_str() -> ServerConfig {    
     toml::from_str(r#"host = "localhost:10001""#)
        .expect("failed to deserialize config")
}

pub fn main() {    
    let config = get_config_from_str();

    streaming_platform::start(config);
}