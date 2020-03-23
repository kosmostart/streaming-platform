use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use serde_derive::{Deserialize};

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub addr: String,
    pub host: String,
    pub dirs: Option<Vec<Dir>>
}

#[derive(Debug, Deserialize, Clone)]
pub struct Dir {
    pub access_key: String,
    pub path: String
}

pub fn get_config() -> Config {
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
