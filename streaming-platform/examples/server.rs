use std::{fmt::Debug, collections::HashMap};
use std::io::BufReader;
use std::io::prelude::*;
use log::*;
use streaming_platform::server;
use streaming_platform::sp_cfg::ServerConfig;

pub fn main() {
    env_logger::init();

    let config = get_config_from_str();
    
    let mut event_subscribes = HashMap::new();
    let mut rpc_subscribes = HashMap::new();
    let mut rpc_response_subscribes = HashMap::new();

    event_subscribes.insert("HiEvent".to_owned(), vec![
        "Client1".to_owned(),
        "Client2".to_owned()
    ]);

    rpc_subscribes.insert("HiRpc".to_owned(), vec![
        "Client1".to_owned()        
    ]);

    rpc_response_subscribes.insert("HiRpc".to_owned(), vec![
        "Client3".to_owned()
    ]);

    info!("{:#?}", config);

    server::start(config, event_subscribes, rpc_subscribes, rpc_response_subscribes);
}

pub fn get_config_from_str() -> ServerConfig {    
    toml::from_str(r#"
        host = "localhost:11001"        
    "#).expect("failed to deserialize config")
}

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