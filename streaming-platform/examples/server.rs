use std::{fmt::Debug, collections::HashMap};
use std::io::BufReader;
use std::io::prelude::*;
use log::*;
use streaming_platform::server;
use streaming_platform::sp_cfg::ServerConfig;

pub fn main() {
    env_logger::init();

    let config = ServerConfig {
        host: "127.0.0.1:11001".to_owned()
    };
    
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

    server::start(config, event_subscribes, rpc_subscribes, rpc_response_subscribes);
}