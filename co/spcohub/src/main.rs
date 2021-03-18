use std::collections::HashMap;
use streaming_platform::{sp_cfg, server::start, sp_dto::{Key, Subscribes}};

fn main() {
    env_logger::init();
    
    let config = sp_cfg::get_config_from_arg();

    let event_subscribes = HashMap::new();
    let mut rpc_subscribes = HashMap::new();
    let mut rpc_response_subscribes = HashMap::new();

    rpc_subscribes.insert("Build".to_owned(), vec![
        Key::new("Deploy", "", "")
    ]);

    rpc_subscribes.insert("Pod".to_owned(), vec![
        Key::new("DeployPack", "", "")
    ]);

    rpc_response_subscribes.insert("Build".to_owned(), vec![
        Key::new("DeployPack", "", "")
    ]);

    rpc_response_subscribes.insert("Web".to_owned(), vec![
        Key::new("Deploy", "", "")
    ]);

    start(config, Subscribes::ByAddr(event_subscribes, rpc_subscribes, rpc_response_subscribes));
}