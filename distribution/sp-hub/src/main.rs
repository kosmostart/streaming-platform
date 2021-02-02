use std::collections::HashMap;
use streaming_platform::{sp_cfg, server::start, sp_dto::Subscribes};

fn main() {
    env_logger::init();
    
    let config = sp_cfg::get_config_from_arg();

    let event_subscribes = HashMap::new();
    let rpc_subscribes = HashMap::new();
    let rpc_response_subscribes = HashMap::new();

    start(config, Subscribes::ByKey(event_subscribes, rpc_subscribes, rpc_response_subscribes));
}