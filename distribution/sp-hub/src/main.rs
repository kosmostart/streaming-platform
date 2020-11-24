use std::collections::HashMap;
use streaming_platform::{sp_cfg, server::start};

fn main() {
    env_logger::init();
    
    let config = sp_cfg::get_config_from_arg();

    let subscribes = HashMap::new();    

    start(config, subscribes);
}