use streaming_platform::{server::{self, ServerConfig}, sp_dto::{Key, new_subscribes_by_addr}};

fn main() {
    env_logger::init();
    
    let config = ServerConfig {
        host: "127.0.0.1:11002".to_owned()
    };

    let event_subscribes = new_subscribes_by_addr(vec![
        ("WebStream", vec![
            Key::new("DeployStream", "Deploy", "Deploy")
        ])        
    ]);

    let rpc_subscribes = new_subscribes_by_addr(vec![        
        ("Auth", vec![
            Key::new("Auth", "Auth", "Auth")
        ]),
        ("Build", vec![
            Key::new("Deploy", "Deploy", "Deploy")
        ]),
        ("Pod", vec![
            Key::new("DeployUnit", "Deploy", "Deploy")
        ]),
        ("Cfg", vec![
            Key::new("AddCfg", "Cfg", "Cfg"),
            Key::new("GetCfgDomain", "Cfg", "Cfg"),
            Key::new("GetCfg", "Cfg", "Cfg")
        ])        
    ]);

    server::start(config, event_subscribes, rpc_subscribes);
}
