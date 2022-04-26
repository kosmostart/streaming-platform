use streaming_platform::{server::{self, ServerConfig}, sp_dto::Subscribe};

fn main() {
    env_logger::init();
    
    let config = ServerConfig {
        host: "127.0.0.1:11002".to_owned()
    };

    let event_subscribes = vec![
        Subscribe::new("WebStream", "DeployStream", "Deploy", "Deploy")
    ];

    let rpc_subscribes = vec![        
        Subscribe::new("Auth", "Auth", "Auth", "Auth"),
        Subscribe::new("Build", "Deploy", "Deploy", "Deploy"),
        Subscribe::new("Pod", "DeployUnit", "Deploy", "Deploy"),
        Subscribe::new("Cfg", "AddCfg", "Cfg", "Cfg"),
        Subscribe::new("Cfg", "GetCfgDomain", "Cfg", "Cfg"),
        Subscribe::new("Cfg", "GetCfg", "Cfg", "Cfg")
    ];

    server::start(config, event_subscribes, rpc_subscribes);
}
