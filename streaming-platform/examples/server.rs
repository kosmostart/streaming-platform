use streaming_platform::{sp_dto::{Key, Subscribe}, server::{self, ServerConfig}};

pub fn main() {
    env_logger::init();

    let config = ServerConfig {
        host: "127.0.0.1:11001".to_owned()
    };

    let event_subscribes = vec![
        Subscribe::new("Client1", "HiEvent", "", ""),
        Subscribe::new("Client2", "HiEvent", "", "")
    ];
    
    let rpc_subscribes = vec![
        Subscribe::new("Client2", "HiRpc", "", "")
    ];

    server::start(config, event_subscribes, rpc_subscribes);
}
