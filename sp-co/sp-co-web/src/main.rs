use serde_json::json;
use sp_web::streaming_platform::client;

pub fn main() {
    env_logger::init();

    /*
    config.insert("addr".to_owned(), "Web".to_owned());
    config.insert("stream_addr".to_owned(), "WebStream".to_owned());
    config.insert("host".to_owned(), "127.0.0.1:11001".to_owned());
    config.insert("listen_addr".to_owned(), "127.0.0.1:12345".to_owned());
    //config.insert("cert_path".to_owned(), "".to_owned());
    //config.insert("key_path".to_owned(), "".to_owned());
    config.insert("access_key".to_owned(), "".to_owned());
    config.insert("auth_token_key".to_owned(), "This is key omg".to_owned());
    //config.insert("deploy_path".to_owned(), "".to_owned());
    */

    let config = json!({
        "cfg_host": "127.0.0.1:11002",
		"cfg_domain": "Cfg",
        "cfg_token": "Web"
    });
 
    client::start_full_message(config, sp_web::process_event, sp_web::process_rpc, sp_web::startup, None, None, (), ());
 }
 