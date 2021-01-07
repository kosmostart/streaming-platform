use std::collections::HashMap;
use serde_json::{json, Value, from_value};
use streaming_platform::{client, MagicBall, sp_dto::{MsgMeta, Message, Response, resp}};

pub async fn process_event(config: HashMap<String, String>, mut mb: MagicBall, msg: Message<Value>) -> Result<(), Box<dyn std::error::Error>>  {
    println!("{:#?}", msg);
    
    Ok(())
}

pub async fn process_rpc(config: HashMap<String, String>, mut mb: MagicBall, msg: Message<Value>) -> Result<Response<Value>, Box<dyn std::error::Error>> {    
    println!("{:#?}", msg);

    resp(json!({
        "data": "hi"
    }))
}

pub async fn startup(config: HashMap<String, String>, mut mb: MagicBall, startup_data: Option<Value>) {
}

pub fn main() {
    env_logger::init();

    let mut config = HashMap::new();

    config.insert("addr".to_owned(), "Client2".to_owned());
    config.insert("host".to_owned(), "localhost:11001".to_owned());
    config.insert("access_key".to_owned(), "".to_owned());
 
    client::start(config, process_event, process_rpc, startup, None);
 }