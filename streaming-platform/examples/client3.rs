use std::collections::HashMap;
use serde_json::{json, Value, from_value};
use streaming_platform::{client, MagicBall, sp_dto::{MsgMeta, Key, Message, Response, resp}};

pub async fn process_event(config: HashMap<String, String>, mut mb: MagicBall, msg: Message<Value>, _: ()) -> Result<(), Box<dyn std::error::Error>>  {
    Ok(())
}

pub async fn process_rpc(config: HashMap<String, String>, mut mb: MagicBall, msg: Message<Value>, _: ()) -> Result<Response<Value>, Box<dyn std::error::Error>> {    
    resp(json!({}))
}

pub async fn startup(config: HashMap<String, String>, mut mb: MagicBall, startup_data: Option<Value>, _: ()) {
    mb.send_event(Key::simple("HiEvent"), json!({
        "data": "hello event"
    })).await;

    let msg = mb.rpc::<_, Value>(Key::simple("HiRpc"), json!({
        "data": "hello rpc"
    })).await;

    println!("{:#?}", msg);
}

pub fn main() {
    env_logger::init();

    let mut config = HashMap::new();

    config.insert("addr".to_owned(), "Client3".to_owned());
    config.insert("host".to_owned(), "127.0.0.1:11001".to_owned());
    config.insert("access_key".to_owned(), "".to_owned());
 
    client::start(config, process_event, process_rpc, startup, None, ());
 }