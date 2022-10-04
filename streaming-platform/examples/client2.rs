use tokio::runtime::Runtime;
use serde_json::{json, Value};
use streaming_platform::{client, MagicBall, Frame, tokio::sync::mpsc::UnboundedReceiver, sp_dto::{Message, Response, resp}};

pub async fn process_event(_config: Value, _mb: MagicBall, msg: Message<Value>, _: (), _emittable_rx: UnboundedReceiver<Frame>) -> Result<(), Box<dyn std::error::Error>> {
    println!("{:#?}", msg);
    
    Ok(())
}

pub async fn process_rpc(_config: Value, _mb: MagicBall, msg: Message<Value>, _: (), _emittable_rx: UnboundedReceiver<Frame>) -> Result<Response<Value>, Box<dyn std::error::Error + Send + Sync>> {
    println!("{:#?}", msg);

    resp(json!({
        "data": "hi"
    }))
}

pub async fn startup(_initial_config: Value, _target_config: Value, _mb: MagicBall, _startup_data: Option<Value>, _: (), _: ()) {
}

pub fn main() {
    env_logger::init();

    let config = json!({
        "host": "127.0.0.1:11001",
        "addr": "Client2",
        "access_key": ""
    });    
 
    let rt = Runtime::new().expect("Failed to create runtime");
    rt.block_on(client::full_message_mode(config, process_event, process_rpc, startup, None, None, (), ()));
}
