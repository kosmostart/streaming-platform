use std::collections::HashMap;
use serde_json::{json, Value, from_value};
use core_api::{log::*, futures::FutureExt, tokio, core::{Dc, Config}, tokio_postgres, Client, NoTls, Error};
use core_api::{streaming_platform::MagicBall, MsgMeta, Message, Response, resp};

pub async fn process_event(config: HashMap<String, String>, mut mb: MagicBall, msg: Message<Value>) -> Result<(), Box<dyn std::error::Error>>  {
    Ok(())
}

pub async fn process_rpc(config: HashMap<String, String>, mut mb: MagicBall, msg: Message<Value>) -> Result<Response<Value>, Box<dyn std::error::Error>> {    
    resp(json!({}))
}

pub async fn startup(config: HashMap<String, String>, mut mb: MagicBall) {
	
}