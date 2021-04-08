#![feature(try_trait)]
use std::collections::HashMap;
use serde_json::{json, Value, to_vec};
use streaming_platform::{client, MagicBall, sp_dto::{MsgMeta, Message, Response, resp}};
use crate::error::Error;
use storage::Dc;

mod sd;
mod storage;
mod error;

pub async fn process_event(config: HashMap<String, String>, mut mb: MagicBall, msg: Message<Value>, _: Dc) -> Result<(), Box<dyn std::error::Error>>  {
    //info!("{:#?}", msg);
    
    Ok(())
}

pub async fn process_rpc(config: HashMap<String, String>, mut mb: MagicBall, msg: Message<Value>, _: Dc) -> Result<Response<Value>, Box<dyn std::error::Error>> {   
    //info!("{:#?}", msg);

    let res = match msg.meta.key.action.as_ref() {
        "Add" => {
            json!({
            })
        }
        "Get" => {
            json!({                
            })
        }
        _ => return Err(Box::new(Error::IncorrectKeyInRequest))
    };

    resp(res)
}

pub async fn startup(config: HashMap<String, String>, mut mb: MagicBall, startup_data: Option<Value>, _: Dc) {
}

pub fn main() {
    env_logger::init();

    let mut config = HashMap::new();

    config.insert("addr".to_owned(), "Cfg".to_owned());
    config.insert("host".to_owned(), "127.0.0.1:11001".to_owned());
    config.insert("access_key".to_owned(), "".to_owned());

    let user_id = 1;
    let root_path = "d:/src/cfg-storage";

    let dc = Dc::new(user_id, root_path).expect("Failed to create dc");
 
    client::start_full_message(config, process_event, process_rpc, startup, None, dc);
 }