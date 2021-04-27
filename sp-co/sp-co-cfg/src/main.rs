use std::collections::HashMap;
use log::*;
use chrono::Utc;
use serde_json::{json, Value, to_vec};
use streaming_platform::{client, MagicBall, sp_dto::{MsgMeta, Message, Response, resp}};
use crate::error::Error;
use storage::Dc;

mod sd;
mod storage;
mod error;

pub async fn process_event(config: Value, mut mb: MagicBall, msg: Message<Value>, _: Dc) -> Result<(), Box<dyn std::error::Error>>  {
    //info!("{:#?}", msg);
    
    Ok(())
}

pub async fn process_rpc(config: Value, mut mb: MagicBall, msg: Message<Value>, dc: Dc) -> Result<Response<Value>, Box<dyn std::error::Error>> {   
    //info!("{:#?}", msg);

    let res = match msg.meta.key.action.as_ref() {
        "Add" => {
            info!("Received Add, payload {:#?}", msg.payload);

			if !msg.payload["domain"].is_string() {
                return Err(Box::new(Error::custom("Empty domain in payload")));
            }

            if !msg.payload["key"].is_string() {
                return Err(Box::new(Error::custom("Empty key in payload")));
            }			

            let active = dc.filter(|a| a["domain"] == msg.payload["domain"] && a["key"] == msg.payload["key"] && a["deactivated_at"].is_null())?;

            for (id, mut payload) in active {
                payload["deactivated_at"] = json!(Utc::now().naive_utc());
                let _ = dc.update(id, payload)?;
            }

            let _ = dc.create(json!({
				"domain": msg.payload["domain"],
                "key": msg.payload["key"],
                "payload": msg.payload["payload"]
            }))?;

            json!({
            })
        }
        "GetDomain" => {
            info!("Received GetDomain, payload {:#?}", msg.payload);

			if !msg.payload["domain"].is_string() {
                return Err(Box::new(Error::custom("Empty domain in payload")));
            }

            let data = dc.filter(|a| a["domain"] == msg.payload["domain"])?;

            json!({
                "data": data
            })
        }
        "Get" => {
            info!("Received Get, payload {:#?}", msg.payload);

			if !msg.payload["domain"].is_string() {
                return Err(Box::new(Error::custom("Empty domain in payload")));
            }

            if !msg.payload["key"].is_string() && !msg.payload["cfg_token"].is_string() {
                return Err(Box::new(Error::custom("Empty key in payload")));
            }

            let key = match msg.payload["key"].as_str() {
                Some(key) => key,
                None => msg.payload["cfg_token"].as_str().ok_or(Error::None)?
            };

            info!("Search for key {}", key);

            match dc.find(|a| a["domain"] == msg.payload["domain"] && a["key"] == key && a["deactivated_at"].is_null())? {
                Some((_, mut payload)) => payload["payload"].take(),
                None => json!({})
            }
        }
        _ => return Err(Box::new(Error::IncorrectKeyInRequest))
    };

    resp(res)
}

pub async fn startup(initial_config: Value, target_config: Value, mut mb: MagicBall, startup_data: Option<Value>, _: Dc) {
}

pub fn main() {
    env_logger::init();

    let user_id = 1;
    let root_path = "c:/src/sp-co-cfg-storage";

    let dc = Dc::new(user_id, root_path).expect("Failed to create dc");

	if dc.find(|a| a["domain"].as_str() == Some("Cfg") && a["key"].as_str() == Some("Auth")).unwrap().is_none() {
		let _ = dc.create(json!({
			"domain": "Cfg",
			"key": "Auth",			
			"payload": {
				"host": "127.0.0.1:11002",
				"addr": "Auth",
				"access_key": ""
			}
		}));
	}

	if dc.find(|a| a["domain"].as_str() == Some("Cfg") && a["key"].as_str() == Some("Web")).unwrap().is_none() {
		let _ = dc.create(json!({
			"domain": "Cfg",
			"key": "Web",			
			"payload": {
				"host": "127.0.0.1:11002",
				"addr": "Web",
				"stream_cfg_token": "WebStream",
				"access_key": "",			
				"listen_addr": "127.0.0.1:12345",
				//"cert_path".to_owned(), "".to_owned());
				//"key_path".to_owned(), "".to_owned());
				"auth_token_key": "This is key omg"
			}
		}));
	}

	if dc.find(|a| a["domain"].as_str() == Some("Cfg") && a["key"].as_str() == Some("WebStream")).unwrap().is_none() {
		let _ = dc.create(json!({
			"domain": "Cfg",
			"key": "WebStream",			
			"payload": {
				"host": "127.0.0.1:11002",
				"addr": "WebStream",
				"access_key": ""
			}
		}));
	}

    let config = json!({
        "host": "127.0.0.1:11002",
        "addr": "Cfg",
        "access_key": ""
    });
 
    client::start_full_message(config, process_event, process_rpc, startup, None, dc);
 }