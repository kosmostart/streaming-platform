use std::env;
use log::*;
use time::OffsetDateTime;
use serde_json::{
	json, Value
};
use sp_storage::{
	Location, Dc, Sc, create_service
};
use streaming_platform::{
	client, MagicBall, sp_dto::{
		Message, Response, resp
	}, 
	tokio::sync::mpsc::UnboundedReceiver, Frame
};
use crate::error::Error;

mod error;

pub async fn process_event(_config: Value, _mb: MagicBall, _msg: Message<Value>, _: Sc, _emittable_rx: UnboundedReceiver<Frame>) -> Result<(), Box<dyn std::error::Error>>  {
    //info!("{:#?}", msg);
    
    Ok(())
}

pub async fn process_rpc(_config: Value, _mb: MagicBall, msg: Message<Value>, sc: Sc, _emittable_rx: UnboundedReceiver<Frame>) -> Result<Response<Value>, Box<dyn std::error::Error>> {
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

            let active = sc["Cfg"].filter(|a| a["domain"] == msg.payload["domain"] && a["key"] == msg.payload["key"] && a["deactivated_at"].is_null())?;

            for (id, mut payload) in active {
                payload["deactivated_at"] = json!(OffsetDateTime::now_utc());
                let _ = sc["Cfg"].update(id, payload)?;
            }

            let _ = sc["Cfg"].create(json!({
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

            let data = sc["Cfg"].filter(|a| a["domain"] == msg.payload["domain"])?;

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

            match sc["Cfg"].find(|a| a["domain"] == msg.payload["domain"] && a["key"] == key && a["deactivated_at"].is_null())? {
                Some((_, mut payload)) => payload["payload"].take(),
                None => json!({})
            }
        }
        _ => return Err(Box::new(Error::IncorrectKeyInRequest))
    };	

    resp(res)
}

pub async fn startup(_initial_config: Value, _target_config: Value, _mb: MagicBall, _startup_data: Option<Value>, _: Sc) {
}

pub fn main() {
    env_logger::init();

    let region_id = 1;
    let scope_id = 1;
    let user_id = 1;

    let storage_path = env::var("SP_CO_CFG_STORAGE_PATH").expect("Failed to get sp co cfg storage path from SP_CO_CFG_STORAGE_PATH env variable");    

	let name = "Cfg";
	let service = "Cfg";
	let domain = "Cfg";

	let service_id = {
        let service_dc = Dc::new(Location::Services { region_id, scope_id }, user_id, &storage_path).expect("Failed to create service dc");

        match service_dc.find(|a| a["name"].as_str() == Some(name)).expect("Find error") {
			Some((service_id, _)) => service_id,
			None => {
				let service_id = create_service(&service_dc, name, service, domain).expect("Failed to create service");
				let sc = Sc::new(user_id, region_id, scope_id, service_id, &storage_path, None, name, service, domain).expect("Failed to create sc");

				sc.token_dc.create(json!({
					"name": "Cfg"
				})).expect("Failed to create token");

				service_id
			}
		}
    };

    let sc = Sc::new(user_id, region_id, scope_id, service_id, &storage_path, None, name, service, domain).expect("Failed to create sc");

	if sc["Cfg"].find(|a| a["domain"].as_str() == Some("Cfg") && a["key"].as_str() == Some("Auth")).unwrap().is_none() {
		let _ = sc["Cfg"].create(json!({
			"domain": "Cfg",
			"key": "Auth",			
			"payload": {
				"host": "127.0.0.1:11002",
				"addr": "Auth",
				"access_key": ""
			}
		}));
	}
	
	if sc["Cfg"].find(|a| a["domain"].as_str() == Some("Cfg") && a["key"].as_str() == Some("Auth")).unwrap().is_none() {
		let _ = sc["Cfg"].create(json!({
			"domain": "Cfg",
			"key": "Auth",			
			"payload": {
				"host": "127.0.0.1:11002",
				"addr": "Auth",
				"access_key": ""
			}
		}));
	}

	if sc["Cfg"].find(|a| a["domain"].as_str() == Some("Cfg") && a["key"].as_str() == Some("Web")).unwrap().is_none() {
		let _ = sc["Cfg"].create(json!({
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

	if sc["Cfg"].find(|a| a["domain"].as_str() == Some("Cfg") && a["key"].as_str() == Some("WebStream")).unwrap().is_none() {
		let _ = sc["Cfg"].create(json!({
			"domain": "Cfg",
			"key": "WebStream",			
			"payload": {
				"host": "127.0.0.1:11002",
				"addr": "WebStream",
				"access_key": ""
			}
		}));
	}

	//info!("{:#?}", sc["Cfg"].get_all());

    let config = json!({
        "host": "127.0.0.1:11002",
        "addr": "Cfg",
        "access_key": ""
    });
 
    client::start_full_message(config, process_event, process_rpc, startup, None, sc, None);	
 }