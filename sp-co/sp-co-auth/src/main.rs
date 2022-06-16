use base64::encode;
use serde_json::{
	json, Value, to_vec
};
use sp_auth::create_auth_token;
use streaming_platform::{
	client, MagicBall, sp_dto::{
		Message, Response, resp
	},
	tokio::sync::mpsc::UnboundedReceiver, Frame
};

#[derive(Debug)]
enum Error {
    IncorrectKeyInRequest,
    //SerdeJson(serde_json::Error)
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SuperErrorError is here!")
    }
}

impl std::error::Error for Error {}

pub async fn process_event(_config: Value, _mb: MagicBall, _msg: Message<Value>, _: (), _emittable_rx: UnboundedReceiver<Frame>) -> Result<(), Box<dyn std::error::Error>>  {
    //info!("{:#?}", msg);
    
    Ok(())
}

pub async fn process_rpc(_config: Value, _mb: MagicBall, msg: Message<Value>, _: (), _emittable_rx: UnboundedReceiver<Frame>) -> Result<Response<Value>, Box<dyn std::error::Error>> {
    //info!("{:#?}", msg);

    let res = match msg.meta.key.action.as_ref() {
        "Auth" => {
            let auth_token_key = b"This is key omg";

            let cookie_payload = json!({

            });
            
            let cookie_hash = create_auth_token(auth_token_key, &cookie_payload)?;

            let part1 = encode(&cookie_hash);
            let part2 = encode(&to_vec(&cookie_payload)?);

            json!({
                "auth_token": part1 + "." + &part2
            })
        }
        _ => return Err(Box::new(Error::IncorrectKeyInRequest))
    };

    resp(res)
}

pub async fn startup(_initial_config: Value, _target_config: Value, _mb: MagicBall, _startup_data: Option<Value>, _: (), _: ()) {
}

pub fn main() {
    env_logger::init();

    let config = json!({
        "cfg_host": "127.0.0.1:11002",
		"cfg_domain": "Cfg",
        "cfg_token": "Auth"
    });
 
    client::start_full_message(config, process_event, process_rpc, startup, None, None, (), ());
 }
 