use log::*;
use serde_json::to_vec;
use warp::http::Response;
use streaming_platform::MagicBall;
use streaming_platform::sp_dto::{get_msg_meta, MsgType};
use crate::{check_auth_token, response};

pub async fn go(aca_origin: Option<String>, auth_token_key: String, cookie_header: Option<String>, body: warp::hyper::body::Bytes, mut mb: MagicBall) -> Result<Response<Vec<u8>>, warp::Rejection> {
    let res = match check_auth_token(auth_token_key.as_bytes(), cookie_header) {
        Some(auth_data) => {
            match get_msg_meta(&body) {                
                Ok(msg_meta) => {
					info!("{} hub: {:?}, {:?}, {:?}", mb.addr, msg_meta.msg_type, msg_meta.key, msg_meta.route.source);

                    match msg_meta.msg_type {
                        MsgType::RpcRequest => {
                            match mb.proxy_rpc_with_auth_data(mb.addr.clone(), auth_data, body.to_vec()).await {
                                Ok((_, res_data)) => Some(res_data),
                                Err(err) => {
                                    error!("{:?}", err);
                                    None                                 
                                }
                            }                   
                        }     
                        MsgType::RpcResponse(_) => {
                            warn!("Not implemented");
                            None
                        }
                        MsgType::Event => {
                            match mb.proxy_event_with_auth_data(mb.addr.clone(), auth_data, body.to_vec()).await {
                                Ok(_) => Some(to_vec("Ok friends").expect("Failed to serialize proxy event ok response")),
                                Err(err) => {
                                    error!("{:?}", err);
                                    None
                                }
                            }
                        }
                        MsgType::ServerRpcRequest => {
                            warn!("Not implemented");
                            None
                        }
                        MsgType::ServerRpcResponse(_) => {
                            warn!("Not implemented");
                            None
                        }
                    }
				}    
                Err(err) => {
                    error!("{:?}", err);
                    None
                }
            }
		}
        None => {
			warn!("Unauthorized hub access attempt");

			None
		}
    };

    let res = res.unwrap_or(to_vec("Here comes the error").expect("Failed to serialize proxy rpc error response error"));

    Ok::<Response<Vec<u8>>, warp::Rejection>(match aca_origin {
        Some(aca_origin) => response(aca_origin, res).expect("Failed to build aca origin response"),
        None => Response::builder().body(res).expect("Failed to build response")
    })
}