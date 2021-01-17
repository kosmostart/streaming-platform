use log::*;
use serde_json::{Value, to_vec};
use warp::http::Response;
use streaming_platform::MagicBall;
use streaming_platform::sp_dto::{get_msg_meta, MsgType};
use crate::{check_auth_token, response};

pub async fn go(aca_origin: Option<String>, auth_token_key: String, cookie_header: Option<String>, body: warp::hyper::body::Bytes, mut mb: MagicBall) -> Result<Response<Vec<u8>>, warp::Rejection> {
    let res = match check_auth_token(auth_token_key.as_bytes(), cookie_header) {
        Some(auth_data) =>
            match get_msg_meta(&body) {                
                Ok(msg_meta) =>
                    match msg_meta.msg_type {
                        MsgType::RpcRequest =>
                            match mb.proxy_rpc(mb.addr.clone(), body.to_vec()).await {
                                Ok((_, res_data)) => Some(res_data),
                                Err(err) => {
                                    error!("{:?}", err);
                                    None                                 
                                }
                            }                        
                        MsgType::RpcResponse(_) => {
                            warn!("Not implemented");
                            None                                 
                        }
                        MsgType::Event => 
                            match mb.proxy_event(mb.addr.clone(), body.to_vec()).await {
                                Ok(_) => Some(to_vec("Ok friends").expect("Failed to serialize proxy event ok response")),
                                Err(err) => {
                                    error!("{:?}", err);
                                    None
                                }
                            }                        
                    }                
                Err(err) => {
                    error!("{:?}", err);
                    None
                }
            }
        None => None
    };

    let res = res.unwrap_or(to_vec("Here comes the error").expect("Failed to serialize proxy rpc error response error"));

    Ok::<Response<Vec<u8>>, warp::Rejection>(match aca_origin {
        Some(aca_origin) => response(aca_origin, res).expect("Failed to build aca origin response"),
        None => Response::builder().body(res).expect("Failed to build response")
    })
}