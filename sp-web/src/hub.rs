use log::*;
use serde_json::to_vec;
use warp::http::Response;
use streaming_platform::MagicBall;
use streaming_platform::sp_dto::{get_msg_meta, MsgType};
use crate::{response, check_auth_token_vec};

pub async fn go(aca_origin: Option<String>, auth_token_key: String, body: warp::hyper::body::Bytes, mut mb: MagicBall) -> Result<Response<Vec<u8>>, warp::Rejection> {    
    let res = match get_msg_meta(&body) {
        Ok(msg_meta) => {
            info!("hub auth token {:?}", msg_meta.auth_token);
            match check_auth_token_vec(auth_token_key.as_bytes(), &msg_meta) {
                Ok(_) => {
                    match msg_meta.msg_type {
                        MsgType::RpcRequest => {                                        
                            match mb.proxy_rpc(mb.addr.clone(), body.to_vec()).await {
                                Ok((_, res_data)) => res_data,
                                Err(err) => {
                                    error!("{:?}", err);
                                    to_vec("here comes the error").expect("failed to serialize proxy rpc error response error")
                                }
                            }                                                                        
                        }
                        MsgType::RpcResponse(_) => to_vec("not yet supported").expect("failed to serialize not yet supported response"),
                        MsgType::Event => {                                        
                            match mb.proxy_event(mb.addr.clone(), body.to_vec()).await {
                                Ok(_) => to_vec("ok friends").expect("failed to serialize proxy event ok response"),
                                Err(err) => {
                                    error!("{:?}", err);
                                    to_vec("here comes the error").expect("failed to serialize proxy rpc error response error")
                                }
                            }
                        }
                    }
                }
                Err(e) => e
            }            
        }
        Err(err) => {
            error!("{:?}", err);
            to_vec("here comes the error").expect("failed to serialize error response")
        }
    };

    let res = match aca_origin {
        Some(aca_origin) => response(aca_origin, res).expect("failed to build aca origin response"),
        None => Response::builder().body(res).expect("failed to build response")
    };


    Ok::<Response<Vec<u8>>, warp::Rejection>(res)
}