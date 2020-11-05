use log::*;
use serde_json::{json, Value, to_vec, to_string, from_value, from_slice};
use warp::{hyper, Filter, path, http::{Response, StatusCode, header::SET_COOKIE}, reply::{Reply, with::header}, query, fs::{dir, File}, http::header::HeaderValue};
use streaming_platform::{MagicBall, futures::{Future, stream::{Stream, StreamExt}}, tokio::sync::{mpsc::UnboundedSender, oneshot}, RestreamMsg, StreamCompletion};
use streaming_platform::sp_dto::{get_msg_meta, get_payload, MsgKind, uuid::Uuid, get_msg, get_msg_meta_and_payload, reply_to_rpc_dto, Participator, RpcResult};
use crate::{response, response_with_cookie, check_auth_token_vec};

pub async fn go(aca_origin: Option<String>, auth_key: String, body: warp::hyper::body::Bytes, mut mb: MagicBall) -> Result<Response<Vec<u8>>, warp::Rejection> {    
    let res = match get_msg_meta(&body) {
        Ok(msg_meta) => {
            info!("hub auth token {:?}", msg_meta.auth_token);
            match check_auth_token_vec(auth_key.as_bytes(), &msg_meta) {
                Ok(_) => {
                    match msg_meta.kind {
                        MsgKind::RpcRequest => {                                        
                            match mb.proxy_rpc(mb.addr.clone(), body.to_vec()).await {
                                Ok((_, res_data)) => res_data,
                                Err(err) => {
                                    error!("{:?}", err);
                                    to_vec("here comes the error").expect("failed to serialize proxy rpc error response error")
                                }
                            }                                                                        
                        }
                        MsgKind::RpcResponse(_) => to_vec("not yet supported").expect("failed to serialize not yet supported response"),
                        MsgKind::Event => {                                        
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