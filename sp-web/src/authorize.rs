use log::*;
use serde_json::{json, Value, to_vec};
use warp::http::{Response, header::SET_COOKIE};
use streaming_platform::MagicBall;
use streaming_platform::sp_dto::{MsgKind, get_msg_meta_and_payload, reply_to_rpc_dto, RpcResult};
use crate::{response, response_with_cookie};

pub async fn go(aca_origin: Option<String>, body: warp::hyper::body::Bytes, mut mb: MagicBall) -> Result<Response<Vec<u8>>, warp::Rejection> {
    let res = match get_msg_meta_and_payload::<Value>(&body) {
        Ok((msg_meta, payload)) => {
            //info!("{:?}", msg_meta);
            match msg_meta.key.as_ref() {
                "Auth" => {
                    match msg_meta.kind {
                        MsgKind::RpcRequest => {                                        
                            match mb.rpc::<_, Value>("Auth", "Auth", payload).await {
                                Ok(msg) => {                                                        
                                    match msg.payload["auth_token"].as_str() {
                                        Some(auth_token) => {
                                            let res = reply_to_rpc_dto(
                                                msg_meta.rx.clone(),
                                                msg_meta.tx.clone(),
                                                msg_meta.key.clone(),
                                                msg_meta.correlation_id,
                                                json!({
                                                    "auth_token": auth_token,
                                                    "domain": msg.payload["domain"]
                                                }),
                                                RpcResult::Ok,
                                                msg_meta.route.clone(),
                                                None,
                                                None
                                            )
                                            .expect("failed to create positive auth response dto");

                                            let cookie_header = "skytfs-token=".to_owned() + auth_token + "; HttpOnly; path=/";

                                            match aca_origin {
                                                Some(aca_origin) => response_with_cookie(aca_origin, &cookie_header, res).expect("failed to build aca origin response"),
                                                None => Response::builder()
                                                            .header(SET_COOKIE, cookie_header)
                                                            .body(res)
                                                            .expect("failed to build response")
                                            }
                                        }
                                        None => {
                                            let res = reply_to_rpc_dto(
                                                msg_meta.rx.clone(),
                                                msg_meta.tx.clone(),
                                                msg_meta.key.clone(),
                                                msg_meta.correlation_id,
                                                json!({}),
                                                RpcResult::Ok,
                                                msg_meta.route.clone(),
                                                None,
                                                None
                                            )
                                            .expect("failed to create negative auth response dto");

                                            match aca_origin {
                                                Some(aca_origin) => response(aca_origin, res).expect("failed to build aca origin response"),
                                                None => Response::builder().body(res).expect("failed to build response")
                                            }
                                        }                                                            
                                    }                                                        
                                }
                                Err(err) => {
                                    error!("{:?}", err);
                                    let res = to_vec("here comes the error").expect("failed to serialize proxy rpc error response error");
                                    match aca_origin {
                                        Some(aca_origin) => response(aca_origin, res).expect("failed to build aca origin response"),
                                        None => Response::builder().body(res).expect("failed to build response")
                                    }
                                }
                            }                                                                        
                        }
                        _ => {
                            warn!("wrong authorize msg kind");
                            let res = to_vec("here comes the error").expect("failed to serialize proxy rpc error response error");
                            match aca_origin {
                                Some(aca_origin) => response(aca_origin, res).expect("failed to build aca origin response"),
                                None => Response::builder().body(res).expect("failed to build response")
                            }
                        }
                    }
                }
                _ => {
                    warn!("wrong authorize msg key");
                    let res = to_vec("here comes the error").expect("failed to serialize proxy rpc error response error");
                    match aca_origin {
                        Some(aca_origin) => response(aca_origin, res).expect("failed to build aca origin response"),
                        None => Response::builder().body(res).expect("failed to build response")
                    }
                }
            }                                

        }
        Err(err) => {
            error!("{:?}", err);
            let res = to_vec("here comes the error").expect("failed to serialize error response");
            match aca_origin {
                Some(aca_origin) => response(aca_origin, res).expect("failed to build aca origin response"),
                None => Response::builder().body(res).expect("failed to build response")
            }
        }
    };                                                

    Ok::<Response<Vec<u8>>, warp::Rejection>(res)
}