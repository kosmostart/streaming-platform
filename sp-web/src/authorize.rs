use log::*;
use serde_json::{json, Value, to_vec};
use warp::http::{Response, header::SET_COOKIE};
use streaming_platform::MagicBall;
use streaming_platform::sp_dto::{
    MsgType, get_msg_meta_and_payload, rpc_response_dto, RpcResult
};
use crate::{response, response_with_cookie};

enum AuthResult {
    Ok(String, Vec<u8>),
    Fail(Vec<u8>),
    Error
}

pub async fn go(aca_origin: Option<String>, body: warp::hyper::body::Bytes, mut mb: MagicBall) -> Result<Response<Vec<u8>>, warp::Rejection> {
	info!("Received authorize request");

    let res = match get_msg_meta_and_payload::<Value>(&body) {
        Ok((msg_meta, payload)) =>

            match msg_meta.key.action.as_ref() {                
                "Auth" => {

                    match msg_meta.msg_type {
                        MsgType::RpcRequest => 

                            match mb.rpc::<_, Value>(msg_meta.key.clone(), payload).await {
                                Ok(msg) =>

                                    match msg.payload["auth_token"].as_str() {
                                        Some(auth_token) =>
                                            AuthResult::Ok(
                                                "skytfs-token=".to_owned() + auth_token + "; HttpOnly; path=/",
                                                rpc_response_dto(
                                                    mb.addr,
                                                    msg_meta.key,
                                                    msg_meta.correlation_id,
                                                    json!({
                                                        "result": true,
                                                        "service": msg.payload["service"]														
                                                    }),
                                                    RpcResult::Ok,
                                                    msg_meta.route,
                                                    None,
                                                    None
                                                ).expect("Failed to create positive auth response dto")
                                            ),
                                        None =>
                                            AuthResult::Fail(rpc_response_dto(
                                                mb.addr,
                                                msg_meta.key,
                                                msg_meta.correlation_id,
                                                json!({}),
                                                RpcResult::Ok,
                                                msg_meta.route,
                                                None,
                                                None
                                            ).expect("Failed to create negative auth response dto"))
                                    }
                                Err(err) => {
                                    error!("{:?}", err);
                                    AuthResult::Error
                                }
                            }                        
                        _ => {
                            warn!("Wrong authorize msg msg_type");
                            AuthResult::Error
                        }
                    }
                }
                _ => {
                    warn!("Wrong authorize msg key");
                    AuthResult::Error
                }
            }        
        Err(err) => {
            error!("{:?}", err);
            AuthResult::Error
        }
    };

    Ok::<Response<Vec<u8>>, warp::Rejection>(match res {        
        AuthResult::Ok(cookie_header, res) => {
            match aca_origin {
                Some(aca_origin) => response_with_cookie(aca_origin, &cookie_header, res).expect("failed to build aca origin response"),
                None => Response::builder()
                            .header(SET_COOKIE, cookie_header)
                            .body(res)
                            .expect("failed to build response")
            }
        }
        AuthResult::Fail(res) => 
            match aca_origin {
                Some(aca_origin) => response(aca_origin, res).expect("Failed to build aca origin response"),
                None => Response::builder().body(res).expect("Failed to build response")
            },
        AuthResult::Error => {
            let res = to_vec("Here comes the error").expect("Failed to serialize proxy rpc error response error");

            match aca_origin {
                Some(aca_origin) => response(aca_origin, res).expect("Failed to build aca origin response"),
                None => Response::builder().body(res).expect("Failed to build response")
            }
        }
    })
}
