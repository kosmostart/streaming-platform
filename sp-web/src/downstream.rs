use log::*;
use serde_json::{Value, to_vec};
use warp::http::Response;
use streaming_platform::MagicBall;
use streaming_platform::sp_dto::{get_msg_meta, MsgType};
use crate::{check_auth_token, response};

pub async fn go(aca_origin: Option<String>, auth_token_key: String, cookie_header: Option<String>, body: warp::hyper::body::Bytes, mut mb: MagicBall) -> Result<Response<Vec<u8>>, warp::Rejection> {
    let res = match check_auth_token(auth_token_key.as_bytes(), cookie_header) {
        Some(auth_data) => Some(vec![]),
        None => None
    };

    let res = res.unwrap_or(to_vec("Here comes the error").expect("Failed to serialize proxy rpc error response error"));

    Ok::<Response<Vec<u8>>, warp::Rejection>(match aca_origin {
        Some(aca_origin) => response(aca_origin, res).expect("Failed to build aca origin response"),
        None => Response::builder().body(res).expect("Failed to build response")
    })
}