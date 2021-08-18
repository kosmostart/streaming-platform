use log::*;
use warp::http::Response;
use warp::hyper;
use streaming_platform::MagicBall;
use streaming_platform::{sp_dto::{get_msg_meta, MsgType, uuid::Uuid}, RestreamMsg};
use streaming_platform::tokio::sync::mpsc::UnboundedSender;
use crate::{check_auth_token, response, response_for_body, response_for_content, ContentAccessType};

pub async fn go(aca_origin: Option<String>, auth_token_key: String, cookie_header: Option<String>, restream_tx: UnboundedSender<RestreamMsg>) -> Result<Response<hyper::body::Body>, warp::Rejection> {
    match check_auth_token(auth_token_key.as_bytes(), cookie_header) {
        Some(auth_data) => {
            let (body_tx, body) = hyper::Body::channel();
            
            match restream_tx.send(RestreamMsg::AddRestream(Uuid::new_v4(), body_tx, None)) {
				Ok(()) => {
					info!("Sending AddRestream");

					let res = response_for_content(aca_origin, body, ContentAccessType::Raw).expect("failed to build aca origin response");
					Ok::<Response<hyper::Body>, warp::Rejection>(res)                                    
				}
				Err(_) => {
					error!("Failed to send start http restream message");

					let body = hyper::Body::from("Here comes the error");
					let res = response_for_body(aca_origin, body).expect("failed to build aca origin response");
					Ok::<Response<hyper::Body>, warp::Rejection>(res)                                    
				}
			}
        }
        None => {
			warn!("Unauthorized downstream access attempt");

			let body = hyper::Body::from("Here comes the error");
            let res = response_for_body(aca_origin, body).expect("failed to build aca origin response");
            Ok::<Response<hyper::Body>, warp::Rejection>(res)                                    
        }
    }
}