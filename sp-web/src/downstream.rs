use log::*;
use serde_json::{Value, json, to_vec};
use warp::http::Response;
use warp::hyper;
use streaming_platform::MagicBall;
use streaming_platform::{sp_dto::{get_msg_meta, MsgType}, RestreamMsg};
use streaming_platform::tokio::sync::mpsc::UnboundedSender;
use crate::{check_auth_token, response};

pub async fn go(aca_origin: Option<String>, auth_token_key: String, cookie_header: Option<String>, restream_tx: UnboundedSender<RestreamMsg>) -> Result<Response<hyper::body::Body>, warp::Rejection> {
    match check_auth_token(auth_token_key.as_bytes(), cookie_header) {
        Some(auth_data) => {
            let (body_tx, body) = hyper::Body::channel();
            
            match restream_tx.send(RestreamMsg::StartHttp(json!({ }), body_tx, None)) {
				Ok(()) => {
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

pub enum ContentAccessType {
	Raw,
    Download(Option<String>, Option<String>),
    View(Option<String>, Option<String>)
}

pub fn response_for_body(aca_origin: Option<String>, body: hyper::Body) -> Result<Response<hyper::Body>, warp::http::Error> {
	match aca_origin {
		Some(aca_origin) => Response::builder()
			.header("Access-Control-Allow-Credentials", "true")
			.header("Access-Control-Allow-Origin", aca_origin)
			.body(body),
		None => Response::builder().body(body)
	}    
}

pub fn response_for_content(aca_origin: Option<String>, body: hyper::Body, content_access_type: ContentAccessType) -> Result<Response<hyper::Body>, warp::http::Error> {
	match content_access_type {
		ContentAccessType::Raw => {
			Response::builder()						
						.body(body)
		}
		ContentAccessType::Download(file_name, content_type) => {
			let (content_disposition, content_type) = (format!(r#"attachment; filename="{}""#, file_name.unwrap_or("name-missing".to_owned())), content_type.unwrap_or("".to_owned()));

			match aca_origin {
				Some(aca_origin) => {
					Response::builder()
						.header("Access-Control-Allow-Credentials", "true")
						.header("Access-Control-Allow-Origin", aca_origin)
						.header("Content-Disposition", content_disposition)
						.header("Content-Type", content_type)
						.body(body)
				}
				None => {
					Response::builder()
						.header("Content-Disposition", content_disposition)
						.header("Content-Type", content_type)
						.body(body)
		
				}
			}
		}
		ContentAccessType::View(file_name, content_type) => {
			let (content_disposition, content_type) = (format!(r#"inline; filename="{}""#, file_name.unwrap_or("name-missing".to_owned())), content_type.unwrap_or("".to_owned()));

			match aca_origin {
				Some(aca_origin) => {
					Response::builder()
						.header("Access-Control-Allow-Credentials", "true")
						.header("Access-Control-Allow-Origin", aca_origin)
						.header("Content-Disposition", content_disposition)
						.header("Content-Type", content_type)
						.body(body)
				}
				None => {
					Response::builder()
						.header("Content-Disposition", content_disposition)
						.header("Content-Type", content_type)
						.body(body)
		
				}
			}
		}		
	}        
}