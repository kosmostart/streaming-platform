use log::*;
use serde_json::json;
use warp::Buf;
use warp::{http::Response, hyper};
use streaming_platform::MagicBall;
use streaming_platform::futures::stream::{Stream, StreamExt};
use streaming_platform::sp_dto::Key;
use crate::{check_auth_token, response_for_body};

pub async fn go(aca_origin: Option<String>, auth_token_key: String, cookie_header: Option<String>, stream: impl Stream<Item = Result<impl Buf, warp::Error>>, mut mb: MagicBall) -> Result<Response<hyper::body::Body>, warp::Rejection> {
	match check_auth_token(auth_token_key.as_bytes(), cookie_header) {
        Some(auth_data) => {

			mb.start_rpc_stream(Key::new("Upload", "File", "File"), json!({
				//"file_name": file_name
			})).await.unwrap();

			stream.for_each(|chunk| {            
				async move {
				}
			});
				
			let body = hyper::Body::from("Here comes the error");	
			let res = response_for_body(aca_origin, body).expect("failed to build aca origin response");
		
			Ok::<Response<hyper::Body>, warp::Rejection>(res)
		}
		None => {
			warn!("Unauthorized upstream access attempt");

			let body = hyper::Body::from("Here comes the error");
            let res = response_for_body(aca_origin, body).expect("failed to build aca origin response");
            Ok::<Response<hyper::Body>, warp::Rejection>(res)                                    
        }
    }    
}