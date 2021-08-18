use warp::Buf;
use warp::{http::Response, hyper};
use streaming_platform::futures::stream::{Stream, StreamExt};
use crate::response_for_body;

pub async fn go(aca_origin: Option<String>, stream: impl Stream<Item = Result<impl Buf, warp::Error>>) -> Result<Response<hyper::body::Body>, warp::Rejection> {
    stream.for_each(|chunk| {            
        async move {
        }
    });
        
	let body = hyper::Body::from("Here comes the error");	
	let res = response_for_body(aca_origin, body).expect("failed to build aca origin response");

	Ok::<Response<hyper::Body>, warp::Rejection>(res)
}