use log::*;
use serde_json::{error, json};
use warp::Buf;
use warp::{http::Response, hyper};
use streaming_platform::MagicBall;
use streaming_platform::futures::stream::{Stream, StreamExt};
use streaming_platform::sp_dto::{Key, MsgMeta, get_msg_len, get_msg_meta_with_len};
use crate::{check_auth_token, response_for_body};

pub async fn go(aca_origin: Option<String>, auth_token_key: String, cookie_header: Option<String>, stream: impl Stream<Item = Result<impl Buf, warp::Error>>, mut mb: MagicBall) -> Result<Response<hyper::body::Body>, warp::Rejection> {
	match check_auth_token(auth_token_key.as_bytes(), cookie_header) {
        Some(auth_data) => {			

			#[derive(Debug)]
			enum Step {
				Len,
				MsgMeta,
				Done
			}

			struct State {
				pub step: Step,
				pub len: usize,
				pub buf: Vec<u8>,
				pub msg_meta: Option<MsgMeta>
			}

			enum CheckResult {
				Next,
				ContinueWithCurrent
			}
			
			fn check(state: &mut State, data: &mut impl Buf) -> CheckResult {				
				//info!("{:?}", state.step);
				
				match state.step {
					Step::Len => {

						if data.remaining() >= 4 {
							state.len = data.get_u32() as usize;
							info!("len by data is {}", state.len);
							
							state.step = Step::MsgMeta;

							if data.remaining() >= state.len {
								let msg_meta = serde_json::from_slice::<MsgMeta>(&data.chunk()[..state.len]).unwrap();
								data.advance(state.len);
								info!("msg meta by data is {:?}", msg_meta);

								state.msg_meta = Some(msg_meta);

								state.step = Step::Done;

								if data.has_remaining() {
									return CheckResult::ContinueWithCurrent;
								} else {
									return CheckResult::Next;
								}
							} else {
								return CheckResult::ContinueWithCurrent;
							}
						}

						while data.has_remaining() {
							state.buf.push(data.get_u8());
						}

						if state.buf.len() >= 4 {
							state.len = get_msg_len(&state.buf[..4]) as usize;
							info!("len is {}", state.len);
							state.step = Step::MsgMeta;
						}
						if state.buf.len() > 4 {
							return CheckResult::ContinueWithCurrent;
						}
					}
					Step::MsgMeta => {

						while data.has_remaining() && state.buf.len() < state.len + 4 {
							state.buf.push(data.get_u8());
						}

						if state.buf.len() >= state.len + 4 {
							let msg_meta = get_msg_meta_with_len(&state.buf, state.len).unwrap();
							info!("msg meta is {:?}", msg_meta);
							state.msg_meta = Some(msg_meta);
							state.step = Step::Done;
						}
					}
					Step::Done => {						
					}
				}

				CheckResult::Next
			}

			let mut state = State {
				step: Step::Len,
				len: 0,
				buf: vec![],
				msg_meta: None
			};
			
			let mut msg_meta = None;

			stream.for_each(|mut data| {
				match data {
					Ok(mut data) => {						
						loop {
							match check(&mut state, &mut data) {
								CheckResult::ContinueWithCurrent => {}
								CheckResult::Next => break
							}
						}

						info!("Loop completed, remaining {} bytes", data.remaining());						

						msg_meta = state.msg_meta.take();
					}
					Err(e) => {
						error!("{}", e);
					}
				}


				let mut mb = mb.clone();
				let msg_meta = msg_meta.clone();

				async move {
					match msg_meta {
						Some(msg_meta) => {
							let _ = mb.proxy_rpc_stream(msg_meta).await;
						}
						None => {

						}
					}					
				}
			}).await;
				
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