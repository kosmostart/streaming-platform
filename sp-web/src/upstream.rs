use log::*;
use serde_json::{error, json};
use warp::Buf;
use warp::{http::Response, hyper};
use streaming_platform::MagicBall;
use streaming_platform::futures::stream::{Stream, StreamExt};
use streaming_platform::sp_dto::{Key, MsgMeta, get_msg_len, get_msg_meta_with_len};
use crate::{check_auth_token, response_for_body};

#[derive(Debug, Clone)]
enum Step {
	Len,
	MsgMeta,
	StartStream,
	Stream,
	Complete(Result<(), ()>)
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

				match data.remaining() >= state.len {
					true => {
						let msg_meta = serde_json::from_slice::<MsgMeta>(&data.chunk()[..state.len]).unwrap();
						data.advance(state.len);								
						info!("msg meta by data is {:?}", msg_meta);

						state.msg_meta = Some(msg_meta);
						state.step = Step::StartStream;

						match data.has_remaining() {
							true => return CheckResult::ContinueWithCurrent,
							false => return CheckResult::Next
						}
					}
					false => return CheckResult::ContinueWithCurrent
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
				state.step = Step::StartStream;
			}
		}
		_ => {}
	}

	CheckResult::Next
}

pub async fn go(aca_origin: Option<String>, auth_token_key: String, cookie_header: Option<String>, stream: impl Stream<Item = Result<impl Buf, warp::Error>>, mut mb: MagicBall) -> Result<Response<hyper::body::Body>, warp::Rejection> {
	match check_auth_token(auth_token_key.as_bytes(), cookie_header) {
        Some(auth_data) => {
			let mut state = State {
				step: Step::Len,
				len: 0,
				buf: vec![],
				msg_meta: None
			};
			
			let mut msg_meta = None;			
			
			stream.scan(state.step.clone(), |current, mut data| {
				state.step = current.clone();

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
						state.step = Step::Complete(Err(()));
					}
				}

				let mut mb = mb.clone();
				let msg_meta = msg_meta.clone();
				let mut step = state.step.clone();
				
				async move {
					match step {
						Step::Len => {}
						Step::MsgMeta => {}
						Step::StartStream => {
							match msg_meta {
								Some(msg_meta) => {										
									let _ = mb.proxy_rpc_stream(msg_meta).await;
									step = Step::Stream;
								}
								None => {
		
								}
							}
						}
						Step::Stream => {

						}
						Step::Complete(res) => {
							info!("Stream competed with {:?}", res);
						}
					}

					Some(step)
				}
			}).for_each(|_| async {}).await;

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