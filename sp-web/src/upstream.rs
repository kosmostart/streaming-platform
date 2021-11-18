use log::*;
use warp::Buf;
use warp::{http::Response, hyper};
use streaming_platform::{FrameType, MAX_FRAME_PAYLOAD_SIZE, MagicBall, MsgSpec};
use streaming_platform::futures::stream::{Stream, StreamExt};
use streaming_platform::sp_dto::{
	MsgMeta, MsgType, get_msg_len, get_msg_meta_with_len, raw_dto
};
use crate::{check_auth_token, response_for_body};

#[derive(Debug, Clone)]
enum Step {
	Len,
	MsgMeta,
	StartStream,
	StreamPayload(MsgSpec, u64, u64),
	StreamAttachment(MsgSpec),
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

fn send_data(data: &mut impl Buf, mb: &mut MagicBall, mut step: Step) -> Step {
	loop {
		let len = data.remaining();
		info!("Sending data, step {:?}, len {}", step, len);

		match step {
			Step::StreamPayload(msg_spec, payload_size, mut bytes_sent) => {
				let bytes_left = (payload_size - bytes_sent) as usize;

				match bytes_left <= len {
					true => {

						match bytes_left > MAX_FRAME_PAYLOAD_SIZE {
							true => {
								bytes_sent = bytes_sent + MAX_FRAME_PAYLOAD_SIZE as u64;
								step = Step::StreamPayload(msg_spec, payload_size, bytes_sent);

								mb.frame_type = FrameType::Payload as u8;
								
								let res = mb.send_frame(&data.chunk()[..MAX_FRAME_PAYLOAD_SIZE], MAX_FRAME_PAYLOAD_SIZE);															
								debug!("{:?}", res);
								data.advance(MAX_FRAME_PAYLOAD_SIZE);							
							}
							false => {
								mb.frame_type = FrameType::PayloadEnd as u8;
								
								let res = mb.send_frame(&data.chunk()[..bytes_left], bytes_left);
								debug!("{:?}", res);
								data.advance(bytes_left);

								step = Step::StreamAttachment(msg_spec);
							}
						}
					}
					false => {
						mb.frame_type = FrameType::Payload as u8;

						let bytes_to_send = bytes_left - len;

						match bytes_to_send > MAX_FRAME_PAYLOAD_SIZE {
							true => {
								bytes_sent = bytes_sent + MAX_FRAME_PAYLOAD_SIZE as u64;
								step = Step::StreamPayload(msg_spec, payload_size, bytes_sent);								
								let res = mb.send_frame(&data.chunk()[..MAX_FRAME_PAYLOAD_SIZE], MAX_FRAME_PAYLOAD_SIZE);															
								debug!("{:?}", res);
								data.advance(MAX_FRAME_PAYLOAD_SIZE);
							}
							false => {
								bytes_sent = bytes_sent + bytes_to_send as u64;
								step = Step::StreamPayload(msg_spec, payload_size, bytes_sent);								
								let res = mb.send_frame(&data.chunk()[..bytes_to_send], bytes_to_send);
								debug!("{:?}", res);
								data.advance(bytes_to_send);
							}
						}
					}
				}
			}
			Step::StreamAttachment(_) => {
				mb.frame_type = FrameType::Attachment as u8;

				match len > MAX_FRAME_PAYLOAD_SIZE {
					true => {						
						let res = mb.send_frame(&data.chunk()[..MAX_FRAME_PAYLOAD_SIZE], MAX_FRAME_PAYLOAD_SIZE);
						debug!("{:?}", res);
						data.advance(MAX_FRAME_PAYLOAD_SIZE);
					}
					false => {						
						let res = mb.send_frame(&data.chunk()[..len], len);
						debug!("{:?}", res);
						data.advance(len);
						return step;
					}
				}
			}
			_ => {}
		}
	}
}

pub async fn go(aca_origin: Option<String>, auth_token_key: String, cookie_header: Option<String>, stream: impl Stream<Item = Result<impl Buf, warp::Error>>, mut mb: MagicBall) -> Result<Response<hyper::body::Body>, warp::Rejection> {
	match check_auth_token(auth_token_key.as_bytes(), cookie_header) {
        Some(_auth_data) => {
			let mut state = State {
				step: Step::Len,
				len: 0,
				buf: vec![],
				msg_meta: None
			};					
			
			let final_step = stream.fold(state.step.clone(), |current, mut data| {
				state.step = current.clone();

				match data {
					Ok(ref mut data) => {						
						loop {
							match check(&mut state, data) {
								CheckResult::ContinueWithCurrent => {}
								CheckResult::Next => break
							}
						}

						info!("Loop completed, remaining {} bytes", data.remaining());						
					}
					Err(ref e) => {
						error!("{}", e);
						state.step = Step::Complete(Err(()));
					}
				}

				let mut mb = mb.clone();
				let msg_meta = state.msg_meta.clone();
				let mut step = state.step.clone();

				info!("Starting payload and attachments streaming, stream step: {:?}", step);
				
				async move {
					match step {
						Step::Len => {}
						Step::MsgMeta => {}
						Step::StartStream => {
							match msg_meta {
								Some(msg_meta) => {
									let payload_size = msg_meta.payload_size;
									
									let _ = mb.proxy_rpc_stream(msg_meta).await;
									
									step = Step::StreamPayload(mb.get_msg_spec(), payload_size, 0);

									info!("Stream started (msg meta sent), step {:?}", step);

									match data {
										Ok(mut data) if data.has_remaining() => {
											step = send_data(&mut data, &mut mb, step);
										}
										_ => {}
									}
								}
								None => {
		
								}
							}
						}
						Step::StreamPayload(ref msg_spec, _, _) | Step::StreamAttachment(ref msg_spec) => {
							match data {
								Ok(mut data) if data.has_remaining() => {
									mb.load_msg_spec(msg_spec);
									step = send_data(&mut data, &mut mb, step);
								}
								_ => {}
							}
						}
						Step::Complete(res) => {
							info!("Stream completed with {:?}", res);
						}
					}

					step
				}
			}).await;

			match &final_step {
				Step::StreamPayload(msg_spec, _, _) |
				Step::StreamAttachment(msg_spec) => {
					mb.load_msg_spec(msg_spec);
				}
				_ => {}
			};

			let rpc_res = match state.msg_meta {
				Some(msg_meta) => {
					match msg_meta.msg_type {
						MsgType::RpcRequest => {
							info!("Sending stream completion");
							match mb.complete_rpc_stream_raw(msg_meta.correlation_id).await {
								Ok((resp_msg_meta, resp_payload_data, resp_attachments_data)) => raw_dto(resp_msg_meta, resp_payload_data, resp_attachments_data).expect("Failed to build raw dto for rpc response"),
								Err(e) => {
									error!("Error on receiving stream completion response, {:?}", e);
									vec![]
								}
							}
						}
						_ => {
							let _ = mb.complete_stream();
							vec![]
						}
					}
				}
				None => {
					let _ = mb.complete_stream();
					error!("Failed to properly complete stream as rpc because of empty msg meta, simple completion sent.");
					vec![]
				}
			};

			info!("Stream completed, step {:?}", final_step);

			let body = hyper::Body::from(rpc_res);
			let res = response_for_body(aca_origin, body).expect("Failed to build aca origin response");
		
			Ok::<Response<hyper::Body>, warp::Rejection>(res)
		}
		None => {
			warn!("Unauthorized upstream access attempt");

			let body = hyper::Body::from("Here comes the error");
            let res = response_for_body(aca_origin, body).expect("Failed to build aca origin response");
            Ok::<Response<hyper::Body>, warp::Rejection>(res)                                    
        }
    }    
}