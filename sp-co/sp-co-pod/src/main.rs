#![feature(try_trait)]
use std::collections::HashMap;
use std::fs;
use serde_json::{json, Value, from_slice, to_vec, to_string, from_str};
use log::*;
use tokio::{io::AsyncWriteExt, fs::File, sync::mpsc::{UnboundedSender, UnboundedReceiver}};
use streaming_platform::{ClientMsg, Frame, FrameType, MAX_FRAME_PAYLOAD_SIZE, MagicBall, ProcessError, RestreamMsg, StreamLayout, client::start_stream, sp_cfg, sp_dto::{MsgMeta, MsgType, rpc_response_dto2_sizes, Participator, RpcResult}, tokio::{self, io::AsyncReadExt}};

struct FileStreamLayout {
    layout: StreamLayout,
	msg_meta: Option<MsgMeta>,
    payload: Option<Value>,
    download_payload: Option<Value>,    
    file: Option<File>,
    rpc_result: RpcResult
}

fn main() {
    env_logger::init();

    let mut config = HashMap::new();

    config.insert("addr".to_owned(), "Pod".to_owned());
    config.insert("host".to_owned(), "127.0.0.1:11001".to_owned());
    config.insert("access_key".to_owned(), "".to_owned());

    start_stream(config, process_stream, startup, None, None, None, ());
}

pub async fn startup(_config: HashMap<String, String>, mut _mb: MagicBall, _startup_data: Option<Value>, _: ()) {
}

pub async fn process_stream(config: HashMap<String, String>, mut mb: MagicBall, mut rx: UnboundedReceiver<ClientMsg>, _: Option<UnboundedSender<RestreamMsg>>, _: Option<UnboundedReceiver<RestreamMsg>>, _: ()) {
    let dirs: Vec<sp_cfg::Dir> = vec![];
    let mut stream_layouts = HashMap::new();

    loop {        
        let client_msg = rx.recv().await.expect("connection issues acquired");
        let stream_id = client_msg.get_stream_id();
        match process_client_msg(&mut mb, &mut stream_layouts, &dirs, client_msg).await {
            Ok(()) => {}
            Err(e) => {
                error!("Process client msg error, {:?}", e);
                /*
                match stream_id {
                    Some(stream_id) => {
                        match stream_layouts.remove(&stream_id) {
                            Some(stream_layout) => {
                                match stream_layout.stream.msg_meta.msg_type {
                                    MsgType::RpcRequest => {
                                        let mut route = stream_layout.stream.msg_meta.route.clone();
                                        route.points.push(Participator::Service(mb.addr.clone()));
                                        let (res, msg_meta_size, payload_size, attachments_size) = rpc_response_dto2_sizes(
                                            mb.addr.clone(),                                            
                                            stream_layout.stream.msg_meta.key.clone(),
                                            stream_layout.stream.msg_meta.correlation_id, 
                                            vec![],
                                            vec![], vec![],
                                            RpcResult::Err,
                                            route,
                                            mb.auth_token.clone(),
                                            mb.auth_data.clone()
                                        ).expect("failed to create rpc reply");
                                        mb.write_vec(stream_layout.stream.id, res, msg_meta_size, payload_size, attachments_size).await.expect("failed to write response to upload");
                                    }
                                    _ => {}
                                }                        
                            }
                            None => {}
                        }
                        error!("{:?}", e);
                    }
                    None => {}
                }
                */
            }
        }
    }
}

async fn process_client_msg(mb: &mut MagicBall, stream_layouts: &mut HashMap<u64, FileStreamLayout>, dirs: &Vec<sp_cfg::Dir>, client_msg: ClientMsg) -> Result<(), Error> {
    match client_msg {
		ClientMsg::Frame(frame) => {
			match frame.get_frame_type() {
				Ok(frame_type) => {
					match frame_type {
						FrameType::MsgMeta => {						
							match stream_layouts.get_mut(&frame.stream_id) {
								Some(stream_layout) => {
                                    match frame.payload {
                                        Some(payload) => {
                                            stream_layout.layout.msg_meta.extend_from_slice(&payload[..frame.payload_size as usize]);
                                        }
                                        None => {}
                                    }
								}
								None => {									
									stream_layouts.insert(frame.stream_id, FileStreamLayout {
										layout: StreamLayout {
											id: frame.stream_id,
											msg_meta: match frame.payload {
                                                Some(payload) => payload[..frame.payload_size as usize].to_vec(),
                                                None => vec![]
                                            },
											payload: vec![],
											attachments_data: vec![]
										},
										msg_meta:None,
										payload: None,
										download_payload: None,
										file: None,
										rpc_result: RpcResult::Ok
									});
								}
							}
						}
						FrameType::MsgMetaEnd => {
							info!("MsgMetaEnd frame");
							
							match stream_layouts.get_mut(&frame.stream_id) {
								Some(stream_layout) => {
                                    match frame.payload {
                                        Some(payload) => {
                                            stream_layout.layout.msg_meta.extend_from_slice(&payload[..frame.payload_size as usize]);	
                                        }
                                        None => {}
                                    }

									let msg_meta: MsgMeta = from_slice(&stream_layout.layout.msg_meta)?;

                                    info!("Started stream {:?}", msg_meta.key);

									stream_layout.msg_meta = Some(msg_meta);
								}
								None => {									
									let mut stream_layout = FileStreamLayout {
										layout: StreamLayout {
											id: frame.stream_id,
											msg_meta: vec![],
											payload: vec![],
											attachments_data: vec![]
										},
										msg_meta: None,
										payload: None,
										download_payload: None,
										file: None,
										rpc_result: RpcResult::Ok
									};

                                    match frame.payload {
                                        Some(payload) => {
                                            stream_layout.layout.msg_meta.extend_from_slice(&payload[..frame.payload_size as usize]);	
                                        }
                                        None => {}
                                    }									

									let msg_meta: MsgMeta = from_slice(&stream_layout.layout.msg_meta)?;
                                    info!("Started stream {:?}", msg_meta.key);
                                    stream_layout.msg_meta = Some(msg_meta);

									stream_layouts.insert(frame.stream_id, stream_layout);
								}
							};
						}						
						FrameType::Payload  => {
                            match frame.payload {
                                Some(payload) => {
                                    let stream_layout = stream_layouts.get_mut(&frame.stream_id).ok_or(ProcessError::StreamLayoutNotFound)?;
                                    stream_layout.layout.payload.extend_from_slice(&payload[..frame.payload_size as usize]);
                                }
                                None => {}
                            }					
						}
						FrameType::PayloadEnd => {
							let stream_layout = stream_layouts.get_mut(&frame.stream_id).ok_or(ProcessError::StreamLayoutNotFound)?;

                            match frame.payload {
                                Some(payload) => {
                                    stream_layout.layout.payload.extend_from_slice(&payload[..frame.payload_size as usize]);
                                }
                                None => {}
                            }							

							match &stream_layout.msg_meta {
								Some(msg_meta) => {
									match msg_meta.key.action.as_ref() {
										"DeployPack" => {							
											let payload: Value = from_slice(&stream_layout.layout.payload)?;
                                            info!("{:#?}", payload);
											let path = String::new() + payload["file_name"].as_str()?;
                                            info!("Creating file {}", path);
											stream_layout.file = Some(File::create(path).await?);
											stream_layout.payload = Some(payload);
										}
										_ => {}
									}
								}
								None => {
									error!("Msg meta empty, stream id {}", frame.stream_id);
								}
							}
						}
						FrameType::Attachment => {
                            info!("Attachment frame");
                            match frame.payload {
                                Some(payload) => {
                                    let stream_layout = stream_layouts.get_mut(&frame.stream_id).ok_or(ProcessError::StreamLayoutNotFound)?;

                                    let file = stream_layout.file.as_mut()?;
							        file.write_all(&payload[..frame.payload_size as usize]).await?;
                                }
                                None => {}                                
                            }													
						}						
						FrameType::AttachmentEnd => {
                            info!("Attachment end frame");							
							let stream_layout = stream_layouts.get_mut(&frame.stream_id).ok_or(ProcessError::StreamLayoutNotFound)?;

                            match frame.payload {
                                Some(payload) => {
                                    let file = stream_layout.file.as_mut()?;
							        file.write_all(&payload[..frame.payload_size as usize]).await?;
                                }
                                None => {}                                
                            }

							stream_layout.file = None;
						}
						FrameType::End => {
                            info!("Stream end frame");	
							match stream_layouts.remove(&frame.stream_id) {
								Some(mut stream_layout) => {
                                    match &stream_layout.msg_meta {
                                        Some(msg_meta) => {
                                            match msg_meta.key.action.as_ref() {
                                                "DeployPack" => {
                                                    let payload = stream_layout.payload?;
                                                    let file_name = payload["file_name"].as_str()?;

                                                    info!("File name: {}", file_name);                                                    
                                                }
                                                _ => {}
                                            }
                                        }
                                        None => {
                                            error!("Msg meta empty, stream id {}", frame.stream_id);
                                        }
                                    }
                                }
								None => {
									error!("Not found stream layout for stream end");
								}
							}
						}
					}

				}
				Err(e) => {
					error!("Get frame type failed, frame type {}, {:?}", frame.frame_type, e);
				}
			}
		}
		_ => {
			error!("Incorrect client message received")
		}
	}

    /*
    match client_msg {
        ClientMsg::MsgMeta(stream_id, msg_meta) => {            
            stream_layouts.insert(stream_id, FileStreamLayout {
                stream: StreamLayout {
                    id: stream_id,
                    msg_meta,
                    payload: vec![],
                    attachments_data: vec![]
                },
                payload: None,
                download_payload: None,                
                file: None,
                rpc_result: RpcResult::Ok
            });
        }
        ClientMsg::PayloadData(stream_id, n, buf) => {
            let stream_layout = stream_layouts.get_mut(&stream_id).ok_or(Error::CustomError("not found stream for payload data".to_owned()))?;
            stream_layout.stream.payload.extend_from_slice(&buf[..n]);            
        }
        ClientMsg::PayloadFinished(stream_id, n, buf) => {
            let stream_layout = stream_layouts.get_mut(&stream_id).ok_or(Error::CustomError("not found stream for payload finish".to_owned()))?;
            stream_layout.stream.payload.extend_from_slice(&buf[..n]);
            match stream_layout.stream.msg_meta.key.action.as_ref() {
                "Upload" => {
                    let _attachment = stream_layout.stream.msg_meta.attachments.iter().nth(0).ok_or(Error::CustomError("no attachment found in msg meta for upload key".to_owned()))?;
                    let payload: Value = from_slice(&stream_layout.stream.payload)?;                    
                    let path = String::new();
                    stream_layout.file = Some(File::create(path).await?);
                    stream_layout.payload = Some(payload);
                }
                "Download" => {
                    let payload: Value = from_slice(&stream_layout.stream.payload)?;
                    stream_layout.download_payload = Some(payload);                        
                }
                _ => {}
            }                        
        }
        ClientMsg::AttachmentData(stream_id, _index, n, buf) => {
            let stream_layout = stream_layouts.get_mut(&stream_id).ok_or(Error::CustomError("not found stream for attachment data".to_owned()))?;
            match stream_layout.stream.msg_meta.key.action.as_ref() {
                "Upload" => {
                    let file = stream_layout.file.as_mut().ok_or(Error::CustomError("file is empty for attachment data".to_owned()))?;
                    file.write_all(&buf[..n]).await?;
                }
                _ => {}
            }                                                
        }
        ClientMsg::AttachmentFinished(stream_id, _index, n, buf) => {
            let stream_layout = stream_layouts.get_mut(&stream_id).ok_or(Error::CustomError("not found stream for attachment finish".to_owned()))?;
            match stream_layout.stream.msg_meta.key.action.as_ref() {
                "Upload" => {
                    let file = stream_layout.file.as_mut().ok_or(Error::CustomError("file is empty for attachment data".to_owned()))?;
                    file.write_all(&buf[..n]).await?;
                    stream_layout.file = None;
                }
                _ => {}
            }                                        
        }
        ClientMsg::MessageFinished(stream_id) => {                                
            let stream_layout = stream_layouts.remove(&stream_id).ok_or(Error::CustomError("not found stream for message finish".to_owned()))?;
            match stream_layout.stream.msg_meta.key.action.as_ref() {                            
                "Upload" => {                                        
                    let _payload = stream_layout.payload.ok_or(Error::CustomError("empty payload for upload message finish".to_owned()))?;                                        
                    let reponse_payload = to_vec(&json!({

                    }))?;
                    let mut route = stream_layout.stream.msg_meta.route.clone();
                    route.points.push(Participator::Service(mb.addr.clone()));
                    let (res, msg_meta_size, payload_size, attachments_size) = rpc_response_dto2_sizes(
                        mb.addr.clone(),                        
                        stream_layout.stream.msg_meta.key.clone(), 
                        stream_layout.stream.msg_meta.correlation_id, 
                        reponse_payload,
                        vec![], vec![],
                        stream_layout.rpc_result.clone(),
                        route,
                        mb.auth_token.clone(),
                        mb.auth_data.clone()
                    )?;
                    mb.write_vec(stream_layout.stream.id, res, msg_meta_size, payload_size, attachments_size).await.expect("failed to write response to upload");
                }
                "Download" => {
                    let FileStreamLayout { 
                        stream,
                        payload: _,
                        download_payload,                        
                        file: _,
                        rpc_result: _
                    } = stream_layout;
                    let payload = download_payload.ok_or(Error::CustomError("empty download payload for download message finish".to_owned()))?;
                    let access_key = payload["access_key"].as_str().ok_or(Error::OptionIsNone("access_key".to_owned()))?;                    
                    let target_dir = dirs.iter().find(|x| x.access_key == access_key).ok_or(Error::TargetDirNotFoundByAccessKey)?;
                    let path = fs::read_dir(&target_dir.path)?.nth(0).ok_or(Error::NoFilesInTargetDir)??.path();

                    let file_name = path.file_name()
                        .ok_or(Error::FileNameIsEmpty)?
                        .to_str()
                        .ok_or(Error::FileNameIsEmpty)?
                        .to_owned();

                    if path.is_file() {                
                        let mb = mb.clone();

                        tokio::spawn(async move {                    
                            match download_file(mb, stream.msg_meta, path, file_name.clone()).await {
                                Ok(()) => {
                                    info!("file download complete, name {}", file_name);
                                }
                                Err(e) => {
                                    error!("download file error {:?}", e);
                                }
                            }
                        });
                    } else {
                        println!("not a file my friends");
                    }                                                            
                }
                _ => {}
            }
        }
        _ => {}
    }
    */
    Ok(())
}

async fn send_file(mut mb: MagicBall, msg_meta: MsgMeta, path: std::path::PathBuf, file_name: String) -> Result<(), Error> {
    let mut file = File::open(&path).await?;
    let size = file.metadata().await?.len();

    mb.stream_rpc_response(msg_meta, json!({
        "file_name": file_name
    })).await?;

    match size {
        0 => {
        }
        _ => {
            let mut buf = [0; MAX_FRAME_PAYLOAD_SIZE];

            loop {
                match file.read(&mut buf).await? {
                    0 => {
                        mb.complete_attachment()?;
                        break;
                    }
                    n => {                
                        mb.send_frame(&buf, n)?;
                    }
                }
            }
        }
    }

    mb.complete_stream()?;

    Ok(())
}

#[derive(Debug)]
pub enum Error {
	None,
	Io(std::io::Error),
    SerdeJson(serde_json::Error),
    StreamingPlatform(streaming_platform::ProcessError),
    SendFrame,
    FileNameIsEmpty,
    TargetDirNotFoundByAccessKey,
    NoFilesInTargetDir,
    OptionIsNone(String),
    CustomError(String)
}

impl From<std::option::NoneError> for Error {
	fn from(_e: std::option::NoneError) -> Error {		
		Error::None
	}
}

impl From<std::io::Error> for Error {
	fn from(e: std::io::Error) -> Error {
		Error::Io(e)
	}
}

impl From<serde_json::Error> for Error {
	fn from(e: serde_json::Error) -> Error {
		Error::SerdeJson(e)
	}
}

impl From<streaming_platform::ProcessError> for Error {
    fn from(e: streaming_platform::ProcessError) -> Error {
        Error::StreamingPlatform(e)
    }
}

impl From<tokio::sync::mpsc::error::SendError<streaming_platform::Frame>> for Error {
    fn from(_: tokio::sync::mpsc::error::SendError<streaming_platform::Frame>) -> Error {
        Error::SendFrame
    }
}