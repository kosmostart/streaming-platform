use std::collections::HashMap;
use std::fs;
use serde_json::{json, Value, from_slice, to_vec, to_string, from_str, from_value};
use log::*;
use tokio::{io::AsyncWriteExt, fs::File, sync::mpsc::{UnboundedSender, UnboundedReceiver}};
use sysinfo::{ProcessExt, SystemExt};
use streaming_platform::{ClientMsg, Frame, FrameType, MAX_FRAME_PAYLOAD_SIZE, MagicBall, ProcessError, RestreamMsg, StreamLayout, client, sp_cfg, sp_dto::{MsgMeta, MsgType, rpc_response_dto2_sizes, Participator, RpcResult}, tokio::{self, io::AsyncReadExt}};
use sp_build_core::{unpack, RunConfig};

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

    let config = json!({
        "cfg_host": "127.0.0.1:11002",
        "cfg_token": "Pod"
    });

    client::start_stream(config, process_stream, startup, None, None, None, ());
}

pub async fn startup(_initial_config: Value, _target_config: Value, mut _mb: MagicBall, _startup_data: Option<Value>, _: ()) {
}

pub async fn process_stream(config: Value, mut mb: MagicBall, mut rx: UnboundedReceiver<ClientMsg>, _: Option<UnboundedSender<RestreamMsg>>, _: Option<UnboundedReceiver<RestreamMsg>>, _: ()) {
    let mut stream_layouts = HashMap::new();

    loop {        
        let client_msg = rx.recv().await.expect("connection issues acquired");
        let stream_id = client_msg.get_stream_id();
        match process_client_msg(&mut mb, &mut stream_layouts, client_msg).await {
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

async fn process_client_msg(mb: &mut MagicBall, stream_layouts: &mut HashMap<u64, FileStreamLayout>, client_msg: ClientMsg) -> Result<(), Error> {
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
						FrameType::Payload => {
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
										"DeployUnit" => {							
											let payload: Value = from_slice(&stream_layout.layout.payload)?;
                                            info!("{:#?}", payload);
											let path = String::new() + payload["deploy_unit_name"].as_str().ok_or(Error::None)?;
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

                                    let file = stream_layout.file.as_mut().ok_or(Error::None).unwrap();
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
                                    let file = stream_layout.file.as_mut().ok_or(Error::None)?;
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
                                    match stream_layout.msg_meta {
                                        Some(msg_meta) => {
                                            match msg_meta.key.action.as_ref() {
                                                "DeployUnit" => {
                                                    let mut payload = stream_layout.payload.ok_or(Error::None)?;
                                                    let deploy_unit_name = payload["deploy_unit_name"].as_str().ok_or(Error::None)?;

                                                    info!("Deploy unit name: {}", deploy_unit_name);

                                                    let mut unpack_result_msg;
                                                    let mut run_config_msg = None;
                                                    let mut run_result_msg = vec![];

                                                    match unpack(".".to_owned(), deploy_unit_name.to_owned()) {
                                                        Ok(()) => {
                                                            unpack_result_msg = "Unpack result is Ok".to_owned();
                                                            info!("{}", unpack_result_msg);

                                                            let run_config: Option<RunConfig> = from_value(payload["run_config"].take())?;

                                                            match run_config {
                                                                Some(run_config) => {
                                                                    run_config_msg = Some(format!("Run config passed, run units amount is {}", run_config.run_units.len()));

                                                                    for run_unit in run_config.run_units {
                                                                        fix_running(&run_unit.name);

                                                                        info!("Starting {}", run_unit.name);

                                                                        let mut run_result;

                                                                        let res = match run_unit.config {
                                                                            Some(config) => {
                                                                                match to_string(&config) {
                                                                                    Ok(config) => std::process::Command::new(run_unit.path.clone() + "/" + &run_unit.name)
                                                                                        .arg(config)
                                                                                        .spawn(),
                                                                                    Err(e) => Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Config serialization to JSON string failed, {:?}", e)))
                                                                                }
                                                                            },
                                                                            None => std::process::Command::new(run_unit.path.clone() + "/" + &run_unit.name)                                    
                                                                                .spawn()
                                                                        };

                                                                        match res {
                                                                            Ok(instance) => {
                                                                                run_result = format!("Started process {} with id {}, path {}", run_unit.name, instance.id(), run_unit.path);
                                                                            }
                                                                            Err(e) => {
                                                                                run_result = format!("Failed to start process {}, {:?}, path {}", run_unit.name, e, run_unit.path);
                                                                            }
                                                                        }

                                                                        run_result_msg.push(run_result);
                                                                    }
                                                                }
                                                                None => {
                                                                    run_config_msg = Some("Run config not passed".to_owned());
                                                                }
                                                            }
                                                        }
                                                        Err(e) => {
                                                            unpack_result_msg = format!("Unpack result is Err, {:?}", e);
                                                            error!("{}", unpack_result_msg);
                                                        }
                                                    }
                                                    
                                                    mb.send_rpc_response(msg_meta, json!({
                                                        "unpack_result": unpack_result_msg,
                                                        "run_config_msg": run_config_msg,
                                                        "run_result_msg": run_result_msg
                                                    })).await?;
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

    Ok(())
}

async fn send_file(mut mb: MagicBall, msg_meta: MsgMeta, path: std::path::PathBuf, file_name: String) -> Result<(), Error> {
    let mut file = File::open(&path).await?;
    let size = file.metadata().await?.len();

    mb.start_rpc_stream_response(msg_meta, json!({
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

fn fix_running(name: &str) {
    info!("Fixing running processes for {}", name);
    info!("Quering system data");

    let mut system = sysinfo::System::new();
    // First we update all information of our system struct.
    system.refresh_all();
    let mut running = vec![];

    for (id, process) in system.get_processes() {
        //info!("{}:{} status: {:?}", pid, proc_.name(), proc_.status());

        running.push((*id as usize, process.name().to_owned()));
    }

    info!("Processes running: {}", running.len());

    for (id, process_name) in running {
        if process_name == name {
            stop_process_by_id(id, &process_name);
        }
    }

    info!("Done for {}", name);
}

fn stop_process_by_id(id: usize, name: &str) {
    info!("Attempt to stop process {} with id {}", name, id);

    if cfg!(windows) {
        std::process::Command::new("taskkill")
            .arg("/F")
            .arg("/PID")
            .arg(id.to_string())
            .output()
            .expect(&format!("Process stop failed, name {}, id {}", name, id));
    }
    else {
        std::process::Command::new("kill")            
            .arg(id.to_string())
            .output()
            .expect(&format!("Process stop failed, name {}, id {}", name, id));
    }

    info!("Process stop result is Ok, name {}, id {}", name, id);
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