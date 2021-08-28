use std::collections::HashMap;
use std::net::SocketAddr;
use std::env::current_dir;
use log::*;
use serde_json::{Value, from_slice, json, to_vec};
use warp::{Filter, http::{Response, header::SET_COOKIE}, hyper::{self, body::Bytes}, Buf};
use streaming_platform::{MagicBall, tokio::{io::AsyncReadExt}};
use streaming_platform::sp_dto::{uuid::Uuid, MsgMeta};
use streaming_platform::{
	client::stream_mode, ClientMsg, FrameType, StreamCompletion, 
	tokio::{self, sync::{mpsc::{self, UnboundedReceiver, UnboundedSender}, oneshot}}, 
	sp_dto::{Key, Message, Participator, resp, rpc_dto_with_sizes, Route, RouteSpec}, 
	RestreamMsg, StreamLayout, ProcessError, Frame
};
use sp_auth::verify_auth_token;
pub use streaming_platform;

mod authorize;
mod hub;
mod upstream;
mod downstream;

pub async fn process_event(_config: Value, mut _mb: MagicBall, _msg: Message<Value>, _: (), emittable_rx: UnboundedReceiver<Frame>) -> Result<(), Box<dyn std::error::Error>>  {
    Ok(())
}

pub async fn process_rpc(_config: Value, mut _mb: MagicBall, _msg: Message<Value>, _: (), emittable_rx: UnboundedReceiver<Frame>) -> Result<streaming_platform::sp_dto::Response<Value>, Box<dyn std::error::Error>> {
    resp(json!({}))    
}

pub async fn startup2(initial_config: Value, target_config: Value, mb: MagicBall, startup_data: Option<Value>, _: ()) {	
}

pub async fn startup(initial_config: Value, target_config: Value, mb: MagicBall, startup_data: Option<Value>, _: ()) {
	let (mut restream_tx, mut restream_rx) = mpsc::unbounded_channel();
    let mut restream_tx2 = restream_tx.clone();
    
    let web_stream_config = json!({		
        "cfg_host": initial_config["cfg_host"],
		"cfg_domain": initial_config["cfg_domain"],
        "cfg_token": target_config["stream_cfg_token"]
    });	

    tokio::spawn(async move {
        stream_mode(web_stream_config, process_stream, startup2, None, Some(restream_tx2), Some(restream_rx), ()).await;
    });
	
    let listen_addr = target_config["listen_addr"].as_str().expect("Missing listen_addr config value");
    let cert_path = target_config["cert_path"].as_str();
    let key_path = target_config["key_path"].as_str();

    let aca_origin = target_config["aca_origin"].as_str().map(|x| x.to_owned());
    let aca_origin2 = aca_origin.clone();
    let aca_origin3 = aca_origin.clone();
	let aca_origin4 = aca_origin.clone();	
    let mb2 = mb.clone();
    let mb3 = mb.clone();

    let listen_addr = listen_addr.parse::<SocketAddr>().expect("Incorrect listen addr passed");

    let auth_token_key = target_config["auth_token_key"].as_str().map(|x| x.to_owned()).expect("Missing auth_token_key config value");
    let auth_token_key1 = auth_token_key.clone();
    let auth_token_key2 = auth_token_key.clone();
    let auth_token_key3 = auth_token_key.clone();
	let auth_token_key4 = auth_token_key.clone();	

    let mut app_indexes = HashMap::new();
    let mut app_paths = HashMap::new();

    match startup_data {
        Some(startup_data) =>
            match startup_data["apps"].as_array() {
                Some(apps) => {                    
                    for app in apps {
                        match app["name"].as_str() {
                            Some(app_name) => {
                                match app["index"].as_str() {
                                    Some(app_index) => {
                                        app_indexes.insert(app_name.to_owned(), (app["allow_unauthorized"].as_bool(), app_index.to_owned()));
                                    }
                                    None => {}
                                }
                                match app["path"].as_str() {
                                    Some(app_path) => {
                                        app_paths.insert(app_name.to_owned(), (app["allow_unauthorized"].as_bool(), app_path.to_owned()));
                                    }
                                    None => {}
                                }
                            }
                            None => {}
                        }                        
                    }                    
                }
                None => {}
            }
        None => {}
    };

    let deploy_path = match target_config["deploy_path"].as_str() {
        Some(path) => path.to_owned(),
        None => current_dir().expect("failed to get current dir").to_str().expect("failed to get current dir str (for deploy path)").to_owned()
    };

    let routes =                    
        warp::path("hi")
            .map(move || {
                Response::builder()
                    .header("content-type", "text/html")
                    .body("hi")
            })
        .or(
            warp::path("authorize")
                .and(warp::post())                
                .and(warp::body::bytes())
                .and_then(move |body: warp::hyper::body::Bytes| {
                    let aca_origin = aca_origin.clone();
                    let mb = mb.clone();
                    
                    crate::authorize::go(aca_origin, body, mb)
                }
            )
        )
        .or(
            warp::path("app")
                .and(warp::path::param())
                .and(warp::header::optional("cookie"))
                .and(warp::path::end())                                
                .map(move |app_name: String, cookie_header: Option<String>| {
                    let auth_token_key = auth_token_key.clone();
                    let mut app_indexes = app_indexes.clone();

                    match app_indexes.remove(&app_name) {
                        Some((allow_unauthorized, index)) =>
                        
                            match allow_unauthorized == Some(true) {
                                true => Response::builder()
                                    .header("content-type", "text/html")
                                    .body(index),
                                false =>
                                    match check_auth_token(auth_token_key.as_bytes(), cookie_header) {
                                        Some(auth_data) => 
                                            Response::builder()
                                                .header("content-type", "text/html")
                                                .body(index),
                                        None => {
                                            warn!("Unauthorized access attempt, app name: {}", app_name);

                                            Response::builder()
                                                .header("content-type", "text/html")
                                                .body("Here comes the error".to_owned())
                                        }
                                    }
                            }
                        None => 
                            Response::builder()
                                .header("content-type", "text/html")
                                .body("Here comes the error".to_owned())
                    }
                }
            )
        )
        .or(
            warp::path("app")
                .and(warp::path::param())
                .and(warp::header::optional("cookie"))
                .and(warp::path::tail())                                
                .map(move |app_name: String, cookie_header: Option<String>, tail: warp::path::Tail| {
					info!("App access attempt");
					
                    let auth_token_key = auth_token_key1.clone();
                    let deploy_path = deploy_path.clone();
                    let mut app_paths = app_paths.clone();					

                    match app_paths.remove(&app_name) {
                        Some((allow_unauthorized, app_path)) => {

                            match allow_unauthorized == Some(true) {
                                true => process_static_file_request(deploy_path, app_path, tail.as_str().to_owned()),
                                false => 
                                    match check_auth_token(auth_token_key.as_bytes(), cookie_header) {
                                        Some(auth_data) => process_static_file_request(deploy_path, app_path, tail.as_str().to_owned()),
                                        None => {
                                            warn!("Unauthorized file access attempt, app name: {}, tail: {}", app_name, tail.as_str());
                
                                            Response::builder()
                                                .header("content-type", "text/html")
                                                .body(warp::hyper::body::Body::from("Here comes the error"))
                                        }
                                    }
                            }
                        }
                        None => Response::builder()
                            .header("content-type", "text/html")
                            .body(warp::hyper::body::Body::from("Here comes the error"))
                    }                    
                }
            )
        )
        .or(
            warp::path("hub")
                .and(warp::post())
                .and(warp::header::optional("cookie"))
                .and(warp::body::bytes())
                .and_then(move |cookie_header: Option<String>, body: warp::hyper::body::Bytes| {
                    let auth_token_key = auth_token_key2.clone();
                    let aca_origin = aca_origin2.clone();                    
                    let mb = mb2.clone();

                    crate::hub::go(aca_origin, auth_token_key, cookie_header, body, mb)
                }
            )
        )        
		.or(
            warp::path("upstream")
				.and(warp::post())
				.and(warp::body::stream())
                .and(warp::header::optional("cookie"))                
                .and_then(move |stream, cookie_header: Option<String>| {		
					let auth_token_key = auth_token_key3.clone();
                    let aca_origin = aca_origin3.clone();
					let mb = mb3.clone();							

                    crate::upstream::go(aca_origin, auth_token_key, cookie_header, stream, mb)
                }
            )
        )
		.or(
            warp::path("downstream")
                .and(warp::get())
                .and(warp::header::optional("cookie"))                
                .and_then(move |cookie_header: Option<String>| {
                    let auth_token_key = auth_token_key4.clone();
                    let aca_origin = aca_origin4.clone();
					
					let mut restream_tx = restream_tx.clone();

                    crate::downstream::go(aca_origin, auth_token_key, cookie_header, restream_tx)
                }
            )
        )		
    ;

    if cert_path.is_some() && key_path.is_some() {
        warp::serve(routes)
        .tls()
        .cert_path(cert_path.expect("Certificate path failed"))
        .key_path(key_path.expect("Key path failed"))
        .run(listen_addr)
        .await;	
    } else {
        warp::serve(routes)        
        .run(listen_addr)
        .await;
    }
}

fn check_auth_token(auth_token_key: &[u8], cookie_header: Option<String>) -> Option<Value> {
    match cookie_header {
        Some(cookie_string) => {
			let split: Vec<&str> = cookie_string.split(";").collect();

			for pair in split {
				let pair = pair.trim();				

				if pair.len() > 13 && &pair[..13] == "skytfs-token=" {
					match verify_auth_token(auth_token_key, &pair[13..]) {
						Ok(auth_data) => return Some(auth_data),
						Err(e) => {
							warn!("Auth token verification failed, {:?}", e);
							return None;
						}
					}
				}
			}

			None
        }
        None => None
    }
}

fn process_static_file_request(deploy_path: String, app_path: String, tail: String) -> Result<Response<warp::hyper::body::Body>, warp::http::Error> {
    let mime = mime_guess::from_path(&tail).first();

    let (mut tx, body) = warp::hyper::body::Body::channel();

    streaming_platform::tokio::spawn(async move {                                
        let file_path = deploy_path + "/" + &app_path + "/" + &tail;
        let mut file = streaming_platform::tokio::fs::File::open(&file_path).await.expect(&("File not found ".to_owned() + &file_path));                                
        let mut file_buf = [0; 1024];

        loop {
            match file.read(&mut file_buf).await.expect("File read failed") {
                0 => break,
                n =>                                 
                    match tx.send_data(warp::hyper::body::Bytes::copy_from_slice(&file_buf[..n])).await {
                        Ok(_) => {}
                        Err(_) => break
                    }
            }
        }
    });                            

    match mime {
        Some(mime_type) => Response::builder()                                    
            .header("content-type", mime_type.essence_str())
            .body(body),
        None => Response::builder()                                    
            .body(body)
    }
}

pub fn response(aca_origin: String, data: Vec<u8>) -> Result<Response<Vec<u8>>, warp::http::Error> {
    Response::builder()
        .header("Access-Control-Allow-Credentials", "true")
        .header("Access-Control-Allow-Origin", aca_origin)
        .body(data) 
}

pub fn response_with_cookie(aca_origin: String, cookie_header: &str, data: Vec<u8>) -> Result<Response<Vec<u8>>, warp::http::Error> {
    Response::builder()
        .header("Access-Control-Allow-Credentials", "true")
        .header("Access-Control-Allow-Origin", aca_origin)
        .header(SET_COOKIE, cookie_header)
        .body(data)
}

struct DownstreamStreamLayout {
    layout: StreamLayout,
    txs: HashMap<Uuid, (warp::hyper::body::Sender, Option<oneshot::Sender<StreamCompletion>>)>
}

async fn process_client_msg(mb: &mut MagicBall, stream_layouts: &mut HashMap<u64, DownstreamStreamLayout>, client_msg: ClientMsg, restream_tx: &mut UnboundedSender<RestreamMsg>) -> Result<(), ProcessError> {
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
									stream_layouts.insert(frame.stream_id, DownstreamStreamLayout {
										layout: StreamLayout {
											id: frame.stream_id,
											msg_meta: match frame.payload {
                                                Some(payload) => payload[..frame.payload_size as usize].to_vec(),
                                                None => vec![]
                                            },
											payload: vec![],
											attachments_data: vec![]
										},
										txs: HashMap::new()
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
								}
								None => {									
									let mut stream_layout = DownstreamStreamLayout {
										layout: StreamLayout {
											id: frame.stream_id,
											msg_meta: vec![],
											payload: vec![],
											attachments_data: vec![]
										},
										txs: HashMap::new()
									};

									match frame.payload {
                                        Some(payload) => {
                                            stream_layout.layout.msg_meta.extend_from_slice(&payload[..frame.payload_size as usize]);
                                        }
                                        None => {}
                                    }

									let msg_meta: MsgMeta = from_slice(&stream_layout.layout.msg_meta)?;

									stream_layouts.insert(frame.stream_id, stream_layout);
								}
							};
						}
						FrameType::Payload | FrameType::PayloadEnd => {
                            match frame.payload {
                                Some(payload) => {
                                    let stream_layout = stream_layouts.get_mut(&frame.stream_id).ok_or(ProcessError::StreamLayoutNotFound)?;
                                    stream_layout.layout.attachments_data.extend_from_slice(&payload[..frame.payload_size as usize]);
                                }
                                None => {}
                            }
						}
						FrameType::Attachment | FrameType::AttachmentEnd => {
                            info!("Attachment frame");

                            let (get_restreams_tx, get_restreams_rx) = oneshot::channel();
                            match restream_tx.send(RestreamMsg::GetRestreams(get_restreams_tx)) {
                                Ok(()) => {}
                                Err(_) => panic!("InnerMsg::GetRestream send error")
                            }
                            let txs = get_restreams_rx.await.expect("failed to get restream");

							let mut stream_layout = stream_layouts.get_mut(&frame.stream_id).ok_or(ProcessError::StreamLayoutNotFound)?;

                            for (request_id, tx, completion_tx) in txs {
                                stream_layout.txs.insert(request_id, (tx, completion_tx));
                            }

							//stream_layout.layout.attachments_data.extend_from_slice(&frame.payload[..frame.payload_size as usize]);

                            let mut requests_to_remove = vec![];

                            match frame.payload {
                                Some(payload) => {
                                    for (&request_id, (body_tx, _)) in stream_layout.txs.iter_mut() {
                                        let data = Bytes::copy_from_slice(&payload[..frame.payload_size as usize]);
        
                                        match body_tx.send_data(data).await {                
                                            Ok(()) => {
                                                info!("Attachment frame send");
                                            }
                                            Err(_) => {
                                                warn!("Failed to send data with body_tx");
                                                requests_to_remove.push(request_id);
                                            }
                                        }
        
                                    }
                                }
                                None => {}
                            }

                            for request_id in requests_to_remove {
                                let _ = stream_layout.txs.remove(&request_id);
                            }
						}
						FrameType::End => {
							match stream_layouts.remove(&frame.stream_id) {
								Some(mut stream_layout) => {
                                    for (_, (_, completion_tx)) in stream_layout.txs {
                                        match completion_tx {
                                            Some(completion_tx) => {
                                                match completion_tx.send(StreamCompletion::Ok) {
                                                    Ok(()) => {}
                                                    Err(_) => {
                                                        error!("Failed to send stream completion")
                                                    }
                                                }
                                            }
                                            None => {}
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

pub async fn process_stream(config: Value, mut mb: MagicBall, mut rx: UnboundedReceiver<ClientMsg>, mut restream_tx: Option<UnboundedSender<RestreamMsg>>, mut restream_rx: Option<UnboundedReceiver<RestreamMsg>>, _: ()) {    
    let mut restream_tx = restream_tx.expect("Restream tx is empty");
    let mut restream_rx = restream_rx.expect("Restream rx is empty");
    let mut mb2 = mb.clone();

    tokio::spawn(async move {
        let mut restreams = vec![];

        loop {
            match restream_rx.recv().await.expect("restream channel dropped") {
                RestreamMsg::AddRestream(request_id, body_tx, completion_tx) => {
                    info!("RestreamMsg::AddRestream, request id {}", request_id);
                    restreams.push((request_id, body_tx, completion_tx));
                }
                RestreamMsg::GetRestreams(reply) => {
					info!("RestreamMsg::GetRestream");
                    match reply.send(restreams.drain(..).collect()) {
                        Ok(()) => {}
                        Err(_) => panic!("Send restream failed")
                    }
                }
            }
        }
    });

    let mut stream_layouts = HashMap::new();

    loop {        
        let client_msg = rx.recv().await.expect("connection issues acquired");
        let stream_id = client_msg.get_stream_id();

        match process_client_msg(&mut mb2, &mut stream_layouts, client_msg, &mut restream_tx).await {
            Ok(()) => {}
            Err(e) => {				
                error!("{:?}", e);
            }
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