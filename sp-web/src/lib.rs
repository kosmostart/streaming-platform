#![feature(proc_macro_hygiene)]
#![feature(async_closure)]
use std::collections::HashMap;
use std::net::SocketAddr;
use std::env::current_dir;
use log::*;
use serde_json::{Value, from_slice, json, to_vec};
use warp::{Filter, http::{Response, header::SET_COOKIE}, hyper::body::Bytes};
use streaming_platform::{MagicBall, tokio::{io::AsyncReadExt}};
use streaming_platform::sp_dto::{uuid::Uuid, MsgMeta};
use streaming_platform::{client::stream_mode, ClientMsg, FrameType, StreamCompletion, tokio::{self, sync::{mpsc::{self, UnboundedReceiver, UnboundedSender}, oneshot}}, sp_dto::{Key, Message, Participator, resp, rpc_dto_with_correlation_id_sizes, Route, RouteSpec}, RestreamMsg, StreamLayout, ProcessError};
use sp_auth::verify_auth_token;
pub use streaming_platform;

mod authorize;
mod hub;
mod downstream;

mod sse_stream {    
    use streaming_platform::tokio::sync::mpsc::{self, UnboundedSender};
    use async_stream::stream;
    use tokio_stream::Stream;
    use warp::sse::Event;

    #[derive(Debug)]
    pub enum Error {
        Kick
    }

    impl std::fmt::Display for Error {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "SuperErrorError is here!")
        }
    }

    impl std::error::Error for Error {}

    pub fn new(is_error: bool) -> (Option<UnboundedSender<Event>>, impl Stream<Item = Result<Event, Error>>) {
        let (tx, mut rx) = mpsc::unbounded_channel();

        let stream = stream! {
            match is_error {
                true => yield Err(Error::Kick),
                false => 
                    while let Some(event) = rx.recv().await {
                        yield Ok(event);
                    }      
            }   
        };

        (match is_error {
            true => None,
            false => Some(tx)
        }, stream)
    }
}

pub async fn process_event(_config: HashMap<String, String>, mut _mb: MagicBall, _msg: Message<Value>, _: ()) -> Result<(), Box<dyn std::error::Error>>  {
    Ok(())
}

pub async fn process_rpc(_config: HashMap<String, String>, mut _mb: MagicBall, _msg: Message<Value>, _: ()) -> Result<streaming_platform::sp_dto::Response<Value>, Box<dyn std::error::Error>> {
    resp(json!({}))    
}

pub async fn startup(config: HashMap<String, String>, mb: MagicBall, startup_data: Option<Value>, _: ()) {	
	let (mut restream_tx, mut restream_rx) = mpsc::unbounded_channel();
    let mut restream_tx2 = restream_tx.clone();
    let config2 = config.clone();

    tokio::spawn(async move {
        let stream_addr = config2.get("stream_addr").expect("missing stream_addr config value").to_owned();
        let host = config2.get("host").expect("missing host config value").to_owned();
        let access_key = "";
        stream_mode(&host, &stream_addr, access_key, process_stream, async move |_, _, _, _| {}, config2, None, Some(restream_tx2), Some(restream_rx), ()).await;
    });
	
    let listen_addr = config.get("listen_addr").expect("Missing listen_addr config value");
    let cert_path = config.get("cert_path");
    let key_path = config.get("key_path");

    let aca_origin = config.get("aca_origin").map(|x| x.to_owned());
    let aca_origin2 = aca_origin.clone();
    let aca_origin3 = aca_origin.clone();
	let aca_origin4 = aca_origin.clone();
    let mb2 = mb.clone();
    let mb3 = mb.clone();

    let listen_addr = listen_addr.parse::<SocketAddr>().expect("Incorrect listen addr passed");

    let auth_token_key = config.get("auth_token_key").map(|x| x.to_owned()).expect("Missing auth_token_key config value");
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

    let deploy_path = match config.get("deploy_path") {
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
            warp::path("events")
                .and(warp::get())
                .and(warp::header::optional("cookie"))
                .map(move |cookie_header: Option<String>| {
                    let auth_token_key = auth_token_key3.clone();
                    let aca_origin = aca_origin3.clone();                    
                    let mb = mb3.clone();

                    match check_auth_token(auth_token_key.as_bytes(), cookie_header) {
                        Some(auth_data) => {
                            let (tx, stream) = sse_stream::new(false);
                            tx.expect("Empty sse tx").send(warp::sse::Event::default().data("Hello"));

                            warp::sse::reply(stream)
                        }
                        None => {
                            warn!("Unauthorized events access attempt");
                            
                            //let (_, stream) = sse_stream::new(true);
                            let (tx, stream) = sse_stream::new(false);
                            tx.expect("Empty sse tx").send(warp::sse::Event::default().data("Hello"));

                            warp::sse::reply(stream)
                        }
                    }
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
									stream_layout.layout.msg_meta.extend_from_slice(&frame.payload[..frame.payload_size as usize]);
								}
								None => {									
									stream_layouts.insert(frame.stream_id, DownstreamStreamLayout {
										layout: StreamLayout {
											id: frame.stream_id,
											msg_meta: frame.payload[..frame.payload_size as usize].to_vec(),
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
									stream_layout.layout.msg_meta.extend_from_slice(&frame.payload[..frame.payload_size as usize]);

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

									stream_layout.layout.msg_meta.extend_from_slice(&frame.payload[..frame.payload_size as usize]);									

									let msg_meta: MsgMeta = from_slice(&stream_layout.layout.msg_meta)?;

									stream_layouts.insert(frame.stream_id, stream_layout);
								}
							};
						}
						FrameType::Payload | FrameType::PayloadEnd => {
							let stream_layout = stream_layouts.get_mut(&frame.stream_id).ok_or(ProcessError::StreamLayoutNotFound)?;
							stream_layout.layout.attachments_data.extend_from_slice(&frame.payload[..frame.payload_size as usize]);
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

                            for (&request_id, (body_tx, _)) in stream_layout.txs.iter_mut() {
                                let data = Bytes::copy_from_slice(&frame.payload[..frame.payload_size as usize]);

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

	/*
    match client_msg {
        ClientMsg::MsgMeta(stream_id, msg_meta) => {
            let (get_restream_tx, get_restream_rx) = oneshot::channel();
            match inner_tx.send(InnerMsg::GetRestream(msg_meta.correlation_id.to_string(), get_restream_tx)) {
                Ok(()) => {}
                Err(_) => panic!("InnerMsg::GetRestream send error")
            }
            let (body_tx, completion_tx) = get_restream_rx.await.expect("failed to get restream");                                
            stream_layouts.insert(stream_id, DownloadStreamLayout {
                stream: StreamLayout {
                    id: stream_id,
                    msg_meta,
                    payload: vec![],
                    attachments_data: vec![]
                },
                body_tx: Some(body_tx),
                completion_tx
            });                
        } 
        ClientMsg::PayloadData(stream_id, n, buf) => {                                
            match stream_layouts.get_mut(&stream_id) {
                Some(stream_layout) => {
                    stream_layout.stream.payload.extend_from_slice(&buf[..n]);
                }
                None => error!("not found stream {} for payload data", stream_id)
            }                
        }
        ClientMsg::PayloadFinished(stream_id, n, buf) => {
            match stream_layouts.get_mut(&stream_id) {
                Some(stream_layout) => {
                    stream_layout.stream.payload.extend_from_slice(&buf[..n]);
                }
                None => error!("not found stream {} for payload data", stream_id)
            }
        }        
        ClientMsg::AttachmentData(stream_id, index, n, buf) => {                
            let stream_layout = stream_layouts.get_mut(&stream_id).ok_or(Error::CustomError("not found stream for attachment data".to_owned()))?;
            let body_tx = stream_layout.body_tx.as_mut().ok_or(Error::CustomError("body tx is empty for attachment data".to_owned()))?;
            let unit = Bytes::copy_from_slice(&buf[..n]);
            match body_tx.send_data(unit).await {
                Ok(()) => {}
                Err(e) => return Err(Error::CustomError(format!("{:?}", e)))
            }  
        }
        ClientMsg::AttachmentFinished(stream_id, index, n, buf) => {
            let stream_layout = stream_layouts.get_mut(&stream_id).ok_or(Error::CustomError("not found stream for attachment finish".to_owned()))?;
            let body_tx = stream_layout.body_tx.as_mut().ok_or(Error::CustomError("body tx is empty for attachment finish".to_owned()))?;
            let unit = Bytes::copy_from_slice(&buf[..n]);
            match body_tx.send_data(unit).await {                
                Ok(()) => {}
                Err(e) => return Err(Error::CustomError(format!("{:?}", e)))
            }
            match stream_layout.completion_tx.take() {
                Some(completion_tx) => {
                    match completion_tx.send(StreamCompletion::Ok) {
                        Ok(()) => {}
                        Err(_) => return Err(Error::CustomError("failed to send stream completion".to_owned()))
                    }
                }
                None => {}
            }            
        }
        ClientMsg::MessageFinished(stream_id) => {
            let _ = stream_layouts.remove(&stream_id).ok_or(Error::CustomError("not found stream for message finish".to_owned()))?;
        }
        _ => {}            
    }
	*/

    Ok(())
}

pub async fn process_stream(config: HashMap<String, String>, mut mb: MagicBall, mut rx: UnboundedReceiver<ClientMsg>, mut restream_tx: Option<UnboundedSender<RestreamMsg>>, mut restream_rx: Option<UnboundedReceiver<RestreamMsg>>, _: ()) {    
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
				/*
                match stream_id {
                    Some(stream_id) => {
                        match stream_layouts.remove(&stream_id) {
                            Some(stream_layout) => {
                                match stream_layout.stream.msg_meta.kind {
                                    MsgKind::RpcRequest => {
                                        let mut route = stream_layout.stream.msg_meta.route.clone();
                                        route.points.push(Participator::Service(mb2.addr.clone()));
                                        let (res, msg_meta_size, payload_size, attachments_size) = rpc_response_dto2_sizes(
                                            mb2.addr.clone(), 
                                            stream_layout.stream.msg_meta.tx.clone(), 
                                            stream_layout.stream.msg_meta.key.clone(), 
                                            stream_layout.stream.msg_meta.correlation_id, 
                                            vec![],
                                            vec![], vec![],
                                            RpcResult::Err,
                                            route,
                                            None,
                                            None
                                        ).expect("failed to create rpc reply");
                                        mb2.write_vec(stream_layout.stream.id, res, msg_meta_size, payload_size, attachments_size).await.expect("failed to write response to upload");
                                    }
                                    _ => {}
                                }                        
                            }
                            None => {}
                        }
                    }
                    None => {

                    }
                } 
				*/               
                error!("{:?}", e);
            }
        }
    }
}

/*
struct DownloadStreamLayout {
    stream: StreamLayout,
    body_tx: Option<hyper::body::Sender>,
    completion_tx: Option<oneshot::Sender<StreamCompletion>>
}

pub enum InnerMsg {
    AddRestream(String, hyper::body::Sender, Option<oneshot::Sender<StreamCompletion>>),
    GetRestream(String, oneshot::Sender<(hyper::body::Sender, Option<oneshot::Sender<StreamCompletion>>)>)
}

pub async fn process_stream(config: HashMap<String, String>, mut mb: MagicBall, mut rx: UnboundedReceiver<ClientMsg>, mut restream_rx: Option<UnboundedReceiver<RestreamMsg>>) {    
    let mut restream_rx = restream_rx.expect("restream rx is empty");
    let (mut inner_tx, mut inner_rx) = mpsc::unbounded_channel();
    let mut inner_tx2 = inner_tx.clone();
    let mut mb2 = mb.clone();
    tokio::spawn(async move {
        let mut restreams = HashMap::new();
        loop {
            match inner_rx.recv().await.expect("restream channel dropped") {
                InnerMsg::AddRestream(correlation_id, body_tx, completion_tx) => {
                    restreams.insert(correlation_id, (body_tx, completion_tx));
                }                
                InnerMsg::GetRestream(correlation_id, reply) => {
                    let restream = restreams.remove(&correlation_id).expect("restream not found for get");
                    match reply.send(restream) {
                        Ok(()) => {}
                        Err(_) => panic!("send restream failed")
                    }
                }
            }
        }
    });    
    tokio::spawn(async move {
        loop {
            match restream_rx.recv().await.expect("restream channel dropped") {
                RestreamMsg::StartHttp(payload, body_tx, completion_tx) => {
                    let (correlation_id, dto, msg_meta_size, payload_size, attachments_sizes) = rpc_dto_with_correlation_id_sizes(
                        mb.addr.clone(),
                        "File".to_owned(), 
                        "Download".to_owned(), 
                        payload, 
                        Route {
                            source: Participator::Service(mb.addr.clone()),
                            spec: RouteSpec::Simple,
                            points: vec![Participator::Service(mb.addr.clone())]
                        },
                        None,
                        None
                    ).expect("failed to create download rpc dto");
                    match inner_tx2.send(InnerMsg::AddRestream(correlation_id.to_string(), body_tx, completion_tx)) {
                        Ok(()) => {}
                        Err(_) => panic!("InnerMsg::Addrestream send error")
                    }
                    let stream_id = mb.get_stream_id();
                    mb.write_vec(
                        stream_id,
                        dto, 
                        msg_meta_size, 
                        payload_size, 
                        attachments_sizes
                    ).await.expect("failed to write download rpc dto");
                }
                _ => error!("incorrect restream msg")
            }
        }
    });
    let mut stream_layouts = HashMap::new();
    loop {        
        let client_msg = rx.recv().await.expect("connection issues acquired");
        let stream_id = client_msg.get_stream_id();
        match process_client_msg(&mut mb2, &mut stream_layouts, client_msg, &mut inner_tx).await {
            Ok(()) => {}
            Err(e) => {
                match stream_id {
                    Some(stream_id) => {
                        match stream_layouts.remove(&stream_id) {
                            Some(stream_layout) => {
                                match stream_layout.stream.msg_meta.kind {
                                    MsgKind::RpcRequest => {
                                        let mut route = stream_layout.stream.msg_meta.route.clone();
                                        route.points.push(Participator::Service(mb2.addr.clone()));
                                        let (res, msg_meta_size, payload_size, attachments_size) = rpc_response_dto2_sizes(
                                            mb2.addr.clone(), 
                                            stream_layout.stream.msg_meta.tx.clone(), 
                                            stream_layout.stream.msg_meta.key.clone(), 
                                            stream_layout.stream.msg_meta.correlation_id, 
                                            vec![],
                                            vec![], vec![],
                                            RpcResult::Err,
                                            route,
                                            None,
                                            None
                                        ).expect("failed to create rpc reply");
                                        mb2.write_vec(stream_layout.stream.id, res, msg_meta_size, payload_size, attachments_size).await.expect("failed to write response to upload");
                                    }
                                    _ => {}
                                }                        
                            }
                            None => {}
                        }
                    }
                    None => {

                    }
                }                
                error!("{:?}", e);
            }
        }
    }
}
*/

/*
async fn process_client_msg(mb: &mut MagicBall, stream_layouts: &mut HashMap<u64, DownloadStreamLayout>, client_msg: ClientMsg, inner_tx: &mut UnboundedSender<InnerMsg>) -> Result<(), Error> {
    match client_msg {
        ClientMsg::MsgMeta(stream_id, msg_meta) => {
            let (get_restream_tx, get_restream_rx) = oneshot::channel();
            match inner_tx.send(InnerMsg::GetRestream(msg_meta.correlation_id.to_string(), get_restream_tx)) {
                Ok(()) => {}
                Err(_) => panic!("InnerMsg::GetRestream send error")
            }
            let (body_tx, completion_tx) = get_restream_rx.await.expect("failed to get restream");                                
            stream_layouts.insert(stream_id, DownloadStreamLayout {
                stream: StreamLayout {
                    id: stream_id,
                    msg_meta,
                    payload: vec![],
                    attachments_data: vec![]
                },
                body_tx: Some(body_tx),
                completion_tx
            });
        } 
        ClientMsg::PayloadData(stream_id, n, buf) => {                                
            match stream_layouts.get_mut(&stream_id) {
                Some(stream_layout) => {
                    stream_layout.stream.payload.extend_from_slice(&buf[..n]);
                }
                None => error!("not found stream {} for payload data", stream_id)
            }                
        }
        ClientMsg::PayloadFinished(stream_id, n, buf) => {
            match stream_layouts.get_mut(&stream_id) {
                Some(stream_layout) => {
                    stream_layout.stream.payload.extend_from_slice(&buf[..n]);
                }
                None => error!("not found stream {} for payload data", stream_id)
            }
        }        
        ClientMsg::AttachmentData(stream_id, index, n, buf) => {                
            let stream_layout = stream_layouts.get_mut(&stream_id).ok_or(Error::CustomError("not found stream for attachment data".to_owned()))?;
            let body_tx = stream_layout.body_tx.as_mut().ok_or(Error::CustomError("body tx is empty for attachment data".to_owned()))?;
            let unit = Bytes::copy_from_slice(&buf[..n]);
            match body_tx.send_data(unit).await {
                Ok(()) => {}
                Err(e) => return Err(Error::CustomError(format!("{:?}", e)))
            }  
        }
        ClientMsg::AttachmentFinished(stream_id, index, n, buf) => {
            let stream_layout = stream_layouts.get_mut(&stream_id).ok_or(Error::CustomError("not found stream for attachment finish".to_owned()))?;
            let body_tx = stream_layout.body_tx.as_mut().ok_or(Error::CustomError("body tx is empty for attachment finish".to_owned()))?;
            let unit = Bytes::copy_from_slice(&buf[..n]);
            match body_tx.send_data(unit).await {                
                Ok(()) => {}
                Err(e) => return Err(Error::CustomError(format!("{:?}", e)))
            }
            match stream_layout.completion_tx.take() {
                Some(completion_tx) => {
                    match completion_tx.send(StreamCompletion::Ok) {
                        Ok(()) => {}
                        Err(_) => return Err(Error::CustomError("failed to send stream completion".to_owned()))
                    }
                }
                None => {}
            }            
        }
        ClientMsg::MessageFinished(stream_id) => {
            let _ = stream_layouts.remove(&stream_id).ok_or(Error::CustomError("not found stream for message finish".to_owned()))?;
        }
        _ => {}            
    }
    Ok(())
}
*/

/*
pub enum ContentAccessType {
    Download(Option<String>, Option<String>),
    View(Option<String>, Option<String>)
}

pub fn response_for_content(aca_origin: Option<String>, body: hyper::Body, content_access_type: ContentAccessType) -> Result<Response<hyper::Body>, warp::http::Error> {
    let (content_disposition, content_type) = match content_access_type {
        ContentAccessType::Download(file_name, content_type) => (format!(r#"attachment; filename="{}""#, file_name.unwrap_or("name-missing".to_owned())), content_type.unwrap_or("".to_owned())),
        ContentAccessType::View(file_name, content_type) => (format!(r#"inline; filename="{}""#, file_name.unwrap_or("name-missing".to_owned())), content_type.unwrap_or("".to_owned()))
    };
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
*/