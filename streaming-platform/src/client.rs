use std::{collections::HashMap, hash::Hash};
use std::future::Future;
use std::error::Error;
use log::*;
use tokio::runtime::Runtime;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, UnboundedSender, UnboundedReceiver};
use serde_json::{json, Value, from_slice, to_vec};
use sp_dto::*;
use crate::proto::*;

/// Starts a stream based client based on provided config. Creates new runtime and blocks.
/// Config must have "addr" key, this will be used as address for endpoint, and "host" key - network addr for the server (in host:port format)
/// Config must have "access_key" key, this will be send for optional authorization, more information about this feature will be provided later.
/// process_stream is used for stream of incoming data processing.
/// startup is executed on the start of this function.
/// restream_rx can be used for restreaming data somewhere else, for example returning data for incoming web request
/// dependency is w/e clonable dependency needed when processing data.
/// The protocol message format is in sp-dto crate.
pub fn start_stream<T: 'static, R: 'static, D: 'static>(config: HashMap<String, String>, process_stream: ProcessStream<T, D>, startup: Startup<R, D>, startup_data: Option<Value>, restream_tx: Option<UnboundedSender<RestreamMsg>>, restream_rx: Option<UnboundedReceiver<RestreamMsg>>, dependency: D) 
where 
    T: Future<Output = ()> + Send,
    R: Future<Output = ()> + Send,
    D: Clone + Send + Sync
{        
    let addr = config.get("addr").expect("Missing addr config value").to_owned();
    let host = config.get("host").expect("Missing host config value").to_owned();    
    let access_key = config.get("access_key").expect("Missing access_key config value").to_owned();
    let rt = Runtime::new().expect("Failed to create runtime");
    rt.block_on(stream_mode(&host, &addr, &access_key, process_stream, startup, config, startup_data, restream_tx, restream_rx, dependency));
}

/// Starts a message based client based on provided config. Creates new runtime and blocks.
/// Config must have "addr" key, this will be used as address for endpoint, and "host" key - network addr for the server (in host:port format)
/// process_event is used for processing incoming message, which are marked as events via message msg_type.
/// process_rpc is used for processing incoming message, which are marked as rpc request via message msg_type.
/// startup is executed on the start of this function.
/// dependency is w/e clonable dependency needed when processing data.
/// The protocol message format is in sp-dto crate.
pub fn start_full_message<T: 'static, Q: 'static, R: 'static, D: 'static>(config: HashMap<String, String>, process_event: ProcessEvent<T, Value, D>, process_rpc: ProcessRpc<Q, Value, D>, startup: Startup<R, D>, startup_data: Option<Value>, dependency: D) 
where 
    T: Future<Output = Result<(), Box<dyn Error>>> + Send,
    Q: Future<Output = Result<Response<Value>, Box<dyn Error>>> + Send,
    R: Future<Output = ()> + Send,
    D: Clone + Send + Sync
{    
    let addr = config.get("addr").expect("Missing addr config value").to_owned();
    let host = config.get("host").expect("Missing host config value").to_owned();
    let access_key = config.get("access_key").expect("Missing access_key config value").to_owned();
    let rt = Runtime::new().expect("Failed to create runtime");
    rt.block_on(full_message_mode(&host, &addr, &access_key, process_event, process_rpc, startup, config, startup_data, dependency));
}

/// Future for stream based client based on provided config.
/// "addr" value will be used as address for endpoint, "host" value - network addr for the server (in host:port format)
/// "access_key" value will be send for optional authorization, more information about this feature will be provided later.
/// process_event is used for processing incoming message, which are marked as events via message msg_type.
/// process_rpc is used for processing incoming message, which are marked as rpc request via message msg_type.
/// startup is executed on the start of this function.
/// restream_rx can be used for restreaming data somewhere else, for example returning data for incoming web request
/// dependency is w/e clonable dependency needed when processing data.
/// The protocol message format is in sp-dto crate.
pub async fn stream_mode<T: 'static, R: 'static, D: 'static>(host: &str, addr: &str, access_key: &str, process_stream: ProcessStream<T, D>, startup: Startup<R, D>, config: HashMap<String, String>, startup_data: Option<Value>, restream_tx: Option<UnboundedSender<RestreamMsg>>, restream_rx: Option<UnboundedReceiver<RestreamMsg>>, dependency: D)
where 
    T: Future<Output = ()> + Send,
    R: Future<Output = ()> + Send,
    D: Clone + Send + Sync
{    
    let (read_tx, read_rx) = mpsc::unbounded_channel();
    let (write_tx, write_rx) = mpsc::unbounded_channel();
    let (rpc_inbound_tx, mut rpc_inbound_rx) = mpsc::unbounded_channel();
    let (rpc_outbound_tx, mut _rpc_outbound_rx) = mpsc::unbounded_channel();
    let addr = addr.to_owned();
    let addr2 = addr.to_owned();   
    let addr3 = addr.to_owned();
    let access_key = access_key.to_owned();        
    let write_tx2 = write_tx.clone();    
    tokio::spawn(async move {
        let mut rpcs = HashMap::new();        

        loop {
            let msg = rpc_inbound_rx.recv().await.expect("rpc inbound msg receive failed");

            match msg {
                RpcMsg::AddRpc(correlation_id, rpc_tx) => {
                    rpcs.insert(correlation_id, rpc_tx);
                }                
                RpcMsg::RpcDataRequest(correlation_id) => {
                    match rpcs.remove(&correlation_id) {
                        Some(rpc_tx) => {
                            match rpc_outbound_tx.send(RpcMsg::RpcDataResponse(correlation_id, rpc_tx)) {
                                Ok(()) => {}
                                Err(_) => panic!("rpc outbound tx send failed on rpc data request")
                            }
                        }
                        None => {                            
                        }
                    }
                }
                _=> {                    
                }
            }
        }
    });  
    let mb = MagicBall::new(addr2, write_tx2, rpc_inbound_tx);
    tokio::spawn(process_stream(config.clone(), mb.clone(), read_rx, restream_tx, restream_rx, dependency.clone()));
    tokio::spawn(startup(config, mb, startup_data, dependency));
    connect_stream_future(host, addr3, access_key, read_tx, write_rx).await;
}

/// Future for message based client based on provided config.
/// "addr" value will be used as address for endpoint, "host" value - network addr for the server (in host:port format)
/// "access_key" value will be send for optional authorization, more information about this feature will be provided later.
/// process_stream is used for stream of incoming data processing.
/// startup is executed on the start of this function.
/// restream_rx can be used for restreaming data somewhere else, for example returning data for incoming web request
/// dependency is w/e clonable dependency needed when processing data.
/// The protocol message format is in sp-dto crate.
pub async fn full_message_mode<P: 'static, T: 'static, Q: 'static, R: 'static, D: 'static>(host: &str, addr: &str, access_key: &str, process_event: ProcessEvent<T, P, D>, process_rpc: ProcessRpc<Q, P, D>, startup: Startup<R, D>, config: HashMap<String, String>, startup_data: Option<Value>, dependency: D)
where 
    T: Future<Output = Result<(), Box<dyn Error>>> + Send,
    Q: Future<Output = Result<Response<P>, Box<dyn Error>>> + Send,
    R: Future<Output = ()> + Send,
    P: serde::Serialize, for<'de> P: serde::Deserialize<'de> + Send,
    D: Clone + Send + Sync
{    
    let (read_tx, mut read_rx) = mpsc::unbounded_channel();
    let (write_tx, write_rx) = mpsc::unbounded_channel();
    let (rpc_inbound_tx, mut rpc_inbound_rx) = mpsc::unbounded_channel();
    let (rpc_outbound_tx, mut rpc_outbound_rx) = mpsc::unbounded_channel();

    let addr = addr.to_owned();
    let addr2 = addr.to_owned();
    let addr3 = addr.to_owned();
    let access_key = access_key.to_owned();
    let rpc_inbound_tx2 = rpc_inbound_tx.clone();
    
    let write_tx2 = write_tx.clone();
    let write_tx3 = write_tx.clone();

    tokio::spawn(async move {
        let mut rpcs = HashMap::new();        

        loop {
            let msg = rpc_inbound_rx.recv().await.expect("Rpc inbound msg receive failed");

            match msg {
                RpcMsg::AddRpc(correlation_id, rpc_tx) => {
                    rpcs.insert(correlation_id, rpc_tx);

                    info!("Add rpc ok {}", correlation_id);
                }                
                RpcMsg::RpcDataRequest(correlation_id) => {
                    match rpcs.remove(&correlation_id) {
                        Some(rpc_tx) => {
                            match rpc_outbound_tx.send(RpcMsg::RpcDataResponse(correlation_id, rpc_tx)) {
                                Ok(()) => {}
                                Err(_) => panic!("Rpc outbound tx send failed on rpc data request")
                            }
                            //info!("send rpc response ok {}", correlation_id);
                        }
                        None => error!("Send rpc response not found {}", correlation_id)
                    }
                }
                _=> {                    
                }
            }
        }
    });    

    tokio::spawn(async move {
        let mb = MagicBall::new(addr2, write_tx2, rpc_inbound_tx);        

        tokio::spawn(startup(config.clone(), mb.clone(), startup_data, dependency.clone()));

        loop {                        
            let msg = match read_rx.recv().await {
                Some(msg) => msg,
                None => {
                    info!("Client connection dropped");
                    break;
                }
            };
            let mut mb = mb.clone();
            let config = config.clone();
            let dependency = dependency.clone();

            match msg {
                ClientMsg::Message(_, msg_meta, payload, attachments_data) => {
                    match msg_meta.msg_type {
                        MsgType::Event => {          

                            debug!("Client got event {}", msg_meta.display());

                            tokio::spawn(async move {
                                let key = msg_meta.key.clone();
                                let payload: P = from_slice(&payload).expect("Failed to deserialize event payload");                                
                                if let Err(e) = process_event(config, mb.clone(), Message {meta: msg_meta, payload, attachments_data}, dependency).await {
                                    error!("Process event error {}, {:?}, {:?}", mb.addr.clone(), key, e);
                                }
                                debug!("Client {} process_event succeeded", mb.addr);
                            });                            
                        }
                        MsgType::RpcRequest => {                        
                            debug!("Client got rpc request {}", msg_meta.display());

                            tokio::spawn(async move {                                
                                let mut route = msg_meta.route.clone();
                                let correlation_id = msg_meta.correlation_id;                                
                                let key = msg_meta.key.clone();
                                let payload: P = from_slice(&payload).expect("failed to deserialize rpc request payload");

                                let (payload, attachments, attachments_data, rpc_result) = match process_rpc(config.clone(), mb.clone(), Message {meta: msg_meta, payload, attachments_data}, dependency).await {
                                    Ok(res) => {
                                        debug!("Client {} process_rpc succeeded", mb.addr);
                                        let (res, attachments, attachments_data) = match res {
                                            Response::Simple(payload) => (payload, vec![], vec![]),
                                            Response::Full(payload, attachments, attachments_data) => (payload, attachments, attachments_data)
                                        };
                                        (to_vec(&res).expect("Failed to serialize rpc process result"), attachments, attachments_data, RpcResult::Ok)
                                    }
                                    Err(e) =>  {
                                        error!("Process rpc error {}, {:?}, {:?}", mb.addr.clone(), key, e);
                                        (to_vec(&json!({ "err": e.to_string() })).expect("failed to serialize rpc process error result"), vec![], vec![], RpcResult::Err)
                                    }
                                };                                

                                route.points.push(Participator::Service(mb.addr.clone()));

                                let key_hash = get_key_hash(key.clone());

                                let (res, msg_meta_size, payload_size, attachments_sizes) = rpc_response_dto2_sizes(mb.addr.clone(),  key, correlation_id, payload, attachments, attachments_data, rpc_result, route, None, None).expect("failed to create rpc reply");

                                debug!("Client {} attempt to write rpc response", mb.addr);

                                let stream_id = mb.get_stream_id();

                                mb.write_full_message(MsgType::RpcResponse(RpcResult::Ok).get_u8(), key_hash, stream_id, res, msg_meta_size, payload_size, attachments_sizes, true).await.expect("Failed to write rpc response");
                             
                                debug!("Client {} write rpc response succeded", mb.addr);
                            });                            
                        }
                        MsgType::RpcResponse(_) => {           
                            debug!("Client got rpc response {}", msg_meta.display());

                            match rpc_inbound_tx2.send(RpcMsg::RpcDataRequest(msg_meta.correlation_id)) {
                                Ok(()) => {
                                    debug!("Client RpcDataRequest send succeeded {}", msg_meta.display());
                                }
                                Err(_) => panic!("Rpc inbound tx2 msg send failed on rpc response")
                            }
                            let msg = rpc_outbound_rx.recv().await.expect("Rpc outbound msg receive failed");                            

                            match msg {
                                RpcMsg::RpcDataResponse(received_correlation_id, rpc_tx) => {
                                    match received_correlation_id == msg_meta.correlation_id {
                                        true => {                                            
                                            match rpc_tx.send((msg_meta, payload, attachments_data)) {
                                                Ok(()) => {
                                                    debug!("Client {} RpcDataResponse receive succeeded", mb.addr);
                                                }
                                                Err((msg_meta, _, _)) => error!("rpc_tx send failed on rpc response {:?}", msg_meta)
                                            }
                                        }
                                        false => error!("Received_correlation_id not equals correlation_id: {}, {}", received_correlation_id, msg_meta.correlation_id)
                                    }
                                }
                                _ => error!("Client handler: wrong RpcMsg")
                            }                                
                        }
                    }
                }
                _ => {}
            }
        }    
    });

    connect_full_message_future(host, addr3, access_key, read_tx, write_rx).await;
}

async fn auth(addr: String, access_key: String, tcp_stream: &mut TcpStream) -> Result<(), ProcessError> {
    let route = Route {
        source: Participator::Service(addr.clone()),
        spec: RouteSpec::Simple,
        points: vec![Participator::Service(addr.clone())]
    };  

    let (dto, msg_meta_size, payload_size, attachments_size) = rpc_dto_with_sizes(addr.clone(), Key::simple("Auth"), json!({
        "access_key": access_key
    }), route, None, None).expect("Failed to create auth dto");

    write_to_tcp_stream(tcp_stream, 0, 0, get_stream_id_onetime(&addr), dto, msg_meta_size, payload_size, attachments_size, true).await
}


async fn connect_stream_future(host: &str, addr: String, access_key: String, read_tx: UnboundedSender<ClientMsg>, write_rx: UnboundedReceiver<Frame>) {
    let mut write_stream = TcpStream::connect(host).await.expect("Connection to host failed");
    auth(addr.clone(), access_key.clone(), &mut write_stream).await.expect("Write stream authorization failed");

    let mut read_stream = TcpStream::connect(host).await.expect("Connection to host failed");
    auth(addr.clone(), access_key, &mut read_stream).await.expect("Read stream authorization failed");

    info!("Connected in stream mode to {} as {}", host, addr);

    let res = process_stream_mode(addr, write_stream, read_stream, read_tx, write_rx).await;

    info!("{:?}", res);
}

async fn connect_full_message_future(host: &str, addr: String, access_key: String, read_tx: UnboundedSender<ClientMsg>, write_rx: UnboundedReceiver<Frame>) {    
    let mut write_stream = TcpStream::connect(host).await.expect("Connection to host failed");
    auth(addr.clone(), access_key.clone(), &mut write_stream).await.expect("Write stream authorization failed");

    let mut read_stream = TcpStream::connect(host).await.expect("Connection to host failed");
    auth(addr.clone(), access_key, &mut read_stream).await.expect("Read stream authorization failed");

    info!("Connected in full message mode to {} as {}", host, addr);

    let res = process_full_message_mode(addr, write_stream, read_stream, read_tx, write_rx).await;

    info!("{:?}", res);
}

async fn process_stream_mode(addr: String, mut write_tcp_stream: TcpStream, mut read_tcp_stream: TcpStream, read_tx: UnboundedSender<ClientMsg>, write_rx: UnboundedReceiver<Frame>) -> Result<(), ProcessError> {
    //let (auth_msg_meta, auth_payload, auth_attachments) = read_full(&mut socket_read).await?;
    //let auth_payload: Value = from_slice(&auth_payload)?;    

    //println!("auth {:?}", auth_msg_meta);
    //println!("auth {:?}", auth_payload);
    
    let addr2 = addr.clone();

    tokio::spawn(async move {
        let res = write_loop(addr2, write_rx, &mut write_tcp_stream).await;
        error!("{:?}", res);
    });

    let mut state = State::new();

	loop {
		match state.read_frame() {
			ReadFrameResult::NotEnoughBytesForFrame => {
				state.read_from_tcp_stream(&mut read_tcp_stream).await?
			}
			ReadFrameResult::NextStep => {}
			ReadFrameResult::Frame(frame) => {
				debug!("Stream frame read, frame type {}, msg type {}, stream id {}", frame.frame_type, frame.msg_type, frame.stream_id);

				match read_tx.send(ClientMsg::Frame(frame)) {
					Ok(()) => {}
					Err(_) => {                        
						panic!("Client message send with read_tx in stream mode failed");
					}
				}				
			}
		}
	}
}

async fn process_full_message_mode(addr: String, mut write_tcp_stream: TcpStream, mut read_tcp_stream: TcpStream, read_tx: UnboundedSender<ClientMsg>, write_rx: UnboundedReceiver<Frame>) -> Result<(), ProcessError> {    
    //let (auth_msg_meta, auth_payload, auth_attachments) = read_full(&mut socket_read).await?;
    //let auth_payload: Value = from_slice(&auth_payload)?;    

    //println!("auth {:?}", auth_msg_meta);
    //println!("auth {:?}", auth_payload);
            
    let mut stream_layouts: HashMap<u64, StreamLayout> = HashMap::new();
    let mut state = State::new();

    let addr2 = addr.clone();

    tokio::spawn(async move {
        let res = write_loop(addr2, write_rx, &mut write_tcp_stream).await;
        error!("{:?}", res);
    });

	loop {
		match state.read_frame() {
			ReadFrameResult::NotEnoughBytesForFrame => {
				state.read_from_tcp_stream(&mut read_tcp_stream).await?
			}
			ReadFrameResult::NextStep => {}
			ReadFrameResult::Frame(frame) => {
				debug!("Full message stream frame read, frame type {}, msg type {}, stream id {}", frame.frame_type, frame.msg_type, frame.stream_id);

				match frame.get_frame_type() {
					Ok(frame_type) => {
						match frame_type {
							FrameType::MsgMeta | FrameType::MsgMetaEnd => {
								match stream_layouts.get_mut(&frame.stream_id) {
									Some(stream_layout) => {
                                        match frame.payload {
                                            Some(payload) => {
                                                stream_layout.msg_meta.extend_from_slice(&payload[..frame.payload_size as usize]);
                                            }
                                            None => {}
                                        }
									}
									None => {
										stream_layouts.insert(frame.stream_id, StreamLayout {
											id: frame.stream_id,
											msg_meta: match frame.payload {
                                                Some(payload) => payload[..frame.payload_size as usize].to_vec(),
                                                None => vec![]
                                            },
											payload: vec![],
											attachments_data: vec![]
										});
									}
								}
							}
							FrameType::Payload | FrameType::PayloadEnd => {
                                match frame.payload {
                                    Some(payload) => {
                                        let stream_layout = stream_layouts.get_mut(&frame.stream_id).ok_or(ProcessError::StreamLayoutNotFound)?;
								        stream_layout.payload.extend_from_slice(&payload[..frame.payload_size as usize]);
                                    }
                                    None => {}
                                }
							}
							FrameType::Attachment | FrameType::AttachmentEnd => {
                                match frame.payload {
                                    Some(payload) => {
                                        let stream_layout = stream_layouts.get_mut(&frame.stream_id).ok_or(ProcessError::StreamLayoutNotFound)?;
								        stream_layout.attachments_data.extend_from_slice(&payload[..frame.payload_size as usize]);
                                    }
                                    None => {}
                                }
							}
							FrameType::End => {
								let stream_layout = stream_layouts.remove(&frame.stream_id).ok_or(ProcessError::StreamLayoutNotFound)?;

								let msg_meta = from_slice(&stream_layout.msg_meta)?;
		
								match read_tx.send(ClientMsg::Message(frame.stream_id, msg_meta, stream_layout.payload, stream_layout.attachments_data)) {
									Ok(()) => {}
									Err(_) => {
										panic!("Client message send with read_tx in full message mode failed")
									}
								}
							}
						}
					}
					Err(e) => {
						error!("Get frame type failed in read: {:?}, addr {}", e, addr);
					}
				}				
			}
		}
	}  
}