use std::{collections::HashMap, hash::Hash};
use std::future::Future;
use std::error::Error;
use log::*;
use tokio::{io::AsyncWriteExt, runtime::Runtime};
use tokio::net::TcpStream;
use tokio::sync::{watch, mpsc::{self, UnboundedSender, UnboundedReceiver}};
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
pub fn start_stream<T: 'static, R: 'static, D: 'static>(config: Value, process_stream: ProcessStream<T, D>, startup: Startup<R, D>, startup_data: Option<Value>, restream_tx: Option<UnboundedSender<RestreamMsg>>, restream_rx: Option<UnboundedReceiver<RestreamMsg>>, dependency: D) 
where 
    T: Future<Output = ()> + Send,
    R: Future<Output = ()> + Send,
    D: Clone + Send + Sync
{        
    let rt = Runtime::new().expect("Failed to create runtime");
    rt.block_on(stream_mode(config, process_stream, startup, startup_data, restream_tx, restream_rx, dependency));
}

/// Starts a message based client based on provided config. Creates new runtime and blocks.
/// Config must have "addr" key, this will be used as address for endpoint, and "host" key - network addr for the server (in host:port format)
/// process_event is used for processing incoming message, which are marked as events via message msg_type.
/// process_rpc is used for processing incoming message, which are marked as rpc request via message msg_type.
/// startup is executed on the start of this function.
/// dependency is w/e clonable dependency needed when processing data.
/// The protocol message format is in sp-dto crate.
pub fn start_full_message<T: 'static, Q: 'static, R: 'static, D: 'static>(config: Value, process_event: ProcessEvent<T, Value, D>, process_rpc: ProcessRpc<Q, Value, D>, startup: Startup<R, D>, startup_data: Option<Value>, dependency: D, emittable_keys: Option<Vec<Key>>) 
where 
    T: Future<Output = Result<(), Box<dyn Error>>> + Send,
    Q: Future<Output = Result<Response<Value>, Box<dyn Error>>> + Send,
    R: Future<Output = ()> + Send,
    D: Clone + Send + Sync
{    
    let rt = Runtime::new().expect("Failed to create runtime");
    rt.block_on(full_message_mode(config, process_event, process_rpc, startup, startup_data, dependency, emittable_keys));
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
pub async fn stream_mode<T: 'static, R: 'static, D: 'static>(config: Value, process_stream: ProcessStream<T, D>, startup: Startup<R, D>, startup_data: Option<Value>, restream_tx: Option<UnboundedSender<RestreamMsg>>, restream_rx: Option<UnboundedReceiver<RestreamMsg>>, dependency: D)
where 
    T: Future<Output = ()> + Send,
    R: Future<Output = ()> + Send,
    D: Clone + Send + Sync
{
	let initial_config = config.clone();
    let target_config = match config["cfg_host"].as_str() {
        Some(cfg_host) => {
			let cfg_domain = config["cfg_domain"].as_str().expect("cfg_domain not passed");
            let cfg_token = config["cfg_token"].as_str().expect("cfg_token not passed");
            let (cfg_tx, mut cfg_rx) = mpsc::unbounded_channel();
			let (rpc_inbound_tx, mut rpc_inbound_rx) = mpsc::unbounded_channel();
			let (write_tx, write_rx) = mpsc::unbounded_channel();

			let rpc_completion_tx = rpc_inbound_tx.clone();
			let completion_tx = write_tx.clone();

            tokio::spawn(cfg_mode(cfg_host.to_owned(), cfg_domain.to_owned(), cfg_token.to_owned(), rpc_inbound_tx, rpc_inbound_rx, write_tx, write_rx, cfg_tx));

            let res = cfg_rx.recv().await.expect("Failed to get config");
			
			match rpc_completion_tx.send(RpcMsg::Complete) {
				Ok(()) => {}
				Err(_) => panic!("Failed to send RpcMsg::Complete")
			}

			match completion_tx.send(WriteMsg::Complete) {
				Ok(()) => {}
				Err(_) => panic!("Failed to send WriteMsg::Complete")
			}

			res
        }
        None => config
    };

    let host = target_config["host"].as_str().expect("Failed to get host from config").to_owned();
    let addr = target_config["addr"].as_str().expect("Failed to get addr from config").to_owned();
    let access_key = target_config["access_key"].as_str().expect("Failed to get access key from config").to_owned();

    let (read_tx, read_rx) = mpsc::unbounded_channel();
    let (write_tx, write_rx) = mpsc::unbounded_channel();
    let (rpc_inbound_tx, mut rpc_inbound_rx) = mpsc::unbounded_channel();
    let (rpc_outbound_tx, mut _rpc_outbound_rx) = mpsc::unbounded_channel();        

    tokio::spawn(async move {
        let mut rpcs = HashMap::new();        

        loop {
            let msg = rpc_inbound_rx.recv().await.expect("rpc inbound msg receive failed");

            match msg {
                RpcMsg::AddRpc(correlation_id, rpc_tx) => {
                    rpcs.insert(correlation_id, rpc_tx);

                    info!("stream_mode: add rpc ok, correlation_id {}", correlation_id);
                }                
                RpcMsg::RpcDataRequest(correlation_id) => {
                    match rpcs.remove(&correlation_id) {
                        Some(rpc_tx) => {
                            match rpc_outbound_tx.send(RpcMsg::RpcDataResponse(correlation_id, rpc_tx)) {
                                Ok(()) => {}
                                Err(_) => panic!("stream_mode: Rpc outbound tx send failed on rpc data request")
                            }
                        }
                        None => error!("stream_mode: not found rpc data for removal, correlation_id {}", correlation_id)
                    }
                }
				RpcMsg::Complete => break,
                _=> {}
            }
        }

		info!("Rpc loop completed");
    });

    let mb = MagicBall::new(addr.to_owned(), write_tx, rpc_inbound_tx);
    tokio::spawn(process_stream(target_config.clone(), mb.clone(), read_rx, restream_tx, restream_rx, dependency.clone()));
    tokio::spawn(startup(initial_config, target_config, mb, startup_data, dependency));
    connect_stream_future(CompleteCondition::Never, host, addr.to_owned(), access_key.to_owned(), read_tx, write_rx).await;
}

/// Future for message based client based on provided config.
/// "addr" value will be used as address for endpoint, "host" value - network addr for the server (in host:port format)
/// "access_key" value will be send for optional authorization, more information about this feature will be provided later.
/// process_stream is used for stream of incoming data processing.
/// startup is executed on the start of this function.
/// restream_rx can be used for restreaming data somewhere else, for example returning data for incoming web request
/// dependency is w/e clonable dependency needed when processing data.
/// The protocol message format is in sp-dto crate.
pub async fn full_message_mode<P: 'static, T: 'static, Q: 'static, R: 'static, D: 'static>(config: Value, process_event: ProcessEvent<T, P, D>, process_rpc: ProcessRpc<Q, P, D>, startup: Startup<R, D>, startup_data: Option<Value>, dependency: D, emittable_keys: Option<Vec<Key>>)
where 
    T: Future<Output = Result<(), Box<dyn Error>>> + Send,
    Q: Future<Output = Result<Response<P>, Box<dyn Error>>> + Send,
    R: Future<Output = ()> + Send,
    P: serde::Serialize, for<'de> P: serde::Deserialize<'de> + Send,
    D: Clone + Send + Sync
{
	let initial_config = config.clone();
    let target_config = match config["cfg_host"].as_str() {
        Some(cfg_host) => {
			let cfg_domain = config["cfg_domain"].as_str().expect("cfg_domain not passed");
            let cfg_token = config["cfg_token"].as_str().expect("cfg_token not passed");
            let (cfg_tx, mut cfg_rx) = mpsc::unbounded_channel();
			let (rpc_inbound_tx, mut rpc_inbound_rx) = mpsc::unbounded_channel();
			let (write_tx, write_rx) = mpsc::unbounded_channel();			

            let rpc_completion_tx = rpc_inbound_tx.clone();
			let completion_tx = write_tx.clone();

            tokio::spawn(cfg_mode(cfg_host.to_owned(), cfg_domain.to_owned(), cfg_token.to_owned(), rpc_inbound_tx, rpc_inbound_rx, write_tx, write_rx, cfg_tx));

            let res = cfg_rx.recv().await.expect("Failed to get config");
			
			match rpc_completion_tx.send(RpcMsg::Complete) {
				Ok(()) => {}
				Err(_) => panic!("Failed to send RpcMsg::Complete")
			}

			match completion_tx.send(WriteMsg::Complete) {
				Ok(()) => {}
				Err(_) => panic!("Failed to send WriteMsg::Complete")
			}

			res
        }
        None => config
    };

    let host = target_config["host"].as_str().expect("Failed to get host from config").to_owned();
    let addr = target_config["addr"].as_str().expect("Failed to get addr from config");
    let access_key = target_config["access_key"].as_str().expect("Failed to get access key from config");

    let (read_tx, mut read_rx) = mpsc::unbounded_channel();
    let (write_tx, write_rx) = mpsc::unbounded_channel();
    let (rpc_inbound_tx, mut rpc_inbound_rx) = mpsc::unbounded_channel();
    let (rpc_outbound_tx, mut rpc_outbound_rx) = mpsc::unbounded_channel();

    let addr = addr.to_owned();
    let addr2 = addr.to_owned();
    let addr3 = addr.to_owned();
    let access_key = access_key.to_owned();

    let rpc_inbound_tx2 = rpc_inbound_tx.clone();

    tokio::spawn(async move {
        let mut rpcs = HashMap::new();        

        loop {
            let msg = rpc_inbound_rx.recv().await.expect("Rpc inbound msg receive failed");

            match msg {
                RpcMsg::AddRpc(correlation_id, rpc_tx) => {
                    rpcs.insert(correlation_id, rpc_tx);

                    info!("full_message_mode: add rpc ok, correlation_id {}", correlation_id);
                }                
                RpcMsg::RpcDataRequest(correlation_id) => {
                    match rpcs.remove(&correlation_id) {
                        Some(rpc_tx) => {
                            match rpc_outbound_tx.send(RpcMsg::RpcDataResponse(correlation_id, rpc_tx)) {
                                Ok(()) => {}
                                Err(_) => panic!("full_message_mode: rpc outbound tx send failed on rpc data request")
                            }
                            //info!("send rpc response ok {}", correlation_id);
                        }
                        None => error!("full_message_mode: not found rpc data for removal, correlation_id {}", correlation_id)
                    }
                }
				RpcMsg::Complete => break,
                _=> {}
            }
        }

		info!("Rpc loop completed");
    });

    tokio::spawn(async move {
        let mb = MagicBall::new(addr2, write_tx, rpc_inbound_tx);        

        tokio::spawn(startup(initial_config, target_config.clone(), mb.clone(), startup_data, dependency.clone()));

        loop {                        
            let msg = match read_rx.recv().await {
                Some(msg) => msg,
                None => {
                    info!("Client connection dropped");
                    break;
                }
            };
            let mut mb = mb.clone();
            let config = target_config.clone();
            let dependency = dependency.clone();
			let (emittable_tx, emittable_rx) = watch::channel(Frame::new(0, 0, 0, 0, 0, 0, None));

            match msg {
                ClientMsg::Message(_, msg_meta, payload, attachments_data) => {
                    match msg_meta.msg_type {
                        MsgType::Event => {          

                            debug!("Client got event {}", msg_meta.display());

                            tokio::spawn(async move {
								let addr = mb.addr.clone();
                                let key = msg_meta.key.clone();
                                let payload: P = from_slice(&payload).expect("Failed to deserialize event payload");                                
								mb.emittable_rx = Some(emittable_rx);
                                if let Err(e) = process_event(config, mb, Message {meta: msg_meta, payload, attachments_data}, dependency).await {
                                    error!("Process event error {}, {:?}, {:?}", addr, key, e);
                                }
                                debug!("Client {} process_event succeeded", addr);
                            });                            
                        }
                        MsgType::RpcRequest => {                        
                            debug!("Client got rpc request {}", msg_meta.display());

                            tokio::spawn(async move {
                                let mut route = msg_meta.route.clone();
                                let correlation_id = msg_meta.correlation_id;
                                let key = msg_meta.key.clone();
                                let payload: P = from_slice(&payload).expect("failed to deserialize rpc request payload");
								let source_hash = get_addr_hash(&msg_meta.tx);

                                let (payload, attachments, attachments_data, rpc_result) = match process_rpc(config.clone(), {let mut res = mb.clone(); mb.emittable_rx = Some(emittable_rx); res}, Message {meta: msg_meta, payload, attachments_data}, dependency).await {
                                    Ok(res) => {
                                        debug!("Client {} process_rpc succeeded", mb.addr);
                                        let (res, attachments, attachments_data) = match res {
                                            Response::Simple(payload) => (payload, vec![], vec![]),
                                            Response::Full(payload, attachments, attachments_data) => (payload, attachments, attachments_data)
                                        };
                                        (to_vec(&res).expect("Failed to serialize rpc process result"), attachments, attachments_data, RpcResult::Ok)
                                    }
                                    Err(e) =>  {
                                        error!("Process rpc error {}, {:?}, {:?}", mb.addr, key, e);
                                        (to_vec(&json!({ "err": e.to_string() })).expect("failed to serialize rpc process error result"), vec![], vec![], RpcResult::Err)
                                    }
                                };                                

                                route.points.push(Participator::Service(mb.addr.clone()));

                                let key_hash = get_key_hash(&key);                                

                                let (res, msg_meta_size, payload_size, attachments_sizes) = rpc_response_dto2_sizes(mb.addr.clone(),  key, correlation_id, payload, attachments, attachments_data, rpc_result, route, None, None).expect("failed to create rpc reply");

                                debug!("Client {} attempt to write rpc response", mb.addr);

                                let stream_id = mb.get_stream_id();

                                mb.write_full_message(MsgType::RpcResponse(RpcResult::Ok).get_u8(), key_hash, stream_id, source_hash, res, msg_meta_size, payload_size, attachments_sizes, true).await.expect("Failed to write rpc response");
                             
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
                ClientMsg::Frame(frame) => {
                    match emittable_tx.send(frame) {
						Ok(()) => {
							debug!("Emittable frame send succeeded");
						}
						Err(_) => {
							error!("Error while sending emittable frame");
						}
					}
                }
            }
        }
    });

    connect_full_message_future(&host, addr3, access_key, read_tx, write_rx, emittable_keys).await;
}

async fn auth(addr: String, access_key: String, tcp_stream: &mut TcpStream) -> Result<(), ProcessError> {
    let route = Route {
        source: Participator::Service(addr.clone()),
        spec: RouteSpec::Simple,
        points: vec![Participator::Service(addr.clone())]
    };  

    let (correlation_id, dto, msg_meta_size, payload_size, attachments_size) = rpc_dto_with_sizes(addr.clone(), Key::simple("Auth"), json!({
        "access_key": access_key
    }), route, None, None).expect("Failed to create auth dto");

    write_to_tcp_stream(tcp_stream, 0, 0, get_stream_id_onetime(&addr), get_addr_hash(&addr), dto, msg_meta_size, payload_size, attachments_size, true).await
}


async fn connect_stream_future(complete_condition: CompleteCondition, host: String, addr: String, access_key: String, read_tx: UnboundedSender<ClientMsg>, write_rx: UnboundedReceiver<WriteMsg>) {
    let mut write_stream = TcpStream::connect(host.clone()).await.expect("Connection to host failed");
    auth(addr.clone(), access_key.clone(), &mut write_stream).await.expect("Write stream authorization failed");

    let mut read_stream = TcpStream::connect(host.clone()).await.expect("Connection to host failed");
    auth(addr.clone(), access_key, &mut read_stream).await.expect("Read stream authorization failed");

    info!("Connected in stream mode to {} as {}", host, addr);

    let res = process_stream_mode(complete_condition, write_stream, read_stream, read_tx, write_rx).await;

    info!("Connections closed, {:?}", res);
}

async fn connect_full_message_future(host: &str, addr: String, access_key: String, read_tx: UnboundedSender<ClientMsg>, write_rx: UnboundedReceiver<WriteMsg>, emittable_keys: Option<Vec<Key>>) {    
    let mut write_stream = TcpStream::connect(host).await.expect("Connection to host failed");
    auth(addr.clone(), access_key.clone(), &mut write_stream).await.expect("Write stream authorization failed");

    let mut read_stream = TcpStream::connect(host).await.expect("Connection to host failed");
    auth(addr.clone(), access_key, &mut read_stream).await.expect("Read stream authorization failed");

    info!("Connected in full message mode to {} as {}", host, addr);

    let res = process_full_message_mode(write_stream, read_stream, read_tx, write_rx, emittable_keys).await;

    info!("{:?}", res);
}

async fn process_stream_mode(complete_condition: CompleteCondition, mut write_tcp_stream: TcpStream, mut read_tcp_stream: TcpStream, read_tx: UnboundedSender<ClientMsg>, write_rx: UnboundedReceiver<WriteMsg>) -> Result<(), ProcessError> {
    //let (auth_msg_meta, auth_payload, auth_attachments) = read_full(&mut socket_read).await?;
    //let auth_payload: Value = from_slice(&auth_payload)?;    

    //info!("auth {:?}", auth_msg_meta);
    //info!("auth {:?}", auth_payload);

    tokio::spawn(async move {
        match write_loop(write_rx, &mut write_tcp_stream).await {
			Ok(()) => info!("Write loop ended"),
			Err(e) => error!("Write loop ended with error, {:?}", e)
		}        
    });	

    let mut state = State::new();

	match complete_condition {
		CompleteCondition::Never => {
			loop {
				match state.read_frame() {
					ReadFrameResult::NotEnoughBytesForFrame => {
						state.read_from_tcp_stream(&mut read_tcp_stream).await?;
					}
					ReadFrameResult::NextStep => {}
					ReadFrameResult::Frame(frame) => {
						debug!("Stream frame read, frame type {}, msg type {}, stream id {}", frame.frame_type, frame.msg_type, frame.stream_id);
		
						let frame_type = frame.frame_type;
		
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
		CompleteCondition::OnStreamEnd => {
			loop {
				match state.read_frame() {
					ReadFrameResult::NotEnoughBytesForFrame => {
						state.read_from_tcp_stream(&mut read_tcp_stream).await?;
					}
					ReadFrameResult::NextStep => {}
					ReadFrameResult::Frame(frame) => {
						debug!("Stream frame read, frame type {}, msg type {}, stream id {}", frame.frame_type, frame.msg_type, frame.stream_id);
		
						let frame_type = frame.frame_type;
		
						match read_tx.send(ClientMsg::Frame(frame)) {
							Ok(()) => {}
							Err(_) => {                        
								panic!("Client message send with read_tx in stream mode failed");
							}
						}
		
						match frame_type {
							6 => break,
							_ => {}
						}
					}
				}
			}

			info!("Read loop completed");
		}
	}

	Ok(())
}

async fn process_full_message_mode(mut write_tcp_stream: TcpStream, mut read_tcp_stream: TcpStream, read_tx: UnboundedSender<ClientMsg>, write_rx: UnboundedReceiver<WriteMsg>, emittable_keys: Option<Vec<Key>>) -> Result<(), ProcessError> {    
    //let (auth_msg_meta, auth_payload, auth_attachments) = read_full(&mut socket_read).await?;
    //let auth_payload: Value = from_slice(&auth_payload)?;    

    //info!("auth {:?}", auth_msg_meta);
    //info!("auth {:?}", auth_payload);
            
    let mut stream_layouts: HashMap<u64, StreamLayout> = HashMap::new();
    let mut state = State::new();

    tokio::spawn(async move {
        let res = write_loop(write_rx, &mut write_tcp_stream).await;
        error!("{:?}", res);
    });

    let emittable_key_hashes = emittable_keys.map(|a| {
        let mut res = vec![];

        for key in a {
            res.push(get_key_hash(&key));
        }

        res
    });

	loop {
		match state.read_frame() {
			ReadFrameResult::NotEnoughBytesForFrame => {
				state.read_from_tcp_stream(&mut read_tcp_stream).await?;
			}
			ReadFrameResult::NextStep => {}
			ReadFrameResult::Frame(frame) => {
				debug!("Full message stream frame read, frame type {}, msg type {}, stream id {}", frame.frame_type, frame.msg_type, frame.stream_id);

				match frame.get_frame_type() {
					Ok(frame_type) => {

                        match &emittable_key_hashes {
                            Some(emittable_key_hashes) => {
                                match emittable_key_hashes.contains(&frame.key_hash) {
                                    true => {
                                        match read_tx.send(ClientMsg::Frame(frame)) {
                                            Ok(()) => {}
                                            Err(_) => {
                                                panic!("Client message send with read_tx in full message mode failed")
                                            }
                                        }
                                    }
                                    false => {
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
                        
                                                match read_tx.send(ClientMsg::Message(frame.stream_id, msg_meta, stream_layout.payload, match stream_layout.attachments_data.is_empty() {
                                                    true => Some(stream_layout.attachments_data),
                                                    false => None
                                                })) {
                                                    Ok(()) => {}
                                                    Err(_) => {
                                                        panic!("Client message send with read_tx in full message mode failed")
                                                    }
                                                }
                                            }
											FrameType::Skip => {}
                                        }
                                    }
                                }
                            }
                            None => {
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
                
                                        match read_tx.send(ClientMsg::Message(frame.stream_id, msg_meta, stream_layout.payload, match stream_layout.attachments_data.is_empty() {
                                            true => Some(stream_layout.attachments_data),
                                            false => None
                                        })) {
                                            Ok(()) => {}
                                            Err(_) => {
                                                panic!("Client message send with read_tx in full message mode failed")
                                            }
                                        }
                                    }
									FrameType::Skip => {}
                                }
                            }
                        }
						
					}
					Err(e) => {
						error!("Get frame type failed in read: {:?}", e);
					}
				}				
			}
		}
	}  
}

struct CfgStreamLayout {
    layout: StreamLayout,
    msg_meta: Option<MsgMeta>,
    payload: Option<Value>
}

pub async fn cfg_mode(cfg_host: String, cfg_domain: String, cfg_token: String, rpc_inbound_tx: UnboundedSender<RpcMsg>, mut rpc_inbound_rx: UnboundedReceiver<RpcMsg>, write_tx: UnboundedSender<WriteMsg>, write_rx: UnboundedReceiver<WriteMsg>, result_tx: UnboundedSender<Value>) {
    let (read_tx, read_rx) = mpsc::unbounded_channel();        
    let (rpc_outbound_tx, mut _rpc_outbound_rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        let mut rpcs = HashMap::new();        

        loop {
            let msg = rpc_inbound_rx.recv().await.expect("Rpc inbound msg receive failed");

            match msg {
                RpcMsg::AddRpc(correlation_id, rpc_tx) => {
                    rpcs.insert(correlation_id, rpc_tx);

                    info!("cfg_mode: add rpc ok, correlation_id {}", correlation_id);
                }                
                RpcMsg::RpcDataRequest(correlation_id) => {
                    match rpcs.remove(&correlation_id) {
                        Some(rpc_tx) => {
                            match rpc_outbound_tx.send(RpcMsg::RpcDataResponse(correlation_id, rpc_tx)) {
                                Ok(()) => {}
                                Err(_) => panic!("cfg_mode: Rpc outbound tx send failed on rpc data request")
                            }
                        }
                        None => error!("cfg_mode: not found rpc data for removal, correlation_id {}", correlation_id)
                    }
                }
				RpcMsg::Complete => break,
                _=> {}
            }
        }

		info!("Rpc loop completed");
    });

    let addr = uuid::Uuid::new_v4().to_string();
    let access_key = "";

    let mut mb = MagicBall::new(addr.clone(), write_tx, rpc_inbound_tx);

    tokio::spawn(process_cfg_stream(mb.clone(), read_rx, result_tx));

    match mb.send_rpc(Key::new("Get", "Cfg", "Cfg"), json!({
		"domain": cfg_domain,
        "cfg_token": cfg_token
    })).await {
        Ok(correlation_id) => info!("Sent cfg rpc, correlation id {}", correlation_id),
        Err(e) => panic!("Failed to send cfg rpc, {:?}", e)
    }

    connect_stream_future(CompleteCondition::OnStreamEnd, cfg_host.to_owned(), addr, access_key.to_owned(), read_tx, write_rx).await;
}

pub async fn process_cfg_stream(mut mb: MagicBall, mut rx: UnboundedReceiver<ClientMsg>, mut result_tx: UnboundedSender<Value>) {
    let mut stream_layouts = HashMap::new();

    loop {        
        let client_msg = rx.recv().await.expect("Connection issues acquired");
        let stream_id = client_msg.get_stream_id();
        match process_client_msg(&mut mb, &mut stream_layouts, client_msg, &mut result_tx).await {
            Ok(completed) => {
                if completed {
                    break;
                }
            }
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

async fn process_client_msg(mb: &mut MagicBall, stream_layouts: &mut HashMap<u64, CfgStreamLayout>, client_msg: ClientMsg, result_tx: &mut UnboundedSender<Value>) -> Result<bool, ProcessError> {
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
									stream_layouts.insert(frame.stream_id, CfgStreamLayout {
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
										payload: None
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
									let mut stream_layout = CfgStreamLayout {
										layout: StreamLayout {
											id: frame.stream_id,
											msg_meta: vec![],
											payload: vec![],
											attachments_data: vec![]
										},
										msg_meta: None,
										payload: None
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
										"Get" => {							
											let payload: Value = from_slice(&stream_layout.layout.payload)?;                                            
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
						}						
						FrameType::AttachmentEnd => {
                            info!("Attachment end frame");
						}
						FrameType::End => {
                            info!("Stream end frame");	
							match stream_layouts.remove(&frame.stream_id) {
								Some(mut stream_layout) => {
                                    match stream_layout.msg_meta {
                                        Some(msg_meta) => {
                                            match msg_meta.key.action.as_ref() {
                                                "Get" => {
                                                    let mut payload = stream_layout.payload.ok_or(ProcessError::None)?;
                                                    result_tx.send(payload).expect("Failed to send config");

                                                    return Ok(true);    
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
						FrameType::Skip => {}
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

    Ok(false)
}