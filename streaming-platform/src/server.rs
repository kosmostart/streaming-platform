use std::collections::HashMap;
use std::net::SocketAddr;
use log::*;
use siphasher::sip::SipHasher24;
use serde_derive::Deserialize;
use serde_json::{from_slice, Value, from_value, to_vec, json};
use tokio::runtime::Runtime;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, UnboundedSender};
use sp_dto::{Key, SubscribeByAddr, SubscribeByKey, subscribes_map_to_keys, subscribes_vec_to_map, subscribes_vec_to_keys, MsgMeta, MsgType, RpcResult, rpc_response_dto2_sizes};
use crate::proto::*;

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub host: String
}

#[derive(Debug, Deserialize, Clone)]
pub struct Dir {
    pub access_key: String,
    pub path: String
}

fn to_hashed_subscribes(_key_hasher: &mut SipHasher24, subscribes: Vec<SubscribeByKey>) -> HashMap<u64, Vec<u64>> {
    let mut res = HashMap::new();    

    for subsribe in subscribes {
        res.insert(get_key_hash(&subsribe.key), subsribe.addrs.iter().map(|a| get_addr_hash(a)).collect());
    }

    res
}

/// Starts the server based on provided ServerConfig struct. Creates new runtime and blocks.
pub fn start(config: ServerConfig, event_subscribes: Vec<SubscribeByAddr>, rpc_subscribes: Vec<SubscribeByAddr>) {
    let rt = Runtime::new().expect("failed to create runtime"); 
    let _ = rt.block_on(start_future(config, event_subscribes, rpc_subscribes));
}

/// Future for new server start based on provided ServerConfig struct, in case you want to create runtime by yourself.
pub async fn start_future(config: ServerConfig, event_subscribes: Vec<SubscribeByAddr>, rpc_subscribes: Vec<SubscribeByAddr>) -> Result<(), ProcessError> {
    let listener = TcpListener::bind(config.host.clone()).await?;
    let (server_tx, mut server_rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {        
        let mut clients = HashMap::new();

        loop {
            let msg = server_rx.recv().await.expect("ServerMsg receive failed");
            match msg {
                ServerMsg::AddClient(addr, net_addr, tx) => {
                    let client = Client {
                        addr,
                        net_addr,
                        tx
                    };
                    clients.insert(get_addr_hash(&client.addr), client);
                }
                ServerMsg::Send(addr_hash, frame) => {
                    match clients.get_mut(&addr_hash) {
                        Some(client) => {
                            match client.tx.send(WriteMsg::Frame(frame)) {
                                Ok(()) => {}                             
                                Err(_msg) => panic!("ServerMsg::Send processing failed - send error, client addr hash {}", addr_hash)
                            }
                        }
                        None => error!("No client with addr hash {} for sending frame, stream id {}, key hash {}", addr_hash, frame.stream_id, frame.key_hash)
                    }
                }                
                ServerMsg::RemoveClient(addr_hash) => {
                    let _ = clients.remove(&addr_hash);
                }
            }     
        }
    });    

    let mut client_states = HashMap::new();
    
    info!("Started on {}", config.host);

    loop {                
        let (mut stream, client_net_addr) = listener.accept().await?;

        info!("New connection from {}", client_net_addr);

        let config = config.clone();
        let server_tx = server_tx.clone();
        let mut state = State::new();

        match auth_tcp_stream(&mut stream, &mut state, client_net_addr, &config).await {
            Ok(addr) => {
                info!("Stream from {} authorized as {}", client_net_addr, addr);
                
                if !client_states.contains_key(&addr) {
                    client_states.insert(addr.clone(), ClientState::new());
                }

                match client_states.get_mut(&addr) {
                    Some(client_state) => {
                        
                        if !client_state.has_writer {
                            client_state.has_writer = true;

                            let mut event_map = HashMap::new();
                            let mut rpc_map = HashMap::new();

                            for sub in &event_subscribes {
                                event_map.insert(sub.addr.clone(), sub.keys.clone());
                            }

                            for sub in &rpc_subscribes {
                                rpc_map.insert(sub.addr.clone(), sub.keys.clone());
                            }

                            tokio::spawn(async move {                                
                                match process_write_tcp_stream(addr.clone(), &mut stream, &mut state, event_map, rpc_map, client_net_addr, server_tx).await {
									Ok(()) => info!("Write process ended, client addr {}", addr),
									Err(e) => {
										match e {
											ProcessError::StreamClosed => info!("Write process ended: stream closed, client addr {}", addr),
											_ => error!("Write process ended with error, client addr {}, {:?}", addr, e)
										}
									}
								}                        
                            });

                        } else {
                            client_state.has_writer = false;

                            tokio::spawn(async move {            
                                let res = process_read_tcp_stream(addr.clone(), stream, client_net_addr, server_tx).await;
                                error!("{} read process ended, {:?}", addr, res);
                            });
                        }
                        
                    }
                    None => error!("failed to get client state for {} stream from {}", addr, client_net_addr)
                }                
            }
            Err(e) => error!("failed to authorize stream from {}, {:?}", client_net_addr, e)
        }        
    }
}

struct ClientState {
    has_writer: bool    
}

impl ClientState {
    pub fn new() -> ClientState {
        ClientState {
            has_writer: false            
        }
    }
}

async fn auth_tcp_stream(tcp_stream: &mut TcpStream, state: &mut State, client_net_addr: SocketAddr, _config: &ServerConfig) -> Result<String, ProcessError> {    
    let mut stream_layout = StreamLayout {
        id: 0,
        msg_meta: vec![],
        payload: vec![],
        attachments_data: vec![]
    };

	loop {		
		match state.read_frame() {
			ReadFrameResult::NotEnoughBytesForFrame => {
				state.read_from_tcp_stream(tcp_stream).await?
			}
			ReadFrameResult::NextStep => {}
			ReadFrameResult::Frame(frame) => {
				debug!("Auth stream frame read, frame type {}, msg type {}, stream id {}", frame.frame_type, frame.msg_type, frame.stream_id);

				match frame.get_frame_type() {
					Ok(frame_type) => {
						match frame_type {
							FrameType::MsgMeta | FrameType::MsgMetaEnd => {
                                match frame.payload {
                                    Some(payload) => {
                                        stream_layout.msg_meta.extend_from_slice(&payload[..frame.payload_size as usize]);
                                    }
                                    None => {}
                                }								
							}
							FrameType::Payload | FrameType::PayloadEnd => {
                                match frame.payload {
                                    Some(payload) => {
                                        stream_layout.payload.extend_from_slice(&payload[..frame.payload_size as usize]);
                                    }
                                    None => {}
                                }								
							}
							FrameType::Attachment | FrameType::AttachmentEnd => {
                                match frame.payload {
                                    Some(payload) => {
                                        stream_layout.attachments_data.extend_from_slice(&payload[..frame.payload_size as usize]);
                                    }
                                    None => {}
                                }								
							}
							FrameType::End => {								
								break;
							}							
						}

					}
					Err(e) => {
						error!("Error on auth stream read for {:?}, get frame type failed, {:?}", client_net_addr, e);
					}
				}
			}
		}
	}
       
    let msg_meta: MsgMeta = from_slice(&stream_layout.msg_meta)?;

    Ok(msg_meta.tx)
}


async fn process_read_tcp_stream(addr: String, mut tcp_stream: TcpStream, client_net_addr: SocketAddr, server_tx: UnboundedSender<ServerMsg>) -> Result<(), ProcessError> {
    let (client_tx, client_rx) = mpsc::unbounded_channel();

    server_tx.send(ServerMsg::AddClient(addr, client_net_addr, client_tx))?;    

    write_loop(client_rx, &mut tcp_stream).await
}

async fn process_write_tcp_stream(_addr: String, tcp_stream: &mut TcpStream, state: &mut State, mut initial_event_subscribes: HashMap<String, Vec<Key>>, mut initial_rpc_subscribes: HashMap<String, Vec<Key>>, _client_net_addr: SocketAddr, server_tx: UnboundedSender<ServerMsg>) -> Result<(), ProcessError> {
    let event_subscribes = subscribes_map_to_keys(initial_event_subscribes.clone());
    let rpc_subscribes = subscribes_map_to_keys(initial_rpc_subscribes.clone());

    let mut key_hasher = get_key_hasher();

    let mut event_subscribes = to_hashed_subscribes(&mut key_hasher, event_subscribes);
    let mut rpc_subscribes = to_hashed_subscribes(&mut key_hasher, rpc_subscribes);

    let mut stream_layouts: HashMap<u64, StreamLayout> = HashMap::new();

	loop {
		match state.read_frame() {
			ReadFrameResult::NotEnoughBytesForFrame => {
				state.read_from_tcp_stream(tcp_stream).await?;
			}
			ReadFrameResult::NextStep => {}
			ReadFrameResult::Frame(frame) => {
				debug!("Main stream frame read, frame type {}, msg type {}, stream id {}", frame.frame_type, frame.msg_type, frame.stream_id);

				match frame.get_msg_type()? {
					MsgType::Event => {
                        match event_subscribes.get(&frame.key_hash) {
                            Some(targets) => {
                                match targets.len() {
                                    0 => {
                                        warn!("Subscribes empty for key hash {}, msg_type {:?}", frame.key_hash, frame.get_msg_type())
                                    }
                                    1 => {
                                        let target = targets[0].clone();
        
                                        debug!("Sending frame to {}", target);
                                        server_tx.send(ServerMsg::Send(target, frame))?;
                                    }
                                    _ => {
                                        let index = targets.len() - 1;
        
                                        for target in targets.iter().take(index) {     
                                            debug!("Sending frame to {}", target);
                                            server_tx.send(ServerMsg::Send(target.clone(), frame.clone()))?;
                                        }
        
                                        let target = &targets[index];
        
                                        debug!("Sending frame to {}", target);
                                        server_tx.send(ServerMsg::Send(target.clone(), frame))?;
                                    }
                                }
                            }
                            None => warn!("No subscribes found for key hash {}, msg_type {:?}", frame.key_hash, frame.get_msg_type())
                        }
                    }
					MsgType::RpcRequest => {
                        match rpc_subscribes.get(&frame.key_hash) {
                            Some(targets) => {
                                match targets.len() {
                                    0 => {
                                        warn!("Subscribes empty for key hash {}, msg_type {:?}", frame.key_hash, frame.get_msg_type())
                                    }
                                    1 => {
                                        let target = targets[0].clone();
        
                                        debug!("Sending frame to {}", target);
                                        server_tx.send(ServerMsg::Send(target, frame))?;
                                    }
                                    _ => {
                                        let index = targets.len() - 1;
        
                                        for target in targets.iter().take(index) {     
                                            debug!("Sending frame to {}", target);
                                            server_tx.send(ServerMsg::Send(target.clone(), frame.clone()))?;
                                        }
        
                                        let target = &targets[index];
        
                                        debug!("Sending frame to {}", target);
                                        server_tx.send(ServerMsg::Send(target.clone(), frame))?;
                                    }
                                }
                            }
                            None => warn!("No subscribes found for key hash {}, msg_type {:?}", frame.key_hash, frame.get_msg_type())
                        }
                    }
					MsgType::RpcResponse(_) => {
                        debug!("Sending frame to source, addr hash {}", frame.source_hash);
                        server_tx.send(ServerMsg::Send(frame.source_hash, frame))?;
                    }
                    MsgType::ServerRpcRequest => {
                        match frame.get_frame_type()? {
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

                                let msg_meta: MsgMeta = from_slice(&stream_layout.msg_meta)?;

                                let res = match msg_meta.key.action.as_ref() {
                                    "GetSubscribes" => {
                                        json!({
                                            "event_subscribes": initial_event_subscribes,
                                            "rpc_subscribes": initial_rpc_subscribes
                                        })
                                    }
                                    "LoadSubscribes" => {
                                        let mut payload: Value = from_slice(&stream_layout.payload)?;

                                        let new_event_subscribes: Vec<SubscribeByAddr> = from_value(payload["event_subscribes"].take())?;
                                        let new_rpc_subscribes: Vec<SubscribeByAddr> = from_value(payload["rpc_subscribes"].take())?;

                                        initial_event_subscribes = subscribes_vec_to_map(new_event_subscribes.clone());
                                        initial_rpc_subscribes = subscribes_vec_to_map(new_rpc_subscribes.clone());

                                        let new_event_subscribes = subscribes_vec_to_keys(new_event_subscribes);
                                        let new_rpc_subscribes = subscribes_vec_to_keys(new_rpc_subscribes);
                                                                    
                                        let mut key_hasher = get_key_hasher();
                                    
                                        let new_event_subscribes = to_hashed_subscribes(&mut key_hasher, new_event_subscribes);
                                        let new_rpc_subscribes = to_hashed_subscribes(&mut key_hasher, new_rpc_subscribes);
                                    
                                        event_subscribes.clear();

                                        for (key, addrs) in new_event_subscribes {
                                            event_subscribes.insert(key, addrs);
                                        }

                                        rpc_subscribes.clear();

                                        for (key, addrs) in new_rpc_subscribes {
                                            rpc_subscribes.insert(key, addrs);
                                        }

                                        info!("Subscribes loaded");

                                        json!({})
                                    }
                                    "AddEventSubscribe" => {
                                        let mut payload: Value = from_slice(&stream_layout.payload)?;
                                        let subscribe: SubscribeByAddr = from_value(payload["subscribe"].take())?;

                                        match initial_event_subscribes.contains_key(&subscribe.addr) {
                                            true => {
                                                warn!("Subscribe {:?} already present in event subscribes", subscribe);

                                                json!({
                                                    "result": "AlreadyPresent"
                                                })
                                            }
                                            false => {
                                                info!("Adding subscribe {:?} to event subscribes", subscribe);

                                                initial_event_subscribes.insert(subscribe.addr.clone(), subscribe.keys.clone());

                                                let new = subscribes_vec_to_keys(vec![subscribe]);

                                                let mut key_hasher = get_key_hasher();
                                    
                                                let new = to_hashed_subscribes(&mut key_hasher, new);

                                                for (key, addrs) in new {
                                                    info!("Inserting key {} with addrs {:?} to event subscribes", key, addrs);
                                                    event_subscribes.insert(key, addrs);
                                                    info!("Done");
                                                }
                                                
                                                info!("Subscribe added to event subscribes");

                                                json!({
                                                    "result": "Added"
                                                })
                                            }
                                        }
                                    }
                                    "RemoveEventSubscribe" => {
                                        let mut payload: Value = from_slice(&stream_layout.payload)?;

                                        let subscribe: SubscribeByAddr = from_value(payload["subscribe"].take())?;

                                        match initial_event_subscribes.remove(&subscribe.addr) {
                                            Some(removed_keys) => {
                                                info!("Removing subscribe {} {:?} from event subscribes", &subscribe.addr, removed_keys);

                                                for key in removed_keys {
                                                    let key_hash = get_key_hash(&key);                                                    
                                                    event_subscribes.remove(&key_hash);
                                                    info!("Removed key {:?}, key hash {} with addr {} from event subscribes", key, key_hash, subscribe.addr);
                                                }                                            

                                                json!({
                                                    "result": "Removed"
                                                })                                                
                                            }
                                            None => {
                                                warn!("Subscribe {:?} not found in event subscribes", subscribe);

                                                json!({
                                                    "result": "NotFound"
                                                })                                                
                                            }
                                        }
                                    }
                                    "AddRpcSubscribe" => {
                                        let mut payload: Value = from_slice(&stream_layout.payload)?;
                                        let subscribe: SubscribeByAddr = from_value(payload["subscribe"].take())?;

                                        match initial_rpc_subscribes.contains_key(&subscribe.addr) {
                                            true => {
                                                warn!("Subscribe {:?} already present in rpc subscribes", subscribe);

                                                json!({
                                                    "result": "AlreadyPresent"
                                                })
                                            }
                                            false => {
                                                info!("Adding subscribe {:?} to rpc subscribes", subscribe);

                                                initial_rpc_subscribes.insert(subscribe.addr.clone(), subscribe.keys.clone());

                                                let new = subscribes_vec_to_keys(vec![subscribe]);

                                                let mut key_hasher = get_key_hasher();
                                    
                                                let new = to_hashed_subscribes(&mut key_hasher, new);

                                                for (key, addrs) in new {
                                                    info!("Inserting key {} with addrs {:?} to event subscribes", key, addrs);
                                                    rpc_subscribes.insert(key, addrs);
                                                    info!("Done");
                                                }
                                                
                                                info!("Subscribe added to rpc subscribes");

                                                json!({
                                                    "result": "Added"
                                                })
                                            }
                                        }
                                    }
                                    "RemoveRpcSubscribe" => {
                                        let mut payload: Value = from_slice(&stream_layout.payload)?;

                                        let subscribe: SubscribeByAddr = from_value(payload["subscribe"].take())?;

                                        match initial_rpc_subscribes.remove(&subscribe.addr) {
                                            Some(removed_keys) => {
                                                info!("Removing subscribe {} {:?} from rpc subscribes", &subscribe.addr, removed_keys);

                                                for key in removed_keys {
                                                    let key_hash = get_key_hash(&key);                                                    
                                                    rpc_subscribes.remove(&key_hash);
                                                    info!("Removed key {:?}, key hash {} with addr {} from rpc subscribes", key, key_hash, subscribe.addr);
                                                }                                            

                                                json!({
                                                    "result": "Removed"
                                                })                                                
                                            }
                                            None => {
                                                warn!("Subscribe {:?} not found in rpc subscribes", subscribe);

                                                json!({
                                                    "result": "NotFound"
                                                })                                                
                                            }
                                        }
                                    }
                                    "Hello" => json!({ "data": "hello" }),
                                    _ => {
                                        warn!("Incorrect server message key");

                                        json!({})
                                    }
                                };

                                let (data, msg_meta_size, payload_size, attachments_sizes) = rpc_response_dto2_sizes("Server".to_owned(), msg_meta.key, msg_meta.correlation_id, to_vec(&res)?, vec![], vec![], RpcResult::Ok, msg_meta.route, None, None).expect("failed to create rpc reply");

                                let stream_id = get_stream_id_onetime("Server");

                                write_full_message_server(&server_tx, frame.source_hash, MsgType::ServerRpcResponse(RpcResult::Ok).get_u8(), frame.key_hash, stream_id, frame.source_stream_id, frame.source_hash, data, msg_meta_size, payload_size, attachments_sizes, true).await?;
                            }
                        }
                    }
                    MsgType::ServerRpcResponse(_) => {
                        warn!("ServerRpcResponse frame received on server");
                    }
				}
			}
		}
	}
}
