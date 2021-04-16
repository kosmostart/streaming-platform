use std::collections::HashMap;
use std::net::SocketAddr;
use std::hash::Hasher;
use log::*;
use siphasher::sip::SipHasher24;
use serde_json::from_slice;
use tokio::runtime::Runtime;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, UnboundedSender};
use sp_dto::bytes::{BytesMut, BufMut};
use sp_dto::{Key, MsgMeta, MsgType, Subscribes};
use sp_cfg::ServerConfig;
use crate::proto::*;

fn to_hashed_subscribes(key_hasher: &mut SipHasher24, subscribes: HashMap<Key, Vec<String>>) -> HashMap<u64, Vec<u64>> {
    let mut res = HashMap::new();
    let mut buf = BytesMut::new();

    for (key, addrs) in subscribes {
        res.insert(get_key_hash(key), addrs.iter().map(|a| get_addr_hash(a)).collect());
    }

    res
}

/// Starts the server based on provided ServerConfig struct. Creates new runtime and blocks.
pub fn start(config: ServerConfig, subscribes: Subscribes) {
    let rt = Runtime::new().expect("failed to create runtime"); 
    let _ = rt.block_on(start_future(config, subscribes));
}

/// Future for new server start based on provided ServerConfig struct, in case you want to create runtime by yourself.
pub async fn start_future(config: ServerConfig, subscribes: Subscribes) -> Result<(), ProcessError> {
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
                                Err(msg) => panic!("ServerMsg::Send processing failed - send error, client addr hash {}", addr_hash)
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

    let (event_subscribes, rpc_subscribes) = match subscribes {
        Subscribes::ByAddr(_, _) => subscribes.traverse_to_keys(),
        Subscribes::ByKey(event_subscribes, rpc_subscribes) => (event_subscribes, rpc_subscribes)
    };

    let mut client_states = HashMap::new();
    let mut key_hasher = get_key_hasher();

    let event_subscribes = to_hashed_subscribes(&mut key_hasher, event_subscribes);
    let rpc_subscribes = to_hashed_subscribes(&mut key_hasher, rpc_subscribes);

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

                            let event_subscribes = event_subscribes.clone();
                            let rpc_subscribes = rpc_subscribes.clone();

                            tokio::spawn(async move {                                
                                let res = process_write_tcp_stream(&mut stream, &mut state, addr.clone(), event_subscribes, rpc_subscribes, client_net_addr, server_tx).await;
                                error!("Write process ended for {}, {:?}", addr, res);
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

async fn process_write_tcp_stream(tcp_stream: &mut TcpStream, state: &mut State, addr: String, event_subscribes: HashMap<u64, Vec<u64>>, rpc_subscribes: HashMap<u64, Vec<u64>>, _client_net_addr: SocketAddr, server_tx: UnboundedSender<ServerMsg>) -> Result<(), ProcessError> {
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
				}
			}
		}
	}
}