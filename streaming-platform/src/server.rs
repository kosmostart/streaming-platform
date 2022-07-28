use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::IndexMut;
use log::*;
use siphasher::sip::SipHasher24;
use serde_derive::Deserialize;
use serde_json::{from_slice, Value, from_value, to_vec, json};
use tokio::runtime::Runtime;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, UnboundedSender, UnboundedReceiver};
use sp_dto::{Key, Subscribe, MsgMeta, MsgType, RpcResult, rpc_response_dto2_sizes};
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

fn to_hashed_subscribes(subscribes: Vec<Subscribe>) -> HashMap<u64, Vec<u64>> {
    let mut res: HashMap<u64, Vec<u64>> = HashMap::new();

    for subscribe in subscribes {
        let key_hash = get_key_hash(&subscribe.key);
        let addr_hash = get_addr_hash(&subscribe.addr);

        match res.get_mut(&key_hash) {
            Some(addrs) => {
                if !addrs.iter().any(|a_h| *a_h == addr_hash) {
                    addrs.push(addr_hash);
                }
            }
            None => {
                res.insert(key_hash, vec![addr_hash]);
            }
        }        
    }

    res
}

fn to_hashed_subscribe(subscribe: &Subscribe) -> (u64, u64) {    
    let addr_hash = get_addr_hash(&subscribe.addr);
    let key_hash = get_key_hash(&subscribe.key);    

    (addr_hash, key_hash)
}

/// Starts the server based on provided ServerConfig struct. Creates new runtime and blocks.
pub fn start(config: ServerConfig, event_subscribes: Vec<Subscribe>, rpc_subscribes: Vec<Subscribe>) {
    let rt = Runtime::new().expect("failed to create runtime"); 
    let _ = rt.block_on(start_future(config, event_subscribes, rpc_subscribes));
}

/// Future for new server start based on provided ServerConfig struct, in case you want to create runtime by yourself.
pub async fn start_future(config: ServerConfig, mut event_subscribes: Vec<Subscribe>, mut rpc_subscribes: Vec<Subscribe>) -> Result<(), ProcessError> {
    let listener = TcpListener::bind(config.host.clone()).await?;
    let (server_tx, mut server_rx) = mpsc::unbounded_channel();
    let (settings_tx, mut settings_rx) = mpsc::unbounded_channel();

    // Server loop
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

    let mut hashed_event_subscribes = to_hashed_subscribes(event_subscribes.clone());
    let mut hashed_rpc_subscribes = to_hashed_subscribes(rpc_subscribes.clone());
    
    let server_tx_for_settings_loop = server_tx.clone();

    // Settings loop
    tokio::spawn(async move {
        let mut client_settings = HashMap::new();
        
        loop {
            let msg = settings_rx.recv().await.expect("SettingsMsg receive failed");
            
            match msg {
                SettingsMsg::AddClient(addr, addr_hash, tx) => {
                    match tx.send(SettingsMsg2::Subscribes(event_subscribes.clone(), rpc_subscribes.clone(), hashed_event_subscribes.clone(), hashed_rpc_subscribes.clone())) {
                        Ok(()) => {}
                        Err(_msg) => panic!("ServerMsg::Send processing on client connect failed - send error, client addr hash {}", addr_hash)
                    }

                    let settings = ClientSettings {
                        addr,
                        addr_hash,
                        tx
                    };

                    client_settings.insert(addr_hash, settings);
                }
                SettingsMsg::GetSubscribes(addr_hash) => {
                    match client_settings.get_mut(&addr_hash) {
                        Some(settings) => {
                            match settings.tx.send(SettingsMsg2::Subscribes(event_subscribes.clone(), rpc_subscribes.clone(), hashed_event_subscribes.clone(), hashed_rpc_subscribes.clone())) {
                                Ok(()) => {}
                                Err(_msg) => panic!("SettingsMsg::Send processing failed - send error, client addr hash {}", addr_hash)
                            }
                        }
                        None => error!("No client with addr hash {} for sending subscribes", addr_hash)
                    }
                }
                SettingsMsg::RemoveClientSettings(addr_hash) => {
                    let _ = client_settings.remove(&addr_hash);
                }
                SettingsMsg::AddEventSubscribe(sender_addr_hash, subscribe, addr_hash, key_hash) => {
                    info!("Adding event subscribe to settings, {:?}, addr_hash {}, key_hash {}", subscribe, addr_hash, key_hash);

                    event_subscribes.push(subscribe);

                    match hashed_event_subscribes.get_mut(&key_hash) {
                        Some(addrs) => {
                            addrs.push(addr_hash);
                        }
                        None => {
                            hashed_event_subscribes.insert(key_hash, vec![addr_hash]);
                        }
                    }
                    
                    info!("Done");

                    info!("Sending notification to clients");

                    let (_, frames) = event_msg_as_frames("Server", Key::new("ReloadSubscribes", "", ""), json!({})).await.expect("Failed to create server event msg as frames");

                    for client_addr_hash in client_settings.keys() {
                        if *client_addr_hash != sender_addr_hash {
                            for frame in frames.clone() {
                                match server_tx_for_settings_loop.send(ServerMsg::Send(*client_addr_hash, frame)) {
                                    Ok(_) => {}
                                    Err(_) => panic!("ServerMsg::Send send failed for notification message from server")
                                }
                            }
                            
                        }
                    }

                    info!("Done");
                }
                SettingsMsg::AddRpcSubscribe(sender_addr_hash, subscribe, addr_hash, key_hash) => {
                    info!("Adding rpc subscribe to settings, {:?}, addr_hash {}, key_hash {}", subscribe, addr_hash, key_hash);

                    rpc_subscribes.push(subscribe);

                    match hashed_rpc_subscribes.get_mut(&key_hash) {
                        Some(addrs) => {
                            addrs.push(addr_hash);
                        }
                        None => {
                            hashed_rpc_subscribes.insert(key_hash, vec![addr_hash]);
                        }
                    }

                    info!("Done");

                    info!("Sending notification to clients");

                    let (_, frames) = event_msg_as_frames("Server", Key::new("ReloadSubscribes", "", ""), json!({})).await.expect("Failed to create server event msg as frames");

                    for client_addr_hash in client_settings.keys() {
                        if *client_addr_hash != sender_addr_hash {
                            for frame in frames.clone() {
                                match server_tx_for_settings_loop.send(ServerMsg::Send(*client_addr_hash, frame)) {
                                    Ok(_) => {}
                                    Err(_) => panic!("ServerMsg::Send send failed for notification message from server")
                                }
                            }
                            
                        }
                    }

                    info!("Done");
                }
                SettingsMsg::RemoveEventSubscribe(sender_addr_hash, subscribe, addr_hash, key_hash) => {                    
                    match event_subscribes.iter().position(|sub| sub.key == subscribe.key && sub.addr == subscribe.addr) {
                        Some(index) => {
                            info!("Removing event subscribe from settings, {:?}, addr_hash {}, key_hash {}", subscribe, addr_hash, key_hash);
                            event_subscribes.remove(index);

                            match hashed_event_subscribes.get_mut(&key_hash) {
                                Some(addrs) => {
                                    match addrs.iter().position(|addr| *addr == addr_hash) {
                                        Some(addr_hash_index) => {
                                            addrs.remove(addr_hash_index);
                                        }
                                        None => {
                                            error!("Addr hash not found for remove from settings in hashed, {:?}, addr_hash {}, key_hash {}", subscribe, addr_hash, key_hash);
                                        }                                        
                                    }

                                    if addrs.is_empty() {
                                        hashed_event_subscribes.remove(&key_hash);
                                    }

                                    info!("Sending notification to clients");

                                    let (_, frames) = event_msg_as_frames("Server", Key::new("ReloadSubscribes", "", ""), json!({})).await.expect("Failed to create server event msg as frames");

                                    for client_addr_hash in client_settings.keys() {
                                        if *client_addr_hash != sender_addr_hash {
                                            for frame in frames.clone() {
                                                match server_tx_for_settings_loop.send(ServerMsg::Send(*client_addr_hash, frame)) {
                                                    Ok(_) => {}
                                                    Err(_) => panic!("ServerMsg::Send send failed for notification message from server")
                                                }
                                            }
                                            
                                        }
                                    }

                                    info!("Done");
                                }
                                None => {
                                    error!("Event subscribe not found by key hash in hashed for remove from settings, {:?}, addr_hash {}, key_hash {}", subscribe, addr_hash, key_hash);
                                }
                            }
                        }
                        None => {
                            error!("Event subscribe not found for remove from settings, {:?}, addr_hash {}, key_hash {}", subscribe, addr_hash, key_hash);
                        }
                    };
                }
                SettingsMsg::RemoveRpcSubscribe(sender_addr_hash, subscribe, addr_hash, key_hash) => {
                    match rpc_subscribes.iter().position(|sub| sub.key == subscribe.key && sub.addr == subscribe.addr) {
                        Some(index) => {
                            info!("Removing rpc subscribe from settings, {:?}, addr_hash {}, key_hash {}", subscribe, addr_hash, key_hash);
                            rpc_subscribes.remove(index);

                            match hashed_rpc_subscribes.get_mut(&key_hash) {
                                Some(addrs) => {
                                    match addrs.iter().position(|addr| *addr == addr_hash) {
                                        Some(addr_hash_index) => {
                                            addrs.remove(addr_hash_index);
                                        }
                                        None => {
                                            error!("Addr hash not found for remove from settings in hashed, {:?}, addr_hash {}, key_hash {}", subscribe, addr_hash, key_hash);
                                        }                                        
                                    }

                                    if addrs.is_empty() {
                                        hashed_rpc_subscribes.remove(&key_hash);
                                    }

                                    info!("Sending notification to clients");

                                    let (_, frames) = event_msg_as_frames("Server", Key::new("ReloadSubscribes", "", ""), json!({})).await.expect("Failed to create server event msg as frames");

                                    for client_addr_hash in client_settings.keys() {
                                        if *client_addr_hash != sender_addr_hash {
                                            for frame in frames.clone() {
                                                match server_tx_for_settings_loop.send(ServerMsg::Send(*client_addr_hash, frame)) {
                                                    Ok(_) => {}
                                                    Err(_) => panic!("ServerMsg::Send send failed for notification message from server")
                                                }
                                            }
                                            
                                        }
                                    }

                                    info!("Done");
                                }
                                None => {
                                    error!("Rpc subscribe not found by key hash in hashed for remove from settings, {:?}, addr_hash {}, key_hash {}", subscribe, addr_hash, key_hash);
                                }
                            }
                        }
                        None => {
                            error!("Rpc subscribe not found for remove from settings, {:?}, addr_hash {}, key_hash {}", subscribe, addr_hash, key_hash);
                        }
                    };
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
        let settings_tx = settings_tx.clone();        
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

                            let (settings_per_client_tx, settings_per_client_rx) = mpsc::unbounded_channel();
                            let addr_hash = get_addr_hash(&addr);

                            match settings_tx.send(SettingsMsg::AddClient(addr.clone(), addr_hash, settings_per_client_tx)) {
                                Ok(()) => {}
                                Err(_msg) => panic!("SettingsMsg::AddClient send error, client addr {}, client addr hash {}", addr, addr_hash)
                            }

                            tokio::spawn(async move {                                
                                match process_write_tcp_stream(addr.clone(), addr_hash, &mut stream, &mut state, client_net_addr, server_tx, settings_tx, settings_per_client_rx).await {
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

async fn process_write_tcp_stream(addr: String, addr_hash: u64, tcp_stream: &mut TcpStream, state: &mut State, _client_net_addr: SocketAddr, server_tx: UnboundedSender<ServerMsg>, settings_tx: UnboundedSender<SettingsMsg>, mut settings_per_client_rx: UnboundedReceiver<SettingsMsg2>) -> Result<(), ProcessError> {    
    let (mut event_subscribes, mut rpc_subscribes, mut hashed_event_subscribes, mut hashed_rpc_subscribes) = match settings_per_client_rx.recv().await.expect("SettingsMsg2 receive failed") {
        SettingsMsg2::Subscribes(events, rpcs, hashed_events, hashed_rpcs) => (events, rpcs, hashed_events, hashed_rpcs)
    };

    info!("{} {:#?}", addr, rpc_subscribes);

    let mut stream_layouts: HashMap<u64, StreamLayout> = HashMap::new();

	loop {
		match state.read_frame() {
			ReadFrameResult::NotEnoughBytesForFrame => {
				state.read_from_tcp_stream(tcp_stream).await?;
			}
			ReadFrameResult::NextStep => {}
			ReadFrameResult::Frame(frame) => {
				debug!("Main stream frame read, frame type {}, msg type {}, stream id {}", frame.frame_type, frame.msg_type, frame.stream_id);

				match MsgType::from_u8(frame.msg_type) {
					MsgType::Event => {
                        match hashed_event_subscribes.get(&frame.key_hash) {
                            Some(targets) => {
                                match targets.len() {
                                    0 => {
                                        warn!("Subscribes empty for key hash {}, msg_type {:?}", frame.key_hash, MsgType::from_u8(frame.msg_type))
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
                            None => warn!("No subscribes found for key hash {}, msg_type {:?}", frame.key_hash, MsgType::from_u8(frame.msg_type))
                        }
                    }
					MsgType::RpcRequest => {                        
                        match hashed_rpc_subscribes.get(&frame.key_hash) {
                            Some(targets) => {
                                match targets.len() {
                                    0 => {
                                        warn!("Subscribes empty for key hash {}, msg_type {:?}", frame.key_hash, MsgType::from_u8(frame.msg_type))
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
                            None => warn!("No subscribes found for key hash {}, msg_type {:?}", frame.key_hash, MsgType::from_u8(frame.msg_type))
                        }
                    }
					MsgType::RpcResponse(_) => {
                        debug!("Sending frame to source, addr hash {}", frame.source_hash);
                        server_tx.send(ServerMsg::Send(frame.source_hash, frame))?;
                    }
                    MsgType::ServerEvent => {
                        warn!("ServerEvent frame received on server");
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
                                            "event_subscribes": event_subscribes,
                                            "rpc_subscribes": rpc_subscribes
                                        })
                                    }
                                    "ReloadSubscribes" => {
                                        settings_tx.send(SettingsMsg::GetSubscribes(addr_hash))?;

                                        (event_subscribes, rpc_subscribes, hashed_event_subscribes, hashed_rpc_subscribes) = match settings_per_client_rx.recv().await.expect("SettingsMsg2 receive failed") {
                                            SettingsMsg2::Subscribes(events, rpcs, hashed_events, hashed_rpcs) => (events, rpcs, hashed_events, hashed_rpcs)
                                        };

                                        json!({
                                            "result": "Reloaded"
                                        })
                                    }
                                    "AddEventSubscribe" => {
                                        let mut payload: Value = from_slice(&stream_layout.payload)?;
                                        let subscribe: Subscribe = from_value(payload["subscribe"].take())?;

                                        match event_subscribes.iter().any(|sub| sub.key == subscribe.key && sub.addr == subscribe.addr) {
                                            true => {
                                                warn!("Subscribe {:?} already present in event subscribes", subscribe);

                                                json!({
                                                    "result": "AlreadyPresent"
                                                })
                                            }
                                            false => {
                                                info!("Adding subscribe {:?} to event subscribes", subscribe);

                                                event_subscribes.push(subscribe.clone());                                                
                 
                                                let (subscribe_addr_hash, subscribe_key_hash) = to_hashed_subscribe(&subscribe);

                                                match hashed_event_subscribes.get_mut(&subscribe_key_hash) {
                                                    Some(addrs) => {
                                                        addrs.push(subscribe_addr_hash);
                                                    }
                                                    None => {
                                                        hashed_event_subscribes.insert(subscribe_key_hash, vec![subscribe_addr_hash]);
                                                    }
                                                }

                                                settings_tx.send(SettingsMsg::AddEventSubscribe(addr_hash, subscribe, subscribe_addr_hash, subscribe_key_hash))?;
                                                
                                                info!("Subscribe added to event subscribes");

                                                json!({
                                                    "result": "Added"
                                                })
                                            }
                                        }
                                    }
                                    "RemoveEventSubscribe" => {
                                        let mut payload: Value = from_slice(&stream_layout.payload)?;
                                        let subscribe: Subscribe = from_value(payload["subscribe"].take())?;
                                        let (subscribe_addr_hash, subscribe_key_hash) = to_hashed_subscribe(&subscribe);

                                        match event_subscribes.iter().position(|sub| sub.key == subscribe.key && sub.addr == subscribe.addr) {
                                            Some(index) => {
                                                info!("Removing event subscribe from settings, {:?}, addr_hash {}, key_hash {}", subscribe, subscribe_addr_hash, subscribe_key_hash);
                                                event_subscribes.remove(index);
                    
                                                match hashed_event_subscribes.get_mut(&subscribe_key_hash) {
                                                    Some(addrs) => {
                                                        match addrs.iter().position(|addr| *addr == subscribe_addr_hash) {
                                                            Some(addr_hash_index) => {
                                                                addrs.remove(addr_hash_index);
                                                            }
                                                            None => {
                                                                error!("Addr hash not found for remove from settings in hashed, {:?}, addr_hash {}, key_hash {}", subscribe, subscribe_addr_hash, subscribe_key_hash);
                                                            }                                        
                                                        }
                    
                                                        if addrs.is_empty() {
                                                            hashed_event_subscribes.remove(&subscribe_key_hash);
                                                        }
                                                    }
                                                    None => {
                                                        error!("Event subscribe not found by key hash in hashed for remove from settings, {:?}, addr_hash {}, key_hash {}", subscribe, subscribe_addr_hash, subscribe_key_hash);
                                                    }
                                                }

                                                settings_tx.send(SettingsMsg::RemoveEventSubscribe(addr_hash, subscribe, subscribe_addr_hash, subscribe_key_hash))?;
                                                
                                                info!("Subscribe removed from event subscribes");

                                                json!({
                                                    "result": "Removed"
                                                })
                                            }
                                            None => {
                                                error!("Event subscribe not found for remove from settings, {:?}, addr_hash {}, key_hash {}", subscribe, subscribe_addr_hash, subscribe_key_hash);
                                                json!({
                                                    "result": "NotFound"                                                    
                                                })
                                            }
                                        }
                                    }
                                    "AddRpcSubscribe" => {
                                        let mut payload: Value = from_slice(&stream_layout.payload)?;
                                        let subscribe: Subscribe = from_value(payload["subscribe"].take())?;

                                        match rpc_subscribes.iter().any(|sub| sub.key == subscribe.key && sub.addr == subscribe.addr) {
                                            true => {
                                                warn!("Subscribe {:?} already present in rpc subscribes", subscribe);

                                                json!({
                                                    "result": "AlreadyPresent"
                                                })
                                            }
                                            false => {
                                                info!("Adding subscribe {:?} to rpc subscribes", subscribe);

                                                rpc_subscribes.push(subscribe.clone());                                                
                                                
                                                let (subscribe_addr_hash, subscribe_key_hash) = to_hashed_subscribe(&subscribe);

                                                match hashed_rpc_subscribes.get_mut(&subscribe_key_hash) {
                                                    Some(addrs) => {
                                                        addrs.push(subscribe_addr_hash);
                                                    }
                                                    None => {
                                                        hashed_rpc_subscribes.insert(subscribe_key_hash, vec![subscribe_addr_hash]);
                                                    }
                                                }

                                                settings_tx.send(SettingsMsg::AddRpcSubscribe(addr_hash, subscribe, subscribe_addr_hash, subscribe_key_hash))?;
                                                
                                                info!("Subscribe added to rpc subscribes");

                                                json!({
                                                    "result": "Added"
                                                })
                                            }
                                        }
                                    }
                                    "RemoveRpcSubscribe" => {
                                        let mut payload: Value = from_slice(&stream_layout.payload)?;
                                        let subscribe: Subscribe = from_value(payload["subscribe"].take())?;
                                        let (subscribe_addr_hash, subscribe_key_hash) = to_hashed_subscribe(&subscribe);

                                        match rpc_subscribes.iter().position(|sub| sub.key == subscribe.key && sub.addr == subscribe.addr) {
                                            Some(index) => {
                                                info!("Removing rpc subscribe from settings, {:?}, addr_hash {}, key_hash {}", subscribe, subscribe_addr_hash, subscribe_key_hash);
                                                rpc_subscribes.remove(index);
                    
                                                match hashed_rpc_subscribes.get_mut(&subscribe_key_hash) {
                                                    Some(addrs) => {
                                                        match addrs.iter().position(|addr| *addr == subscribe_addr_hash) {
                                                            Some(addr_hash_index) => {
                                                                addrs.remove(addr_hash_index);
                                                            }
                                                            None => {
                                                                error!("Addr hash not found for remove from settings in hashed, {:?}, addr_hash {}, key_hash {}", subscribe, subscribe_addr_hash, subscribe_key_hash);
                                                            }                                        
                                                        }
                    
                                                        if addrs.is_empty() {
                                                            hashed_rpc_subscribes.remove(&subscribe_key_hash);
                                                        }
                                                    }
                                                    None => {
                                                        error!("Rpc subscribe not found by key hash in hashed for remove from settings, {:?}, addr_hash {}, key_hash {}", subscribe, subscribe_addr_hash, subscribe_key_hash);
                                                    }
                                                }

                                                settings_tx.send(SettingsMsg::RemoveRpcSubscribe(addr_hash, subscribe, subscribe_addr_hash, subscribe_key_hash))?;
                                                
                                                info!("Subscribe removed from rpc subscribes");

                                                json!({
                                                    "result": "Removed"
                                                })
                                            }
                                            None => {
                                                error!("Event subscribe not found for remove from settings, {:?}, addr_hash {}, key_hash {}", subscribe, subscribe_addr_hash, subscribe_key_hash);
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
