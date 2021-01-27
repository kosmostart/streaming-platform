use std::collections::HashMap;
use std::net::SocketAddr;
use log::*;
use tokio::runtime::Runtime;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, UnboundedSender};
use sp_dto::{Key, MsgType};
use sp_cfg::ServerConfig;
use crate::proto::*;

/// Starts the server based on provided ServerConfig struct. Creates new runtime and blocks.
pub fn start(config: ServerConfig, event_subscribes: HashMap<Key, Vec<String>>, rpc_subscribes: HashMap<Key, Vec<String>>, rpc_response_subscribes: HashMap<Key, Vec<String>>) {
    let rt = Runtime::new().expect("failed to create runtime"); 
    let _ = rt.block_on(start_future(config, event_subscribes, rpc_subscribes, rpc_response_subscribes));
}

/// Future for new server start based on provided ServerConfig struct, in case you want to create runtime by yourself.
pub async fn start_future(config: ServerConfig, event_subscribes: HashMap<Key, Vec<String>>, rpc_subscribes: HashMap<Key, Vec<String>>, rpc_response_subscribes: HashMap<Key, Vec<String>>) -> Result<(), ProcessError> {
    let listener = TcpListener::bind(config.host.clone()).await?;
    let (server_tx, mut server_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {        
        let mut clients = HashMap::new();
        loop {
            let msg = server_rx.recv().await.expect("ServerMsg receive failed");
            match msg {
                ServerMsg::AddClient(addr, net_addr, tx) => {
                    let client = Client { 
                        net_addr,
                        tx
                    };
                    clients.insert(addr, client);
                }
                ServerMsg::SendUnit(addr, stream_unit) => {
                    //info!("sending stream unit to client {}", addr);
                    match clients.get_mut(&addr) {
                        Some(client) => {
                            //match client.tx.send(stream_unit).await {
                            match client.tx.send(stream_unit) {
                                Ok(()) => {}
                                /*
                                Err(_) => {
                                    error!("error processing ServerMsg::SendUnit, send failed");
                                }
                                */                                
                                Err(_) => panic!("ServerMsg::SendArray processing failed - send error")
                            }
                            //info!("sent unit to client {}", addr);
                        }
                        None => error!("no client for send stream unit {}", addr)
                    }
                }                
                ServerMsg::RemoveClient(addr) => {
                    let _ = clients.remove(&addr);
                }
            }     
        }
    });

    let mut client_states = HashMap::new();

    loop {                
        let (mut stream, client_net_addr) = listener.accept().await?;
        info!("new connection from {}", client_net_addr);
        let config = config.clone();
        let server_tx = server_tx.clone();
        match auth_stream(&mut stream, client_net_addr, &config).await {
            Ok(addr) => {
                info!("stream from {} authorized as {}", client_net_addr, addr);
                if !client_states.contains_key(&addr) {
                    client_states.insert(addr.clone(), ClientState::new());
                }
                match client_states.get_mut(&addr) {
                    Some(client_state) => {
                        
                        if !client_state.has_writer {
                            client_state.has_writer = true;
                            let event_subscribes = event_subscribes.clone();
                            let rpc_subscribes = rpc_subscribes.clone();
                            let rpc_response_subscribes = rpc_response_subscribes.clone();
                            tokio::spawn(async move {                                
                                let res = process_write_stream(addr.clone(), event_subscribes, rpc_subscribes, rpc_response_subscribes, &mut stream, client_net_addr, server_tx).await;
                                error!("{} write process ended, {:?}", addr, res);
                            });
                        } else {
                            client_state.has_writer = false;
                            tokio::spawn(async move {            
                                let res = process_read_stream(addr.clone(), stream, client_net_addr, server_tx).await;
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

async fn auth_stream(stream: &mut TcpStream, _client_net_addr: SocketAddr, _config: &ServerConfig) -> Result<String, ProcessError> {    
    let mut state = State::new("Server".to_owned());
    let mut stream_layouts = HashMap::new();
    let auth_stream_layout;

    loop {
        match read(&mut state, stream).await? {
            ReadResult::MsgMeta(stream_id, msg_meta, _) => {
                //info!("{} {:?}", stream_id, msg_meta);
                stream_layouts.insert(stream_id, StreamLayout {
                    id: stream_id,
                    msg_meta,
                    payload: vec![],
                    attachments_data: vec![]
                });
            }
            ReadResult::PayloadData(stream_id, n, buf) => {
                //info!("payload data {} {}", stream_id, n);
                let stream_layout = stream_layouts.get_mut(&stream_id).ok_or(ProcessError::StreamLayoutNotFound)?;
                stream_layout.payload.extend_from_slice(&buf[..n]);

            }
            ReadResult::PayloadFinished(stream_id, n, buf) => {
                //info!("payload finished {} {}", stream_id, n);
                let stream_layout = stream_layouts.get_mut(&stream_id).ok_or(ProcessError::StreamLayoutNotFound)?;
                stream_layout.payload.extend_from_slice(&buf[..n]);
            }
            ReadResult::AttachmentData(stream_id, _, n, buf) => {
                let stream_layout = stream_layouts.get_mut(&stream_id).ok_or(ProcessError::StreamLayoutNotFound)?;
                stream_layout.attachments_data.extend_from_slice(&buf[..n]);
            }
            ReadResult::AttachmentFinished(stream_id, _, n, buf) => {
                let stream_layout = stream_layouts.get_mut(&stream_id).ok_or(ProcessError::StreamLayoutNotFound)?;
                stream_layout.attachments_data.extend_from_slice(&buf[..n]);
            }
            ReadResult::MessageFinished(stream_id, finish_bytes) => {
                //info!("message finished {}", stream_id);
                let stream_layout = stream_layouts.get_mut(&stream_id).ok_or(ProcessError::StreamLayoutNotFound)?;
                match finish_bytes {
                    MessageFinishBytes::Payload(n, buf) => {
                        stream_layout.payload.extend_from_slice(&buf[..n]);
                    }
                    MessageFinishBytes::Attachment(_, n, buf) => {
                        stream_layout.attachments_data.extend_from_slice(&buf[..n]);
                    }                    
                }
                auth_stream_layout = stream_layouts.remove(&stream_id);
                //read_tx.send(ClientMsg::Message(stream_layout.msg_meta, stream_layout.payload, stream_layout.attachments_data)).await?;
                break;
            }
            ReadResult::MessageAborted(stream_id) => {
                match stream_id {
                    Some(stream_id) => {
                        let _ = stream_layouts.remove(&stream_id);
                    }
                    None => {}
                }
            }
        }
    }
    
    let auth_stream_layout = auth_stream_layout.ok_or(ProcessError::AuthStreamLayoutIsEmpty)?;    
    //let auth_payload: Value = from_slice(&auth_stream_layout.payload)?;

    Ok(auth_stream_layout.msg_meta.tx)
}


async fn process_read_stream(addr: String, mut stream: TcpStream, client_net_addr: SocketAddr, server_tx: UnboundedSender<ServerMsg>) -> Result<(), ProcessError> {
    let mut _state = State::new("write stream from Server to ".to_owned() + &addr);    
    let (client_tx, client_rx) = mpsc::unbounded_channel();

    server_tx.send(ServerMsg::AddClient(addr.clone(), client_net_addr, client_tx))?;    

    write_loop(addr, client_rx, &mut stream).await
}

async fn process_write_stream(addr: String, event_subscribes: HashMap<Key, Vec<String>>, rpc_subscribes: HashMap<Key, Vec<String>>, rpc_response_subscribes: HashMap<Key, Vec<String>>, stream: &mut TcpStream, _client_net_addr: SocketAddr, server_tx: UnboundedSender<ServerMsg>) -> Result<(), ProcessError> {    
    let mut state = State::new("read stream from Server to ".to_owned() + &addr);        
    let mut client_addrs = HashMap::new();    

    loop {        
        match read(&mut state, stream).await? {
            ReadResult::MsgMeta(stream_id, msg_meta, buf) => {
                info!("{} {:?}", stream_id, msg_meta);

                client_addrs.insert(stream_id, (msg_meta.key.clone(), msg_meta.msg_type.clone()));

                let subscribes = match msg_meta.msg_type {
                    MsgType::Event => &event_subscribes,
                    MsgType::RpcRequest => &rpc_subscribes,
                    MsgType::RpcResponse(_) => &rpc_response_subscribes
                };
                
                match subscribes.get(&msg_meta.key) {
                    Some(targets) => {
                        for target in targets {     
                            debug!("Sending unit to addr11 {}", target);
                            server_tx.send(ServerMsg::SendUnit(target.clone(), StreamUnit::Vector(stream_id, buf.clone())))?;
                        }
                    }
                    None => warn!("No subscribes found for key {:#?}, msg_type {:#?}", msg_meta.key, msg_meta.msg_type)
                }
            }
            ReadResult::PayloadData(stream_id, n, buf) => {                        
                let (key, msg_type) = client_addrs.get(&stream_id).ok_or(ProcessError::ClientAddrNotFound)?;

                let subscribes = match msg_type {
                    MsgType::Event => &event_subscribes,
                    MsgType::RpcRequest => &rpc_subscribes,
                    MsgType::RpcResponse(_) => &rpc_response_subscribes
                };

                match subscribes.get(key) {
                    Some(targets) => {
                        for target in targets {                            
                            debug!("Sending unit to addr9 {}", target);
                            server_tx.send(ServerMsg::SendUnit(target.clone(), StreamUnit::Array(stream_id, n, buf.clone())))?;
                        }
                    }
                    None => warn!("No subscribes found for key {:#?}", key)
                }
            }
            ReadResult::PayloadFinished(stream_id, n, buf) => {
                let (key, msg_type) = client_addrs.get(&stream_id).ok_or(ProcessError::ClientAddrNotFound)?;

                let subscribes = match msg_type {
                    MsgType::Event => &event_subscribes,
                    MsgType::RpcRequest => &rpc_subscribes,
                    MsgType::RpcResponse(_) => &rpc_response_subscribes
                };

                match subscribes.get(key) {
                    Some(targets) => {
                        for target in targets {                            
                            debug!("Sending unit to addr7 {}", target);
                            server_tx.send(ServerMsg::SendUnit(target.clone(), StreamUnit::Array(stream_id, n, buf.clone())))?;
                        }
                    }
                    None => warn!("No subscribes found for key {:#?}", key)
                }
            }
            ReadResult::AttachmentData(stream_id, _, n, buf) => {
                let (key, msg_type) = client_addrs.get(&stream_id).ok_or(ProcessError::ClientAddrNotFound)?;

                let subscribes = match msg_type {
                    MsgType::Event => &event_subscribes,
                    MsgType::RpcRequest => &rpc_subscribes,
                    MsgType::RpcResponse(_) => &rpc_response_subscribes
                };
                
                match subscribes.get(key) {
                    Some(targets) => {
                        for target in targets {                            
                            debug!("Sending unit to addr5 {}", target);
                            server_tx.send(ServerMsg::SendUnit(target.clone(), StreamUnit::Array(stream_id, n, buf.clone())))?;
                        }
                    }
                    None => warn!("No subscribes found for key {:#?}", key)
                }
            }
            ReadResult::AttachmentFinished(stream_id, _, n, buf) => {
                let (key, msg_type) = client_addrs.get(&stream_id).ok_or(ProcessError::ClientAddrNotFound)?;

                let subscribes = match msg_type {
                    MsgType::Event => &event_subscribes,
                    MsgType::RpcRequest => &rpc_subscribes,
                    MsgType::RpcResponse(_) => &rpc_response_subscribes
                };
                
                match subscribes.get(key) {
                    Some(targets) => {
                        for target in targets {                            
                            debug!("Sending unit to addr3 {}", target);
                            server_tx.send(ServerMsg::SendUnit(target.clone(), StreamUnit::Array(stream_id, n, buf.clone())))?;
                        }
                    }
                    None => warn!("No subscribes found for key {:#?}", key)
                }
            }
            ReadResult::MessageFinished(stream_id, finish_bytes) => {
                let (key, msg_type) = client_addrs.get(&stream_id).ok_or(ProcessError::ClientAddrNotFound)?;

                let subscribes = match msg_type {
                    MsgType::Event => &event_subscribes,
                    MsgType::RpcRequest => &rpc_subscribes,
                    MsgType::RpcResponse(_) => &rpc_response_subscribes
                };
                
                match finish_bytes {
                    MessageFinishBytes::Payload(n, buf) |
                    MessageFinishBytes::Attachment(_, n, buf) => {                        
                        match subscribes.get(key) {
                            Some(targets) => {
                                for target in targets {                                    
                                    debug!("Sending unit to addr1 {}", target);
                                    server_tx.send(ServerMsg::SendUnit(target.clone(), StreamUnit::Array(stream_id, n, buf.clone())))?;
                                }
                            }
                            None => warn!("No subscribes found for key {:#?}", key)
                        }
                    }                            
                }
            }
            ReadResult::MessageAborted(stream_id) => {
                match stream_id {
                    Some(stream_id) => {
                        let _ = client_addrs.remove(&stream_id).ok_or(ProcessError::ClientAddrNotFound)?;
                    }
                    None => {}
                }
            }
        }        
    }
}