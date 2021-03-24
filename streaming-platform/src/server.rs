use std::collections::HashMap;
use std::net::SocketAddr;
use std::hash::Hasher;
use log::*;
use siphasher::sip::SipHasher24;
use tokio::runtime::Runtime;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, UnboundedSender};
use sp_dto::bytes::{Buf, BytesMut, BufMut};
use sp_dto::{Key, MsgType, Subscribes};
use sp_cfg::ServerConfig;
use crate::proto::*;

fn to_hashed_subscribes(key_hasher: &mut SipHasher24, subscribes: HashMap<Key, Vec<String>>) -> HashMap<u64, Vec<String>> {
    let mut res = HashMap::new();
    let mut buf = BytesMut::new();

    for (key, value) in subscribes {
        let key_hash = get_key_hash(key_hasher, &mut buf, key);
        res.insert(key_hash, value);
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
                        net_addr,
                        tx
                    };
                    clients.insert(addr, client);
                }
                ServerMsg::Send(addr, stream_unit) => {
                    //info!("sending stream unit to client {}", addr);
                    match clients.get_mut(&addr) {
                        Some(client) => {
                            //match client.tx.send(stream_unit).await {
                            match client.tx.send(stream_unit) {
                                Ok(()) => {}
                                /*
                                Err(_) => {
                                    error!("error processing ServerMsg::Send, send failed");
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

    let (event_subscribes, rpc_subscribes, rpc_response_subscribes) = match subscribes {
        Subscribes::ByAddr(_, _, _) => subscribes.traverse_to_keys(),
        Subscribes::ByKey(event_subscribes, rpc_subscribes, rpc_response_subscribes) => (event_subscribes, rpc_subscribes, rpc_response_subscribes)
    };

    let mut client_states = HashMap::new();
    let mut key_hasher = get_key_hasher();

    let event_subscribes = to_hashed_subscribes(&mut key_hasher, event_subscribes);
    let rpc_subscribes = to_hashed_subscribes(&mut key_hasher, rpc_subscribes);
    let rpc_response_subscribes = to_hashed_subscribes(&mut key_hasher, rpc_response_subscribes);

    info!("Started on {}", config.host);

    loop {                
        let (mut stream, client_net_addr) = listener.accept().await?;

        info!("New connection from {}", client_net_addr);

        let config = config.clone();
        let server_tx = server_tx.clone();

        match auth_tcp_stream(&mut stream, client_net_addr, &config).await {
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
                            let rpc_response_subscribes = rpc_response_subscribes.clone();
                            tokio::spawn(async move {                                
                                let res = process_write_tcp_stream(addr.clone(), event_subscribes, rpc_subscribes, rpc_response_subscribes, &mut stream, client_net_addr, server_tx).await;
                                error!("{} write process ended, {:?}", addr, res);
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

async fn auth_tcp_stream(tcp_stream: &mut TcpStream, _client_net_addr: SocketAddr, _config: &ServerConfig) -> Result<String, ProcessError> {    
    let mut state = State2::new();
    
    //let mut msg_meta_vec = vec![];

    loop {
        match state.read_frame(tcp_stream).await {
            Ok(frame) => {

                debug!("Process auth tcp stream: got frame, stream_id {}", frame.stream_id);
            }
            Err(e) => {
                error!("Error on read from {}: {:?}", "addr", e);
                state.clear();
            }
        } 
    }
       
    //let auth_payload: Value = from_slice(&auth_stream_layout.payload)?;

    Ok("".to_owned())
}


async fn process_read_tcp_stream(addr: String, mut tcp_stream: TcpStream, client_net_addr: SocketAddr, server_tx: UnboundedSender<ServerMsg>) -> Result<(), ProcessError> {
    let mut _state = State::new("Write tcp stream from Server to ".to_owned() + &addr);    
    let (client_tx, client_rx) = mpsc::unbounded_channel();

    server_tx.send(ServerMsg::AddClient(addr.clone(), client_net_addr, client_tx))?;    

    write_loop(addr, client_rx, &mut tcp_stream).await
}

async fn process_write_tcp_stream(addr: String, event_subscribes: HashMap<u64, Vec<String>>, rpc_subscribes: HashMap<u64, Vec<String>>, rpc_response_subscribes: HashMap<u64, Vec<String>>, tcp_stream: &mut TcpStream, _client_net_addr: SocketAddr, server_tx: UnboundedSender<ServerMsg>) -> Result<(), ProcessError> {
    let mut state = State2::new();        

    loop {
        match state.read_frame(tcp_stream).await {
            Ok(frame) => {
                debug!("Process write tcp stream: got frame, stream_id {}", frame.stream_id);

                let subscribes = match frame.get_msg_type()? {
                    MsgType::Event => &event_subscribes,
                    MsgType::RpcRequest => &rpc_subscribes,
                    MsgType::RpcResponse(_) => &rpc_response_subscribes
                };
                
                match subscribes.get(&frame.key_hash) {
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
            Err(e) => {
                error!("Error on read from {}: {:?}", addr, e);
                state.clear();
            }
        } 
    }
}

/*
async fn process_write_tcp_stream(addr: String, event_subscribes: HashMap<Key, Vec<String>>, rpc_subscribes: HashMap<Key, Vec<String>>, rpc_response_subscribes: HashMap<Key, Vec<String>>, stream: &mut TcpStream, _client_net_addr: SocketAddr, server_tx: UnboundedSender<ServerMsg>) -> Result<(), ProcessError> {    
    let mut state = State::new("Read stream from Server to ".to_owned() + &addr);        
    let mut client_addrs = HashMap::new();    

    loop {        
        match read(&mut state, stream).await? {
            ReadResult::MsgMeta(stream_id, msg_meta, buf) => {
                info!("Sending from {}, key is {:?}, message type is {:?}, stream id is {}", msg_meta.tx, msg_meta.key, msg_meta.msg_type, stream_id);
                debug!("{}, {:?}", stream_id, msg_meta);

                client_addrs.insert(stream_id, (msg_meta.key.clone(), msg_meta.msg_type.clone()));

                let subscribes = match msg_meta.msg_type {
                    MsgType::Event => &event_subscribes,
                    MsgType::RpcRequest => &rpc_subscribes,
                    MsgType::RpcResponse(_) => &rpc_response_subscribes
                };
                
                match subscribes.get(&msg_meta.key) {
                    Some(targets) => {
                        for target in targets {     
                            debug!("Sending unit to {}", target);
                            server_tx.send(ServerMsg::Send(target.clone(), Frame::Vector(stream_id, buf.clone())))?;
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
                            server_tx.send(ServerMsg::Send(target.clone(), Frame::Array(stream_id, n, buf.clone())))?;
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
                            server_tx.send(ServerMsg::Send(target.clone(), Frame::Array(stream_id, n, buf.clone())))?;
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
                            server_tx.send(ServerMsg::Send(target.clone(), Frame::Array(stream_id, n, buf.clone())))?;
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
                            server_tx.send(ServerMsg::Send(target.clone(), Frame::Array(stream_id, n, buf.clone())))?;
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
                                    server_tx.send(ServerMsg::Send(target.clone(), Frame::Array(stream_id, n, buf.clone())))?;
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
*/