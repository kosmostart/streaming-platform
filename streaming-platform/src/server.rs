use std::collections::HashMap;
use std::io::{Cursor, BufReader, Read};
use std::net::SocketAddr;
use log::*;
use tokio::runtime::Runtime;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, Sender};
use tokio::prelude::*;
use serde_json::{Value, from_slice};
use sp_dto::MsgMeta;    
use crate::proto::*;

pub fn start(config: ServerConfig) {
    let mut rt = Runtime::new().expect("failed to create runtime"); 
    rt.block_on(start_future(config));
}

pub async fn start_future(config: ServerConfig) -> Result<(), ProcessError> {
    let mut listener = TcpListener::bind(config.host.clone()).await?;
    let (mut server_tx, mut server_rx) = mpsc::channel(MPSC_SERVER_BUF_SIZE);
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
                            match client.tx.try_send(stream_unit) {
                                Ok(()) => {}
                                /*
                                Err(_) => {
                                    error!("error processing ServerMsg::SendUnit, send failed");
                                }
                                */                                
                                Err(e) => { 
                                    match e {                                        
                                        tokio::sync::mpsc::error::TrySendError::Full(_) => panic!("ServerMsg::SendArray processing failed - buffer full"),
                                        tokio::sync::mpsc::error::TrySendError::Closed(_) => panic!("ServerMsg::SendArray processing failed - client channel closed")
                                    }                                    
                                }                                
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
                            tokio::spawn(async move {            
                                let res = process_write_stream(addr.clone(), &mut stream, client_net_addr, server_tx, &config).await;
                                error!("{} reader process eneded, {:?}", addr, res);
                            });
                        } else {
                            if !client_state.has_reader {
                                client_state.has_reader = true;
                                tokio::spawn(async move {            
                                    let res = process_read_stream(addr.clone(), stream, client_net_addr, server_tx, &config).await;
                                    error!("{} reader process eneded, {:?}", addr, res);
                                });
                            } else {
                                error!("writer and reader already present");
                            }                            
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
    has_writer: bool,
    has_reader: bool
}

impl ClientState {
    pub fn new() -> ClientState {
        ClientState {
            has_writer: false,
            has_reader: false
        }
    }
}

async fn auth_stream(stream: &mut TcpStream, client_net_addr: SocketAddr, config: &ServerConfig) -> Result<String, ProcessError> {    
    let mut state = State::new("Server".to_owned());
    let mut stream_layouts = HashMap::new();
    let mut auth_stream_layout = None;

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


async fn process_read_stream(addr: String, mut stream: TcpStream, client_net_addr: SocketAddr, mut server_tx: Sender<ServerMsg>, config: &ServerConfig) -> Result<(), ProcessError> {    
    let mut state = State::new("write stream from Server to ".to_owned() + &addr);    
    let (mut client_tx, mut client_rx) = mpsc::channel(MPSC_CLIENT_BUF_SIZE);

    server_tx.try_send(ServerMsg::AddClient(addr.clone(), client_net_addr, client_tx))?;    

    write_loop(addr, client_rx, &mut stream).await
}

async fn process_write_stream(addr: String, stream: &mut TcpStream, client_net_addr: SocketAddr, mut server_tx: Sender<ServerMsg>, config: &ServerConfig) -> Result<(), ProcessError> {    
    let mut state = State::new("read stream from Server to ".to_owned() + &addr);        
    let mut client_addrs = HashMap::new();

    loop {        
        match read(&mut state, stream).await? {
            ReadResult::MsgMeta(stream_id, msg_meta, buf) => {
                //info!("{} {:?}", stream_id, msg_meta);
                client_addrs.insert(stream_id, msg_meta.rx.clone());
                //info!("sending msg meta");
                server_tx.try_send(ServerMsg::SendUnit(msg_meta.rx.clone(), StreamUnit::Vector(stream_id, buf)))?;
            }
            ReadResult::PayloadData(stream_id, n, buf) => {                        
                let client_addr = client_addrs.get(&stream_id).ok_or(ProcessError::ClientAddrNotFound)?;
                //info!("sending payload data");
                server_tx.try_send(ServerMsg::SendUnit(client_addr.clone(), StreamUnit::Array(stream_id, n, buf)))?;
            }
            ReadResult::PayloadFinished(stream_id, n, buf) => {
                let client_addr = client_addrs.get(&stream_id).ok_or(ProcessError::ClientAddrNotFound)?;
                //info!("sending payload finished");
                server_tx.try_send(ServerMsg::SendUnit(client_addr.clone(), StreamUnit::Array(stream_id, n, buf)))?;
            }
            ReadResult::AttachmentData(stream_id, _, n, buf) => {
                let client_addr = client_addrs.get(&stream_id).ok_or(ProcessError::ClientAddrNotFound)?;
                //info!("sending attachment");
                server_tx.try_send(ServerMsg::SendUnit(client_addr.clone(), StreamUnit::Array(stream_id, n, buf)))?;
            }
            ReadResult::AttachmentFinished(stream_id, _, n, buf) => {
                let client_addr = client_addrs.get(&stream_id).ok_or(ProcessError::ClientAddrNotFound)?;
                //info!("sending attachment finished");
                server_tx.try_send(ServerMsg::SendUnit(client_addr.clone(), StreamUnit::Array(stream_id, n, buf)))?;
            }
            ReadResult::MessageFinished(stream_id, finish_bytes) => {
                let client_addr = client_addrs.remove(&stream_id).ok_or(ProcessError::ClientAddrNotFound)?;
                match finish_bytes {
                    MessageFinishBytes::Payload(n, buf) => {
                        server_tx.try_send(ServerMsg::SendUnit(client_addr.clone(), StreamUnit::Array(stream_id, n, buf)))?;
                    }
                    MessageFinishBytes::Attachment(_, n, buf) => {
                        server_tx.try_send(ServerMsg::SendUnit(client_addr.clone(), StreamUnit::Array(stream_id, n, buf)))?;
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
