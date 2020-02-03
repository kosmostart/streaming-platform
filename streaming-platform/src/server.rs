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
    loop {        
        let (mut stream, client_net_addr) = listener.accept().await?;
        let config = config.clone();
        let server_tx = server_tx.clone();
        info!("connected");  
        tokio::spawn(async move {
            let res = process_stream(stream, client_net_addr, server_tx, &config).await;
            info!("{:?}", res);
        });        
    }
}

async fn process_stream(mut stream: TcpStream, client_net_addr: SocketAddr, mut server_tx: Sender<ServerMsg>, config: &ServerConfig) -> Result<(), ProcessError> {
    let addr = "Server".to_owned();
    let (mut socket_read, mut socket_write) = tokio::io::split(stream);
    let mut state = State::new(addr.clone());
    let mut stream_layouts = HashMap::new();
    let mut auth_stream_layout = None;

    loop {
        match read(&mut state, &mut socket_read).await? {
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
    let auth_payload: Value = from_slice(&auth_stream_layout.payload)?;
    state.addr = "Server connection from ".to_owned() + &auth_stream_layout.msg_meta.tx;
    //info!("{:?}", auth_stream_layout.msg_meta);
    let (mut client_tx, mut client_rx) = mpsc::channel(MPSC_CLIENT_BUF_SIZE);
    server_tx.try_send(ServerMsg::AddClient(auth_stream_layout.msg_meta.tx, client_net_addr, client_tx))?;
    let mut client_addrs = HashMap::new();

    tokio::spawn(async move {
        let res = write_loop(addr, client_rx, socket_write).await;
        error!("{:?}", res);
    });
    
    loop {        
        match read(&mut state, &mut socket_read).await? {
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
