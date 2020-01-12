use std::collections::HashMap;
use std::io::{Cursor, BufReader, Read};
use std::net::SocketAddr;
use log::*;
use futures::{select, pin_mut, future::FutureExt};
use bytes::{BytesMut, BufMut};
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
                    //info!("send buf {} {}", addr, n);
                    match clients.get_mut(&addr) {
                        Some(client) => {
                            match client.tx.send(stream_unit).await {
                                Ok(()) => {}
                                Err(_) => panic!("ServerMsg::SendArray processing failed on tx send")
                            }
                            //info!("send buf ok {} {}", addr, n);
                        }
                        None => info!("no client for send array {}", addr)
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
    let (mut socket_read, mut socket_write) = stream.split();

    //let (auth_msg_meta, auth_payload, auth_attachments) = read_full(&mut socket_read).await?;
    //let auth_payload: Value = from_slice(&auth_payload)?;        
    
    let (mut client_tx, mut client_rx) = mpsc::channel(MPSC_CLIENT_BUF_SIZE);

    server_tx.send(ServerMsg::AddClient("".to_owned(), client_net_addr, client_tx)).await?;
    //server_tx.send(ServerMsg::AddClient(auth_msg_meta.tx.clone(), client_net_addr, client_tx)).await?;

    let mut adapter = socket_read.take(LENS_BUF_SIZE as u64);
    let mut state = State::new();
    let mut buf_u32 = BytesMut::with_capacity(4);
    let mut client_addrs = HashMap::new();
    loop {
        let f1 = read(&mut state, &mut adapter).fuse();
        let f2 = client_rx.recv().fuse();
        pin_mut!(f1, f2);
        let res = select! {
            res = f1 => {
                match res? {                    
                    ReadResult::MsgMeta(stream_id, msg_meta, buf) => {
                        info!("{:?}", msg_meta);
                        client_addrs.insert(stream_id, msg_meta.rx.clone());
                        server_tx.send(ServerMsg::SendUnit(msg_meta.rx.clone(), StreamUnit::Vector(stream_id, buf))).await?;
                    }
                    ReadResult::PayloadData(stream_id, n, buf) => {                        
                        let client_addr = client_addrs.get(&stream_id).ok_or(ProcessError::ClientAddrNotFound)?;
                        server_tx.send(ServerMsg::SendUnit(client_addr.clone(), StreamUnit::Array(stream_id, n, buf))).await?;
                    }
                    ReadResult::PayloadFinished(stream_id, n, buf) => {
                        let client_addr = client_addrs.get(&stream_id).ok_or(ProcessError::ClientAddrNotFound)?;
                        server_tx.send(ServerMsg::SendUnit(client_addr.clone(), StreamUnit::Array(stream_id, n, buf))).await?;
                    }
                    ReadResult::AttachmentData(stream_id, _, n, buf) => {
                        let client_addr = client_addrs.get(&stream_id).ok_or(ProcessError::ClientAddrNotFound)?;
                        server_tx.send(ServerMsg::SendUnit(client_addr.clone(), StreamUnit::Array(stream_id, n, buf))).await?;
                    }
                    ReadResult::AttachmentFinished(stream_id, _, n, buf) => {
                        let client_addr = client_addrs.get(&stream_id).ok_or(ProcessError::ClientAddrNotFound)?;
                        server_tx.send(ServerMsg::SendUnit(client_addr.clone(), StreamUnit::Array(stream_id, n, buf))).await?;
                    }
                    ReadResult::MessageFinished(stream_id) => {                        
                        let _ = client_addrs.remove(&stream_id).ok_or(ProcessError::ClientAddrNotFound)?;
                    }
                };                            
            }
            res = f2 => {
                //info!("f2 {}", auth_msg_meta.tx);                
                match res? {
                    StreamUnit::Array(stream_id, n, buf) => {
                        buf_u32.put_u32(stream_id);
                        socket_write.write_all(&buf_u32[..]).await?;
                        buf_u32.put_u32(n as u32);
                        socket_write.write_all(&buf_u32[..]).await?;
                        socket_write.write_all(&buf[..n]).await?;
                    }
                    StreamUnit::Vector(stream_id, buf) => {
                        buf_u32.put_u32(stream_id);
                        socket_write.write_all(&buf_u32[..]).await?;
                        buf_u32.put_u32(buf.len() as u32);
                        socket_write.write_all(&buf_u32[..]).await?;
                        socket_write.write_all(&buf).await?;
                    }
                }
                //info!("f2 ok {} {}", auth_msg_meta.tx, n);
            }
        };
    }
}
