use std::collections::HashMap;
use std::io::{Cursor, BufReader, Read};
use std::net::SocketAddr;
use log::*;
use futures::{select, pin_mut, future::FutureExt};
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
                ServerMsg::SendBuf(addr, n, buf) => {
                    //info!("send buf {} {}", addr, n);

                    match clients.get_mut(&addr) {
                        Some(client) => {
                            match client.tx.send((n, buf)).await {
                                Ok(()) => {}
                                Err(_) => panic!("ServerMsg::SendBuf processing failed on tx send")
                            }
                            //info!("send buf ok {} {}", addr, n);
                        }
                        None => {
                            info!("no client for send buf {} {}", addr, n);
                        }
                    }
                }
                ServerMsg::RemoveClient(addr) => {
                    clients.remove(&addr);
                }
            }     
        }
    });    

    println!("ok");

    loop {        
        let (mut stream, client_net_addr) = listener.accept().await?;
        let config = config.clone();
        let server_tx = server_tx.clone();

        println!("connected");  

        tokio::spawn(async move {
            let res = process_stream(stream, client_net_addr, server_tx, &config).await;

            println!("{:?}", res);
        });        
    }
}

async fn process_stream(mut stream: TcpStream, client_net_addr: SocketAddr, mut server_tx: Sender<ServerMsg>, config: &ServerConfig) -> Result<(), ProcessError> {
    let (mut socket_read, mut socket_write) = stream.split();

    let (auth_msg_meta, auth_payload, auth_attachments) = read_full(&mut socket_read).await?;
    let auth_payload: Value = from_slice(&auth_payload)?;    

    println!("auth {:?}", auth_msg_meta);
    println!("auth {:?}", auth_payload);
    
    let (mut client_tx, mut client_rx) = mpsc::channel(MPSC_CLIENT_BUF_SIZE);

    server_tx.send(ServerMsg::AddClient(auth_msg_meta.tx.clone(), client_net_addr, client_tx)).await?;

    let mut adapter = socket_read.take(LEN_BUF_SIZE as u64);
    let mut state = State::new();

    let mut len_buf: Option<[u8; DATA_BUF_SIZE]> = None;
    let mut msg_meta: Option<MsgMeta> = None;

    loop {
        let f1 = read(&mut state, &mut adapter).fuse();
        let f2 = client_rx.recv().fuse();

        pin_mut!(f1, f2);

        let res = select! {
            res = f1 => {                
                let res = res?;                

                match res {
                    ReadResult::LenFinished(buf) => {
                        //info!("len finished");
                        len_buf = Some(buf);                 
                    }
                    ReadResult::MsgMeta(new_msg_meta, buf) => {
                        //info!("msg meta");
                        info!("{:?}", new_msg_meta);
                        server_tx.send(ServerMsg::SendBuf(new_msg_meta.rx.clone(), LEN_BUF_SIZE, len_buf.ok_or(ProcessError::NoneError)?)).await?;
                        len_buf = None;
                        //info!("server new_msg_meta len {}", buf.len());
                        server_write(buf, &new_msg_meta.rx, &mut server_tx).await?;
                        msg_meta = Some(new_msg_meta);                        
                    }
                    ReadResult::PayloadData(n, buf) => {
                        //info!("payload data");
                        match msg_meta {
                            Some(ref msg_meta) => {
                                server_tx.send(ServerMsg::SendBuf(msg_meta.rx.clone(), n, buf)).await?;
                            }
                            None => {

                            }
                        }
                    }
                    ReadResult::PayloadFinished => {
                        //info!("payload finished");
                    }
                    ReadResult::AttachmentData(index, n, buf) => {
                        //info!("attachment data");
                        match msg_meta {
                            Some(ref msg_meta) => {
                                server_tx.send(ServerMsg::SendBuf(msg_meta.rx.clone(), n, buf)).await?;
                            }
                            None => {

                            }
                        }
                    }
                    ReadResult::AttachmentFinished(index) => {
                        //info!("attachment finished");
                    }
                    ReadResult::MessageFinished => {                        
                        //info!("message finished");
                        msg_meta = None;
                    }
                };                            
            }
            res = f2 => {
                //info!("f2 {}", auth_msg_meta.tx);
                let (n, buf) = res?;
                socket_write.write_all(&buf[..n]).await?;
                //info!("f2 ok {} {}", auth_msg_meta.tx, n);
            }
        };
    }
}
