use std::collections::HashMap;
use std::io::{Cursor, BufReader, Read};
use std::net::SocketAddr;
use futures::future::FutureExt;
use futures::{select, pin_mut};
use tokio::runtime::Runtime;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, Sender};
use tokio::prelude::*;
use serde_json::{Value, from_slice};
use sp_dto::MsgMeta;    
use crate::proto::*;

pub fn start(config: Config) {
    let rt = Runtime::new().expect("failed to create runtime"); 
    
    rt.block_on(start_future(config));
}

pub async fn start_future(config: Config) -> Result<(), ProcessError> {
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
                    match clients.get_mut(&addr) {
                        Some(client) => client.tx.send((n, buf)).await.expect("ServerMsg::SendBuf processing failed on tx send"),
                        None => {
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

async fn process_stream(mut stream: TcpStream, client_net_addr: SocketAddr, mut server_tx: Sender<ServerMsg>, config: &Config) -> Result<(), ProcessError> {
    let (mut socket_read, mut socket_write) = stream.split();

    let (auth_msg_meta, auth_payload, auth_attachments) = read_full(&mut socket_read).await?;
    let auth_payload: Value = from_slice(&auth_payload)?;    

    println!("auth {:?}", auth_msg_meta);
    println!("auth {:?}", auth_payload);
    
    let (mut client_tx, mut client_rx) = mpsc::channel(MPSC_CLIENT_BUF_SIZE);

    server_tx.send(ServerMsg::AddClient(auth_msg_meta.tx.clone(), client_net_addr, client_tx)).await?;

    let mut adapter = socket_read.take(LEN_BUF_SIZE as u64);
    let mut state = State::new();

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
                        match msg_meta {
                            Some(ref msg_meta) => {
                                server_tx.send(ServerMsg::SendBuf(msg_meta.tx.clone(), LEN_BUF_SIZE, buf)).await?;
                            }
                            None => {

                            }
                        }
                    }
                    ReadResult::MsgMeta(new_msg_meta, buf) => {
                        println!("{:?}", new_msg_meta);
                        server_write(buf, &new_msg_meta.rx, &mut server_tx);
                        msg_meta = Some(new_msg_meta);                        
                    }
                    ReadResult::PayloadData(n, buf) => {                        
                        match msg_meta {
                            Some(ref msg_meta) => {
                                server_tx.send(ServerMsg::SendBuf(msg_meta.tx.clone(), n, buf)).await?;
                            }
                            None => {

                            }
                        }
                    }
                    ReadResult::PayloadFinished => {
                        println!("payload ok");
                    }
                    ReadResult::AttachmentData(index, n, buf) => {
                        match msg_meta {
                            Some(ref msg_meta) => {
                                server_tx.send(ServerMsg::SendBuf(msg_meta.tx.clone(), n, buf)).await?;
                            }
                            None => {

                            }
                        }
                    }
                    ReadResult::AttachmentFinished(index) => {
                        //println!("attachment ok");
                    }
                    ReadResult::MessageFinished => {
                        //println!("message ok");
                    }
                };                            
            }
            res = f2 => {
                let (n, buf) = res?;
                socket_write.write_all(&buf[..n]).await?;
            }
        };
    }

    /*
    println!("{:?}", payload);

    server_tx.send(ServerMsg::AddClient(msg_meta.tx.clone(), client_net_addr, client_tx))?;             

    loop {
        let route = select! {
            recv(state.rx) -> msg => BufRoute::Broker(msg?),
            recv(client_rx) -> msg => BufRoute::Socket(msg?)
        };

        match route {
            BufRoute::Broker(buf) => {
                server_tx.send(ServerMsg::SendBuf(msg_meta.tx.clone(), buf))?;
            }
            BufRoute::Socket(buf) => {
                socket_write.write_all(&buf).await?;
            }
        }
    }
    */
    
    /*
    match state.read_msg(&mut socket_read).await {
        Ok(_) => {
            loop {
                if state.read_msg(&mut socket_read).await.is_err() {
                    break;
                }

                // remove client because we have stopped reading from socket
                //server_tx.send(ServerMsg::RemoveClient(client_addr));
            }
        }
        Err(err) => {

        }
    }
    */ 

}
