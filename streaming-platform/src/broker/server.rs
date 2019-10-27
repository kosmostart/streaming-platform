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
use crate::proto::*;

pub fn start() {
    let rt = Runtime::new().expect("failed to create runtime"); 
    
    rt.block_on(start_future());
}

pub async fn start_future() -> Result<(), ProcessError> {
    let config_path = std::env::args().nth(1)
        .expect("path to config file not passed as argument");

    let file = std::fs::File::open(config_path)
        .expect("failed to open config");

    let mut buf_reader = BufReader::new(file);

    let mut config_string = String::new();

    buf_reader.read_to_string(&mut config_string)
        .expect("failed to read config");

    let config: Config = toml::from_str(&config_string)
        .expect("failed to deserialize config");	

    let mut listener = TcpListener::bind(config.host.clone()).await?;

    let (mut server_tx, mut server_rx) = mpsc::channel(MPSC_SERVER_BUF_SIZE);

    tokio::spawn(async move {        
        let mut clients = HashMap::new();

        loop {
            let msg = server_rx.recv().await.expect("clients msg receive failed");

            match msg {
                ServerMsg::AddClient(addr, net_addr, tx) => {
                    let client = Client { 
                        net_addr,
                        tx 
                    };

                    clients.insert(addr, client);
                }
                ServerMsg::SendBuf(addr, buf) => {
                    let client = clients.get(&addr);
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

    server_tx.send(ServerMsg::AddClient(auth_msg_meta.tx.clone(), client_net_addr, client_tx)).await.unwrap();    

    let mut adapter = socket_read.take(LEN_BUF_SIZE as u64);
    let mut state = State::new();

    let mut msg_meta = None;

    loop {
        let f1 = read(&mut state, &mut adapter).fuse();
        let f2 = client_rx.recv().fuse();

        pin_mut!(f1, f2);

        let res = select! {
            res = f1 => {                
                let res = res?;

                match res {
                    ReadResult::LenFinished => {
                        println!("len ok");
                    }
                    ReadResult::MsgMeta(new_msg_meta) => {
                        println!("{:?}", new_msg_meta);                                            
                        msg_meta = Some(new_msg_meta);
                    }
                    ReadResult::PayloadData(n, buf) => {
                        println!("payload data");
                    }
                    ReadResult::PayloadFinished => {
                        println!("payload ok");
                    }
                    ReadResult::AttachmentData(index, n, buf) => {
                        println!("attachment data");
                    }
                    ReadResult::AttachmentFinished(index) => {
                        println!("attachment ok");
                    }
                    ReadResult::MessageFinished => {
                        println!("message ok");
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
