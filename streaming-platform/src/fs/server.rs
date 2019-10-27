use std::fmt;
use std::error::Error;
use std::collections::HashMap;
use std::io::prelude::*;
use std::io::{Cursor, BufReader};
use std::fs::{self, DirEntry};
use std::path::Path;
use std::net::SocketAddr;
use bytes::Buf;
use futures::future::{Fuse, FusedFuture, FutureExt};
use futures::stream::StreamExt;
use futures::{select, pin_mut};
use tokio::runtime::Runtime;
use tokio::io::Take;
use tokio::net::{TcpListener, TcpStream, tcp::split::{ReadHalf, WriteHalf}};
use tokio::sync::mpsc::{self, Sender, Receiver};
use tokio::prelude::*;
use tokio::fs::File;
use serde_json::{Value, from_slice};
use serde_derive::Deserialize;
use sp_dto::*;
use crate::proto::*;

async fn process(mut stream: TcpStream, client_net_addr: SocketAddr, mut server_tx: Sender<ServerMsg>, config: &Config) -> Result<(), ProcessError> {
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
                        
                        let res = process_msg(&new_msg_meta, &auth_payload, &mut socket_write, config).await?;
                        println!("process {:?}", res);
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

    Ok(())
}

async fn process_msg(msg_meta: &MsgMeta, payload: &Value, socket_write: &mut WriteHalf<'_>, config: &Config) -> Result<(), ProcessError> {
    match msg_meta.key.as_ref() {
        "Hub.GetFile" => {
            let access_key = payload["access_key"].as_str().ok_or(ProcessError::GetFile(GetFileError::NoAccessKeyInPayload))?;
            let dirs = config.dirs.as_ref().ok_or(ProcessError::GetFile(GetFileError::ConfigDirsIsEmpty))?;
            let target_dir = dirs.iter().find(|x| x.access_key == access_key).ok_or(ProcessError::GetFile(GetFileError::TargetDirNotFound))?;
            let path = fs::read_dir(&target_dir.path)?.nth(0).ok_or(ProcessError::GetFile(GetFileError::NoFilesInTargetDir))??.path();        

            if path.is_file() {                
                let mut file_buf = [0; 1024];
                let mut file = File::open(&path).await?;
                let size = file.metadata().await?.len();

                let reply_dto = reply_to_rpc_dto_with_later_attachments2("SvcHub".to_owned(), msg_meta.tx.clone(), msg_meta.key.clone(), msg_meta.correlation_id, vec![],
                vec![
                    (path.file_name()
                        .ok_or(ProcessError::GetFile(GetFileError::FileNameIsEmpty))?
                        .to_str()
                        .ok_or(ProcessError::GetFile(GetFileError::FileNameIsEmpty))?
                        .to_owned()
                    , size)
                ], msg_meta.route.clone())?;

                socket_write.write_all(&reply_dto).await?;                

                loop {
                    match file.read(&mut file_buf).await? {
                        0 => break,
                        n => socket_write.write_all(&file_buf[..n]).await?
                    }
                }                
            } else {
                println!("not a file my friends");
            }
        }
        _ => {}
    }    

    Ok(())
}
