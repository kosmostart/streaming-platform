use std::error::Error;
use bytes::BufMut;
use futures::future::{Fuse, FusedFuture, FutureExt};
use futures::stream::StreamExt;
use futures::{select, pin_mut};
use tokio::fs::File;                  
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{self, Sender, Receiver};
use serde_json::{Value, from_slice, json};
use sp_dto::*;
use crate::proto::*;

pub enum ClientMsg {
    FileReceiveComplete(String)
}

pub async fn write(data: Vec<u8>, write_tx: &mut Sender<(usize, [u8; DATA_BUF_SIZE])>) -> Result<(), ProcessError> {
    let mut source = &data[..];

    loop {
        let mut data_buf = [0; DATA_BUF_SIZE];
        let n = source.read(&mut data_buf).await.unwrap();

        //println!("n {}, data_buf len {}", n, dto.len());

        match n {
            0 => break,
            _ => {
                write_tx.send((n, data_buf)).await.unwrap();
                //println!("write ok");
            }
        }
    }

    Ok(())
}

pub async fn connect(host: &str, addr: &str, access_key: &str, mut read_tx: Sender<ClientMsg>, mut write_rx: Receiver<(usize, [u8; DATA_BUF_SIZE])>) {    
    let addr = addr.to_owned();
    let access_key = access_key.to_owned();            

//pub async fn connect_future(host: String, mut read_tx: Sender<ClientMsg>, mut write_rx: Receiver<(usize, [u8; DATA_BUF_SIZE])>) {
    let mut stream = TcpStream::connect(host).await.unwrap();    

    let res = process(stream, read_tx, write_rx).await;

    println!("{:?}", res);
}

async fn process(mut stream: TcpStream, mut read_tx: Sender<ClientMsg>, mut write_rx: Receiver<(usize, [u8; DATA_BUF_SIZE])>) -> Result<(), ProcessError> {
    let (mut socket_read, mut socket_write) = stream.split();

    //let (auth_msg_meta, auth_payload, auth_attachments) = read_full(&mut socket_read).await?;
    //let auth_payload: Value = from_slice(&auth_payload)?;    

    //println!("auth {:?}", auth_msg_meta);
    //println!("auth {:?}", auth_payload);        

    let mut adapter = socket_read.take(LEN_BUF_SIZE as u64);
    let mut state = State::new();

    let mut msg_meta = None;
    let mut file = None;

    loop {
        let f1 = read(&mut state, &mut adapter).fuse();
        let f2 = write_rx.recv().fuse();

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

                        match new_msg_meta.key.as_ref() {
                            "Hub.GetFile" => {                                
                                let attachment = new_msg_meta.attachments.iter().nth(0).ok_or(ProcessError::GetFile(GetFileError::AttachmentsAreEmpty))?;
                                file = Some((attachment.name.clone(), File::create(&attachment.name).await?));                                
                            }
                            _ => file = None
                        }
                        
                        msg_meta = Some(new_msg_meta);
                    }
                    ReadResult::PayloadData(n, buf) => {
                        println!("payload data");
                    }
                    ReadResult::PayloadFinished => {
                        println!("payload ok");
                    }
                    ReadResult::AttachmentData(index, n, buf) => {                        
                        match file {
                            Some((_, ref mut file)) => file.write_all(&buf[..n]).await?,
                            None => {}
                        }                        
                    }
                    ReadResult::AttachmentFinished(index) => {                        
                        match file {
                            Some((name, _)) => {
                                read_tx.send(ClientMsg::FileReceiveComplete(name)).await?;
                                file = None;
                            }
                            None => {}
                        }                        
                    }
                    ReadResult::MessageFinished => {
                        println!("message ok");
                    }
                };                            
            }
            res = f2 => {                
                let (n, buf) = res?;                
                socket_write.write_all(&buf[..n]).await?;
                println!("send ok");
            }
        };
    }    

    Ok(())
}
