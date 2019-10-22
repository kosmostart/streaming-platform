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

pub fn connect(addr: &str) {
    let rt = Runtime::new().expect("failed to create runtime"); 
    
    rt.block_on(connect_future(addr));
}

pub async fn connect_future(addr: &str) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    
    /*    
    let route = Route {
        source: Participator::Service(addr.to_owned()),
        spec: RouteSpec::Simple,
        points: vec![Participator::Service(addr.to_owned())]
    };  

    let rpc_dto = rpc_dto(addr.to_owned(), addr.to_owned(), "key".to_owned(), json!({
    }), route).unwrap();

    if let Err(err) = socket_write.write_all(&rpc_dto).await {
        println!("failed to write to socket {:?}", err);
        return;
    }

    if let Err(err) = socket_write.write_all(&rpc_dto).await {
        println!("failed to write to socket {:?}", err);
        return;
    }

    println!("write ok");
    */
    let (mut read_tx, mut read_rx) = mpsc::channel(MPSC_CLIENT_BUF_SIZE);
    let (mut write_tx, mut write_rx) = mpsc::channel(MPSC_CLIENT_BUF_SIZE);

    let route = Route {
        source: Participator::Service(addr.to_owned()),
        spec: RouteSpec::Simple,
        points: vec![Participator::Service(addr.to_owned())]
    };  

    let dto = rpc_dto(addr.to_owned(), addr.to_owned(), "Auth".to_owned(), json!({
        "access_key": "12345"
    }), route).unwrap();

    println!("dto len {}", dto.len());

    let mut dto2 = &dto[..];

    loop {
        let mut data_buf = [0; DATA_BUF_SIZE];
        let n = dto2.read(&mut data_buf).await.unwrap();

        println!("n {}, data_buf len {}", n, dto.len());

        match n {
            0 => break,
            _ => {
                write_tx.send((n, data_buf)).await.unwrap();
                println!("write ok");
            }
        }
    }

    let route = Route {
        source: Participator::Service(addr.to_owned()),
        spec: RouteSpec::Simple,
        points: vec![Participator::Service(addr.to_owned())]
    };  

    let dto = rpc_dto(addr.to_owned(), addr.to_owned(), "Hub.GetFile".to_owned(), json!({        
    }), route).unwrap();

    println!("dto len {}", dto.len());

    let mut dto2 = &dto[..];

    loop {
        let mut data_buf = [0; DATA_BUF_SIZE];
        let n = dto2.read(&mut data_buf).await.unwrap();

        println!("n {}, data_buf len {}", n, dto.len());

        match n {
            0 => break,
            _ => {
                write_tx.send((n, data_buf)).await.unwrap();
                println!("write ok");
            }
        }
    }
    
    let res = process(stream, read_tx, write_rx).await;

    println!("{:?}", res);
}

async fn process(mut stream: TcpStream, mut read_tx: Sender<[u8; DATA_BUF_SIZE]>, mut write_rx: Receiver<(usize, [u8; DATA_BUF_SIZE])>) -> Result<(), ProcessError> {
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
                                file = Some(File::create(&attachment.name).await?);
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
                            Some(ref mut file) => {
                                file.write_all(&buf[..n]).await?;
                            }
                            None => {

                            }
                        }                                                                    
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
                println!("send ok");
            }
        };
    }    

    Ok(())
}
