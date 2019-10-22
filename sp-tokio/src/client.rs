use std::error::Error;
use bytes::BufMut;
use futures::future::{Fuse, FusedFuture, FutureExt};
use futures::stream::StreamExt;
use futures::{select, pin_mut};
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
    let mut stream = TcpStream::connect("127.0.0.1:60000").await.unwrap();
    
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
        
}

async fn process(mut stream: TcpStream, mut read_tx: Sender<String>, mut write_rx: Receiver<String>) -> Result<(), ProcessError> {
    let (mut socket_read, mut socket_write) = stream.split();

    let (auth_msg_meta, auth_payload, auth_attachments) = read_full(&mut socket_read).await?;
    let auth_payload: Value = from_slice(&auth_payload)?;    

    println!("auth {:?}", auth_msg_meta);
    println!("auth {:?}", auth_payload);        

    let mut adapter = socket_read.take(LEN_BUF_SIZE as u64);
    let mut state = State::new();

    let mut msg_meta = None;

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
                        msg_meta = Some(new_msg_meta);
                    }
                    ReadResult::PayloadData(buf) => {
                        println!("payload data");
                    }
                    ReadResult::PayloadFinished => {
                        println!("payload ok");
                    }
                    ReadResult::AttachmentData(index, buf) => {
                        println!("attachment data");
                    }
                    ReadResult::AttachmentFinished(index) => {
                        println!("attachment ok");
                    }
                };                            
            }
            res = f2 => {

            }
        };
    }    

    Ok(())
}
