use futures::{select, pin_mut};
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{self, Sender, Receiver};
use serde_json::json;
use sp_dto::*;
use crate::proto::*;

pub async fn connect_future(host: &str, mut read_tx: Sender<ClientMsg>, mut write_rx: Receiver<(usize, [u8; DATA_BUF_SIZE])>) {    
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
                    ReadResult::PayloadData(n, buf) => {
                        println!("payload data");
                    }
                    ReadResult::PayloadFinished => {
                        println!("payload ok");
                    }
                    ReadResult::AttachmentData(index, n, buf) => {                        
                    }
                    ReadResult::AttachmentFinished(index) => {           
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
}

pub fn magic_ball(host: &str, addr: &str, access_key: &str) {
    let rt = Runtime::new().expect("failed to create runtime");

    let (mut read_tx, mut read_rx) = mpsc::channel(MPSC_CLIENT_BUF_SIZE);
    let (mut write_tx, mut write_rx) = mpsc::channel(MPSC_CLIENT_BUF_SIZE);

    let addr = addr.to_owned();
    let access_key = access_key.to_owned();

    let mut write_tx = write_tx.clone();

    rt.spawn(async move {        
        let target = "SvcHub";

        let route = Route {
            source: Participator::Service(addr.clone()),
            spec: RouteSpec::Simple,
            points: vec![Participator::Service(addr.clone())]
        };  

        let dto = rpc_dto(addr.clone(), target.to_owned(), "Auth".to_owned(), json!({
            "access_key": access_key
        }), route).unwrap();

        let res = write(dto, &mut write_tx).await;
        println!("{:?}", res);

        let route = Route {
            source: Participator::Service(addr.clone()),
            spec: RouteSpec::Simple,
            points: vec![Participator::Service(addr.clone())]
        };  

        let dto = rpc_dto(addr.to_owned(), target.to_owned(), "Hub.GetFile".to_owned(), json!({        
        }), route).unwrap();

        let res = write(dto, &mut write_tx).await;
        println!("{:?}", res);
    });

    rt.spawn(async move {
        loop {
            let msg = read_rx.recv().await.expect("connection issues acquired");
            tokio::spawn(async move {
                println!("async task");
            });
            match msg {
                ClientMsg::FileReceiveComplete(name) => {                    
                }
            }
        }    
    });

    rt.block_on(connect_future(host, read_tx, write_rx));
}
