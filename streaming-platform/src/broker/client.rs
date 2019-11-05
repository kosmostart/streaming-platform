use std::collections::HashMap;
use futures::{select, pin_mut};
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{self, Sender, Receiver};
use serde_json::{json, Value, from_slice, to_vec};
use sp_dto::*;
use crate::proto::*;

pub fn magic_ball(host: &str, addr: &str, access_key: &str, mode: Mode, config: HashMap<String, String>) {
    let mut rt = Runtime::new().expect("failed to create runtime");

    let (mut read_tx, mut read_rx) = mpsc::channel(MPSC_CLIENT_BUF_SIZE);
    let (mut write_tx, mut write_rx) = mpsc::channel(MPSC_CLIENT_BUF_SIZE);
    let (mut rpc_inbound_tx, mut rpc_inbound_rx) = mpsc::channel(MPSC_RPC_BUF_SIZE);
    let (mut rpc_outbound_tx, mut rpc_outbound_rx) = mpsc::channel(MPSC_RPC_BUF_SIZE);

    let addr = addr.to_owned();
    let addr2 = addr.to_owned();
    let access_key = access_key.to_owned();
    let mut rpc_inbound_tx2 = rpc_inbound_tx.clone();
    
    let mut write_tx2 = write_tx.clone();
    let mut write_tx3 = write_tx.clone();

    rt.spawn(async move {
        let mut rpcs = HashMap::new();        

        loop {
            let msg = rpc_inbound_rx.recv().await.expect("rpc inbound msg receive failed");

            match msg {
                RpcMsg::AddRpc(correlation_id, rpc_tx) => {
                    rpcs.insert(correlation_id, rpc_tx);
                }                
                RpcMsg::RpcDataRequest(correlation_id) => {
                    match rpcs.remove(&correlation_id) {
                        Some(rpc_tx) => {
                            rpc_outbound_tx.send(RpcMsg::RpcDataResponse(correlation_id, rpc_tx));
                        }
                        None => {                            
                        }
                    }                        
                }
                _=> {                    
                }
            }
        }
    });

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

    match mode {
        Mode::Stream(process_msg) => {
            rt.spawn(async move {
                loop {
                    let msg = read_rx.recv().await.expect("connection issues acquired");                 
                    process_msg(msg);
                }    
            });
        }
        Mode::FullMessage(process_event, proccess_rpc_request) => {
            rt.spawn(async move {
                let mut mb = MagicBall::new(addr2, write_tx2, rpc_inbound_tx);

                loop {
                    let msg = read_rx.recv().await.expect("connection issues acquired");
                    match msg {
                        ClientMsg::Message(mut msg_meta, payload, attachments) => {
                            match msg_meta.kind {
                                MsgKind::Event => {
                                    process_event(&config, &mut mb, &msg_meta, payload, attachments);
                                }
                                MsgKind::RpcRequest => {
                                    let (payload, attachments, attachments_data) = proccess_rpc_request(&config, &mut mb, &msg_meta, payload, attachments);
                                    msg_meta.route.points.push(Participator::Service(mb.get_addr()));
                                    let res = reply_to_rpc_dto2(mb.get_addr(), msg_meta.tx, msg_meta.key, msg_meta.correlation_id, payload, attachments, attachments_data, msg_meta.route).expect("failed to create rpc reply");
                                    write(res, &mut write_tx3).await.expect("failed to write rpc response");
                                }
                                MsgKind::RpcResponse => {
                                    rpc_inbound_tx2.send(RpcMsg::RpcDataRequest(msg_meta.correlation_id));

                                    let msg = rpc_outbound_rx.recv().await.expect("rpc outbound msg receive failed");

                                    match msg {
                                        RpcMsg::RpcDataResponse(received_correlation_id, rpc_tx) => {
                                            match received_correlation_id == msg_meta.correlation_id {
                                                true => {
                                                    //debug!("Sending to rpc_tx {:?}", msg_meta);
                                                    rpc_tx.send((msg_meta, payload, attachments));
                                                }
                                                false => {
                                                    //error!("received_correlation_id not equals correlation_id: {}, {}", received_correlation_id, msg_meta.correlation_id);
                                                }
                                            }
                                        }
                                        _ => {
                                            //error!("Client handler: wrong RpcMsg");
                                        }
                                    }                                
                                }
                            }
                        }
                        _ => {}
                    }
                }    
            });
        }
        Mode::FullMessageSimple(process_event, proccess_rpc_request) => {
            rt.spawn(async move {
                let mut mb = MagicBall::new(addr2, write_tx2, rpc_inbound_tx);

                loop {
                    let msg = read_rx.recv().await.expect("connection issues acquired");
                    match msg {
                        ClientMsg::Message(mut msg_meta, payload, attachments) => {
                            match msg_meta.kind {
                                MsgKind::Event => {
                                    let payload: Value = from_slice(&payload).expect("failed to deserialize event payload");
                                    process_event(&config, &mut mb, &msg_meta, payload, attachments);
                                }
                                MsgKind::RpcRequest => {
                                    let payload: Value = from_slice(&payload).expect("failed to deserialize rpc request payload");
                                    let payload = to_vec(&proccess_rpc_request(&config, &mut mb, &msg_meta, payload, attachments)).expect("failed to serialize rpc process result");
                                    msg_meta.route.points.push(Participator::Service(mb.get_addr()));
                                    let res = reply_to_rpc_dto2(mb.get_addr(), msg_meta.tx, msg_meta.key, msg_meta.correlation_id, payload, vec![], vec![], msg_meta.route).expect("failed to create rpc reply");
                                    write(res, &mut write_tx3).await.expect("failed to write rpc response");
                                }
                                MsgKind::RpcResponse => {
                                    rpc_inbound_tx2.send(RpcMsg::RpcDataRequest(msg_meta.correlation_id));

                                    let msg = rpc_outbound_rx.recv().await.expect("rpc outbound msg receive failed");

                                    match msg {
                                        RpcMsg::RpcDataResponse(received_correlation_id, rpc_tx) => {
                                            match received_correlation_id == msg_meta.correlation_id {
                                                true => {
                                                    //debug!("Sending to rpc_tx {:?}", msg_meta);
                                                    rpc_tx.send((msg_meta, payload, attachments));
                                                }
                                                false => {
                                                    //error!("received_correlation_id not equals correlation_id: {}, {}", received_correlation_id, msg_meta.correlation_id);
                                                }
                                            }
                                        }
                                        _ => {
                                            //error!("Client handler: wrong RpcMsg");
                                        }
                                    }                                
                                }
                            }
                        }
                        _ => {}
                    }
                }    
            });
        }
    }    

    rt.block_on(connect_future(host, mode, read_tx, write_rx));
}

pub async fn connect_future(host: &str, mode: Mode, mut read_tx: Sender<ClientMsg>, mut write_rx: Receiver<(usize, [u8; DATA_BUF_SIZE])>) {    
    let mut stream = TcpStream::connect(host).await.unwrap();    

    let res = match mode {
        Mode::Stream(_) => process_stream(stream, read_tx, write_rx).await,
        Mode::FullMessage(_, _) | 
        Mode::FullMessageSimple(_, _) => process_full_message(stream, read_tx, write_rx).await
    };
    println!("{:?}", res);
}

async fn process_stream(mut stream: TcpStream, mut read_tx: Sender<ClientMsg>, mut write_rx: Receiver<(usize, [u8; DATA_BUF_SIZE])>) -> Result<(), ProcessError> {
    let (mut socket_read, mut socket_write) = stream.split();

    //let (auth_msg_meta, auth_payload, auth_attachments) = read_full(&mut socket_read).await?;
    //let auth_payload: Value = from_slice(&auth_payload)?;    

    //println!("auth {:?}", auth_msg_meta);
    //println!("auth {:?}", auth_payload);        

    let mut adapter = socket_read.take(LEN_BUF_SIZE as u64);
    let mut state = State::new();    

    loop {
        let f1 = read(&mut state, &mut adapter).fuse();
        let f2 = write_rx.recv().fuse();

        pin_mut!(f1, f2);

        let res = select! {
            res = f1 => {                
                let res = res?;                

                match res {
                    ReadResult::LenFinished(_) => {}
                    ReadResult::MsgMeta(new_msg_meta, _) => read_tx.send(ClientMsg::MsgMeta(new_msg_meta)).await?,
                    ReadResult::PayloadData(n, buf) => read_tx.send(ClientMsg::PayloadData(n, buf)).await?,
                    ReadResult::PayloadFinished => read_tx.send(ClientMsg::PayloadFinished).await?,
                    ReadResult::AttachmentData(index, n, buf) => read_tx.send(ClientMsg::AttachmentData(index, n, buf)).await?,
                    ReadResult::AttachmentFinished(index) => read_tx.send(ClientMsg::AttachmentFinished(index)).await?,
                    ReadResult::MessageFinished => read_tx.send(ClientMsg::MessageFinished).await?
                };                            
            }
            res = f2 => {                
                let (n, buf) = res?;                
                socket_write.write_all(&buf[..n]).await?;                
            }
        };
    }
}

async fn process_full_message(mut stream: TcpStream, mut read_tx: Sender<ClientMsg>, mut write_rx: Receiver<(usize, [u8; DATA_BUF_SIZE])>) -> Result<(), ProcessError> {
    let (mut socket_read, mut socket_write) = stream.split();

    //let (auth_msg_meta, auth_payload, auth_attachments) = read_full(&mut socket_read).await?;
    //let auth_payload: Value = from_slice(&auth_payload)?;    

    //println!("auth {:?}", auth_msg_meta);
    //println!("auth {:?}", auth_payload);

    let mut state = State::new();    

    loop {
        let f1 = read_full(&mut socket_read).fuse();
        let f2 = write_rx.recv().fuse();

        pin_mut!(f1, f2);

        let res = select! {
            res = f1 => {                
                let (msg_meta, payload, attachments) = res?;
                read_tx.send(ClientMsg::Message(msg_meta, payload, attachments)).await?;                
            }
            res = f2 => {                
                let (n, buf) = res?;                
                socket_write.write_all(&buf[..n]).await?;                
            }
        };
    }    
}
