use std::collections::HashMap;
use std::future::Future;
use std::error::Error;
use log::*;
use futures::{select, pin_mut, future::FutureExt};
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{self, Sender, Receiver};
use serde_json::{json, Value, from_slice, to_vec};
use sp_dto::*;
use crate::proto::*;

pub fn stream_mode<T: 'static, Q: 'static>(host: &str, addr: &str, access_key: &str, process_stream_msg: ProcessStreamMsg<T>, startup: StreamStartup<Q>, config: HashMap<String, String>)
where 
    T: Future<Output = ()> + Send,
    Q: Future<Output = ()> + Send
{
    let mut rt = Runtime::new().expect("failed to create runtime");    

    let (mut read_tx, mut read_rx) = mpsc::channel(MPSC_CLIENT_BUF_SIZE);
    let (mut write_tx, mut write_rx) = mpsc::channel(MPSC_CLIENT_BUF_SIZE);
    let (mut rpc_inbound_tx, mut rpc_inbound_rx) = mpsc::channel(MPSC_RPC_BUF_SIZE);
    let (mut rpc_outbound_tx, mut rpc_outbound_rx) = mpsc::channel(MPSC_RPC_BUF_SIZE);

    let addr = addr.to_owned();
    let addr2 = addr.to_owned();
    let addr3 = addr.to_owned();
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
                            match rpc_outbound_tx.send(RpcMsg::RpcDataResponse(correlation_id, rpc_tx)).await {
                                Ok(()) => {}
                                Err(_) => panic!("rpc outbound tx send failed on rpc data request")
                            }
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
    });

    rt.spawn(async move {
        tokio::spawn(startup(config.clone()));

        loop {
            let msg = read_rx.recv().await.expect("connection issues acquired");
            tokio::spawn(process_stream_msg(msg));
        }    
    });

    rt.block_on(connect_stream_future(host, addr3, read_tx, write_rx));    
}

pub fn full_message_raw_mode<T: 'static, Q: 'static, R: 'static>(host: &str, addr: &str, access_key: &str, process_event: ProcessEventRaw<T>, process_rpc: ProcessRpcRaw<Q>, startup: Startup<R>, config: HashMap<String, String>)
where 
    T: Future<Output = Result<(), Box<dyn Error>>> + Send,
    Q: Future<Output = Result<ResponseRaw, Box<dyn Error>>> + Send,
    R: Future<Output = ()> + Send
{
    let mut rt = Runtime::new().expect("failed to create runtime");    

    let (mut read_tx, mut read_rx) = mpsc::channel(MPSC_CLIENT_BUF_SIZE);
    let (mut write_tx, mut write_rx) = mpsc::channel(MPSC_CLIENT_BUF_SIZE);
    let (mut rpc_inbound_tx, mut rpc_inbound_rx) = mpsc::channel(MPSC_RPC_BUF_SIZE);
    let (mut rpc_outbound_tx, mut rpc_outbound_rx) = mpsc::channel(MPSC_RPC_BUF_SIZE);

    let addr = addr.to_owned();
    let addr2 = addr.to_owned();
    let addr3 = addr.to_owned();
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
                            match rpc_outbound_tx.send(RpcMsg::RpcDataResponse(correlation_id, rpc_tx)).await {
                                Ok(()) => {}
                                Err(_) => panic!("rpc outbound tx send failed on rpc data request")
                            }
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
    });

    rt.spawn(async move {
        let mut mb = MagicBall::new(addr2, write_tx2, rpc_inbound_tx);

        tokio::spawn(startup(config.clone(), mb.clone()));

        loop {
            let msg = read_rx.recv().await.expect("connection issues acquired");
            let mut mb = mb.clone();
            let config = config.clone();
            let mut write_tx3 = write_tx3.clone();
            match msg {
                ClientMsg::Message(mut msg_meta, payload, attachments_data) => {
                    match msg_meta.kind {
                        MsgKind::Event => {
                            tokio::spawn(async move {
                                if let Err(e) = process_event(config.clone(), mb.clone(), MessageRaw { meta: msg_meta, payload, attachments_data }).await {
                                    error!("process event error {} {:?}", mb.get_addr(), e);
                                }
                            });
                        }                                                                            
                        MsgKind::RpcRequest => {
                            tokio::spawn(async move {
                                let mut route = msg_meta.route.clone();
                                let correlation_id = msg_meta.correlation_id;
                                let tx = msg_meta.tx.clone();
                                let key = msg_meta.key.clone();
                                let (payload, attachments, attachments_data) = match process_rpc(config.clone(), mb.clone(), MessageRaw { meta: msg_meta, payload, attachments_data }).await {
                                    Ok(res) => {
                                        let (payload, attachments, attachments_data) = match res {
                                            ResponseRaw::Simple(payload) => (payload, vec![], vec![]),
                                            ResponseRaw::Full(payload, attachments, attachments_data) => (payload, attachments, attachments_data)
                                        };
                                        (payload, attachments, attachments_data)
                                    }
                                    Err(e) => {
                                        error!("process rpc error {} {:?}", mb.get_addr(), e);
                                        let payload = to_vec(&json!({
                                            "e": e.to_string()
                                        })).expect("failed to serialize rpc process error");

                                        (payload, vec![], vec![])
                                    }
                                };
                                route.points.push(Participator::Service(mb.get_addr()));
                                let res = reply_to_rpc_dto2(mb.get_addr(), tx, key, correlation_id, payload, attachments, attachments_data, route).expect("failed to create rpc reply");
                                write(res, &mut write_tx3).await.expect("failed to write rpc response");
                            });                            
                        }
                        MsgKind::RpcResponse => {
                            match rpc_inbound_tx2.send(RpcMsg::RpcDataRequest(msg_meta.correlation_id)).await {
                                Ok(()) => {}
                                Err(_) => panic!("rpc inbound tx2 send failed")
                            }

                            let msg = rpc_outbound_rx.recv().await.expect("rpc outbound msg receive failed");

                            match msg {
                                RpcMsg::RpcDataResponse(received_correlation_id, rpc_tx) => {
                                    match received_correlation_id == msg_meta.correlation_id {
                                        true => {                                            
                                            match rpc_tx.send((msg_meta, payload, attachments_data)) {
                                                Ok(()) => {}
                                                Err((msg_meta, _, _)) => error!("rpc_tx send failed on rpc response {:?}", msg_meta)
                                            }
                                        }
                                        false => error!("received_correlation_id not equals correlation_id: {}, {}", received_correlation_id, msg_meta.correlation_id)
                                    }
                                }
                                _ => error!("Client handler: wrong RpcMsg")
                            }                                
                        }
                    }
                }
                _ => {}
            }
        }    
    });

    rt.block_on(connect_full_message_future(host, addr3, read_tx, write_rx));    
}

pub fn full_message_mode<P: 'static, T: 'static, Q: 'static, R: 'static>(host: &str, addr: &str, access_key: &str, process_event: ProcessEvent<T, P>, process_rpc: ProcessRpc<Q, P>, startup: Startup<R>, config: HashMap<String, String>)
where 
    T: Future<Output = Result<(), Box<dyn Error>>> + Send,
    Q: Future<Output = Result<Response<P>, Box<dyn Error>>> + Send,
    R: Future<Output = ()> + Send,
    P: serde::Serialize, for<'de> P: serde::Deserialize<'de> + Send
{
    let mut rt = Runtime::new().expect("failed to create runtime");    

    let (mut read_tx, mut read_rx) = mpsc::channel(MPSC_CLIENT_BUF_SIZE);
    let (mut write_tx, mut write_rx) = mpsc::channel(MPSC_CLIENT_BUF_SIZE);
    let (mut rpc_inbound_tx, mut rpc_inbound_rx) = mpsc::channel(MPSC_RPC_BUF_SIZE);
    let (mut rpc_outbound_tx, mut rpc_outbound_rx) = mpsc::channel(MPSC_RPC_BUF_SIZE);

    let addr = addr.to_owned();
    let addr2 = addr.to_owned();
    let addr3 = addr.to_owned();
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
                    //info!("add rpc ok {}", correlation_id);
                }                
                RpcMsg::RpcDataRequest(correlation_id) => {
                    match rpcs.remove(&correlation_id) {
                        Some(rpc_tx) => {
                            match rpc_outbound_tx.send(RpcMsg::RpcDataResponse(correlation_id, rpc_tx)).await {
                                Ok(()) => {}
                                Err(_) => panic!("rpc outbound tx send failed on rpc data request")
                            }
                            //info!("send rpc response ok {}", correlation_id);
                        }
                        None => error!("send rpc response not found {}", correlation_id)
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
    });

    rt.spawn(async move {
        let mut mb = MagicBall::new(addr2, write_tx2, rpc_inbound_tx);        
        tokio::spawn(startup(config.clone(), mb.clone()));
        loop {                        
            let msg = read_rx.recv().await.expect("connection issues acquired");
            let mut mb = mb.clone();
            let config = config.clone();
            let mut write_tx3 = write_tx3.clone();
            match msg {
                ClientMsg::Message(mut msg_meta, payload, attachments_data) => {
                    match msg_meta.kind {
                        MsgKind::Event => {                            
                            tokio::spawn(async move {
                                let payload: P = from_slice(&payload).expect("failed to deserialize event payload");                                
                                if let Err(e) = process_event(config, mb, Message { meta: msg_meta, payload, attachments_data }).await {
                                    error!("process event error {}", e);
                                }                                
                            });                            
                        }
                        MsgKind::RpcRequest => {                            
                            tokio::spawn(async move {                                
                                let mut route = msg_meta.route.clone();
                                let correlation_id = msg_meta.correlation_id;
                                let tx = msg_meta.tx.clone();
                                let key = msg_meta.key.clone();
                                let payload: P = from_slice(&payload).expect("failed to deserialize rpc request payload");                            
                                let (payload, attachments, attachments_data) = match process_rpc(config.clone(), mb.clone(), Message { meta: msg_meta, payload, attachments_data }).await {
                                    Ok(res) => {
                                        let (res, attachments, attachments_data) = match res {
                                            Response::Simple(payload) => (payload, vec![], vec![]),
                                            Response::Full(payload, attachments, attachments_data) => (payload, attachments, attachments_data)
                                        };
                                        (to_vec(&res).expect("failed to serialize rpc process result"), attachments, attachments_data)
                                    }
                                    Err(e) =>  {
                                        error!("process rpc error {} {:?}", mb.get_addr(), e);
                                        (to_vec(&json!({ "err": e.to_string() })).expect("failed to serialize rpc process error result"), vec![], vec![])
                                    }
                                };                                
                                route.points.push(Participator::Service(mb.get_addr()));
                                let res = reply_to_rpc_dto2(mb.get_addr(), tx, key, correlation_id, payload, attachments, attachments_data, route).expect("failed to create rpc reply");
                                write(res, &mut write_tx3).await.expect("failed to write rpc response");                                
                            });                            
                        }
                        MsgKind::RpcResponse => {                                                        
                            match rpc_inbound_tx2.send(RpcMsg::RpcDataRequest(msg_meta.correlation_id)).await {
                                Ok(()) => {}
                                Err(_) => panic!("rpc inbound tx2 msg send failed on rpc response")
                            }
                            let msg = rpc_outbound_rx.recv().await.expect("rpc outbound msg receive failed");                            

                            match msg {
                                RpcMsg::RpcDataResponse(received_correlation_id, rpc_tx) => {
                                    match received_correlation_id == msg_meta.correlation_id {
                                        true => {                                            
                                            match rpc_tx.send((msg_meta, payload, attachments_data)) {
                                                Ok(()) => {}
                                                Err((msg_meta, _, _)) => error!("rpc_tx send failed on rpc response {:?}", msg_meta)
                                            }
                                        }
                                        false => error!("received_correlation_id not equals correlation_id: {}, {}", received_correlation_id, msg_meta.correlation_id)
                                    }
                                }
                                _ => error!("Client handler: wrong RpcMsg")
                            }                                
                        }
                    }
                }
                _ => {}
            }
        }    
    });

    rt.block_on(connect_full_message_future(host, addr3, read_tx, write_rx));
}

pub async fn connect_stream_future(host: &str, addr: String, mut read_tx: Sender<ClientMsg>, mut write_rx: Receiver<(usize, [u8; DATA_BUF_SIZE])>) {    
    let mut stream = TcpStream::connect(host).await.expect("onnection to host failed");

    let res = process_stream(addr, stream, read_tx, write_rx).await;

    println!("{:?}", res);
}

pub async fn connect_full_message_future(host: &str, addr: String, mut read_tx: Sender<ClientMsg>, mut write_rx: Receiver<(usize, [u8; DATA_BUF_SIZE])>) {    
    let mut stream = TcpStream::connect(host).await.expect("onnection to host failed");

    let res = process_full_message(addr, stream, read_tx, write_rx).await;

    println!("{:?}", res);
}

async fn process_stream(addr: String, mut stream: TcpStream, mut read_tx: Sender<ClientMsg>, mut write_rx: Receiver<(usize, [u8; DATA_BUF_SIZE])>) -> Result<(), ProcessError> {
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

async fn process_full_message(addr: String, mut stream: TcpStream, mut read_tx: Sender<ClientMsg>, mut write_rx: Receiver<(usize, [u8; DATA_BUF_SIZE])>) -> Result<(), ProcessError> {
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
                //info!("client fm f1 {}", addr);
                let (msg_meta, payload, attachments) = res?;
                //info!("client fm f1 {} {:?}", addr, msg_meta);
                read_tx.send(ClientMsg::Message(msg_meta, payload, attachments)).await?;
                //info!("client fm f1 ok");
            }
            res = f2 => {             
                //info!("client fm f2 {}", addr);   
                let (n, buf) = res?;                
                socket_write.write_all(&buf[..n]).await?;                
                //info!("client fm f2 ok {} {}", n, addr);   
            }
        };
    }    
}
