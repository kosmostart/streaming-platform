use std::collections::HashMap;
use std::future::Future;
use std::error::Error;
use log::*;
use futures::{select, pin_mut, future::FutureExt};
use bytes::{BytesMut, BufMut};
use tokio::runtime::Runtime;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::sync::mpsc::{self, UnboundedSender, UnboundedReceiver};
use tokio::time::{timeout, Elapsed};
use serde_json::{json, Value, from_slice, to_vec};
use sp_dto::*;
use crate::proto::*;

/// Future for stream based client based on provided config.
/// "addr" value will be used as address for endpoint, "host" value - network addr for the server (in host:port format)
/// "access_key" value will be send for optional authorization, more information about this feature will be provided later.
/// process_event is used for processing incoming message, which are marked as events via message kind.
/// process_rpc is used for processing incoming message, which are marked as rpc request via message kind.
/// startup is executed on the start of this function.
/// restream_rx can be used for restreaming data somewhere else, for example returning data for incoming web request
/// The protocol message format is in sp-dto crate.
pub async fn stream_mode<T: 'static, R: 'static>(host: &str, addr: &str, access_key: &str, process_stream: ProcessStream<T>, startup: Startup<R>, config: HashMap<String, String>, restream_rx: Option<UnboundedReceiver<RestreamMsg>>)
where 
    T: Future<Output = ()> + Send,
    R: Future<Output = ()> + Send
{    
    let (mut read_tx, mut read_rx) = mpsc::unbounded_channel();
    let (mut write_tx, mut write_rx) = mpsc::unbounded_channel();
    let (mut rpc_inbound_tx, mut rpc_inbound_rx) = mpsc::unbounded_channel();
    let (mut rpc_outbound_tx, mut _rpc_outbound_rx) = mpsc::unbounded_channel();
    let addr = addr.to_owned();
    let addr2 = addr.to_owned();   
    let addr3 = addr.to_owned();
    let access_key = access_key.to_owned();        
    let mut write_tx2 = write_tx.clone();    
    tokio::spawn(async move {
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
                            match rpc_outbound_tx.send(RpcMsg::RpcDataResponse(correlation_id, rpc_tx)) {
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
    let mut mb = MagicBall::new(addr2, write_tx2, rpc_inbound_tx);
    tokio::spawn(process_stream(config.clone(), mb.clone(), read_rx, restream_rx));
    tokio::spawn(startup(config, mb));
    connect_stream_future(host, addr3, access_key, read_tx, write_rx).await;
}

/// Future for message based client based on provided config.
/// "addr" value will be used as address for endpoint, "host" value - network addr for the server (in host:port format)
/// "access_key" value will be send for optional authorization, more information about this feature will be provided later.
/// process_stream is used for stream of incoming data processing.
/// startup is executed on the start of this function.
/// restream_rx can be used for restreaming data somewhere else, for example returning data for incoming web request
/// The protocol message format is in sp-dto crate.
pub async fn full_message_mode<P: 'static, T: 'static, Q: 'static, R: 'static>(host: &str, addr: &str, access_key: &str, process_event: ProcessEvent<T, P>, process_rpc: ProcessRpc<Q, P>, startup: Startup<R>, config: HashMap<String, String>)
where 
    T: Future<Output = Result<(), Box<dyn Error>>> + Send,
    Q: Future<Output = Result<Response<P>, Box<dyn Error>>> + Send,
    R: Future<Output = ()> + Send,
    P: serde::Serialize, for<'de> P: serde::Deserialize<'de> + Send
{    
    let (mut read_tx, mut read_rx) = mpsc::unbounded_channel();
    let (mut write_tx, mut write_rx) = mpsc::unbounded_channel();
    let (mut rpc_inbound_tx, mut rpc_inbound_rx) = mpsc::unbounded_channel();
    let (mut rpc_outbound_tx, mut rpc_outbound_rx) = mpsc::unbounded_channel();

    let addr = addr.to_owned();
    let addr2 = addr.to_owned();
    let addr3 = addr.to_owned();
    let access_key = access_key.to_owned();
    let mut rpc_inbound_tx2 = rpc_inbound_tx.clone();
    
    let mut write_tx2 = write_tx.clone();
    let mut write_tx3 = write_tx.clone();

    tokio::spawn(async move {
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
                            match rpc_outbound_tx.send(RpcMsg::RpcDataResponse(correlation_id, rpc_tx)) {
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

    tokio::spawn(async move {
        let mut mb = MagicBall::new(addr2, write_tx2, rpc_inbound_tx);        
        tokio::spawn(startup(config.clone(), mb.clone()));
        loop {                        
            let msg = match read_rx.recv().await {
                Some(msg) => msg,
                None => {
                    info!("client connection dropped");
                    break;
                }
            };
            let mut mb = mb.clone();
            let config = config.clone();
            let mut write_tx3 = write_tx3.clone();
            match msg {
                ClientMsg::Message(_, mut msg_meta, payload, attachments_data) => {
                    match msg_meta.kind {
                        MsgKind::Event => {          
                            debug!("client got event {}", msg_meta.display());
                            tokio::spawn(async move {
                                let payload: P = from_slice(&payload).expect("failed to deserialize event payload");                                
                                if let Err(e) = process_event(config, mb.clone(), Message { meta: msg_meta, payload, attachments_data }).await {
                                    error!("process event error {}", e);
                                }
                                debug!("client {} process_event succeeded", mb.addr);
                            });                            
                        }
                        MsgKind::RpcRequest => {                        
                            debug!("client got rpc request {}", msg_meta.display());
                            tokio::spawn(async move {                                
                                let mut route = msg_meta.route.clone();
                                let correlation_id = msg_meta.correlation_id;
                                let tx = msg_meta.tx.clone();
                                let key = msg_meta.key.clone();
                                let payload: P = from_slice(&payload).expect("failed to deserialize rpc request payload");                            
                                let (payload, attachments, attachments_data, rpc_result) = match process_rpc(config.clone(), mb.clone(), Message { meta: msg_meta, payload, attachments_data }).await {
                                    Ok(res) => {
                                        debug!("client {} process_rpc succeeded", mb.addr);
                                        let (res, attachments, attachments_data) = match res {
                                            Response::Simple(payload) => (payload, vec![], vec![]),
                                            Response::Full(payload, attachments, attachments_data) => (payload, attachments, attachments_data)
                                        };
                                        (to_vec(&res).expect("failed to serialize rpc process result"), attachments, attachments_data, RpcResult::Ok)
                                    }
                                    Err(e) =>  {
                                        error!("process rpc error {} {:?}", mb.addr.clone(), e);
                                        (to_vec(&json!({ "err": e.to_string() })).expect("failed to serialize rpc process error result"), vec![], vec![], RpcResult::Err)
                                    }
                                };                                
                                route.points.push(Participator::Service(mb.addr.clone()));
                                let (res, msg_meta_size, payload_size, attacchments_size) = reply_to_rpc_dto2_sizes(mb.addr.clone(), tx, key, correlation_id, payload, attachments, attachments_data, rpc_result, route, None, None).expect("failed to create rpc reply");
                                debug!("client {} attempt to write rpc response", mb.addr);
                                write(mb.get_stream_id(), res, msg_meta_size, payload_size, attacchments_size, &mut write_tx3).await.expect("failed to write rpc response");                                
                                debug!("client {} write rpc response succeded", mb.addr);
                            });                            
                        }
                        MsgKind::RpcResponse(_) => {           
                            debug!("client got rpc response {}", msg_meta.display());
                            match rpc_inbound_tx2.send(RpcMsg::RpcDataRequest(msg_meta.correlation_id)) {
                                Ok(()) => {
                                    debug!("client RpcDataRequest send succeeded {}", msg_meta.display());
                                }
                                Err(_) => panic!("rpc inbound tx2 msg send failed on rpc response")
                            }
                            let msg = rpc_outbound_rx.recv().await.expect("rpc outbound msg receive failed");                            

                            match msg {
                                RpcMsg::RpcDataResponse(received_correlation_id, rpc_tx) => {
                                    match received_correlation_id == msg_meta.correlation_id {
                                        true => {                                            
                                            match rpc_tx.send((msg_meta, payload, attachments_data)) {
                                                Ok(()) => {
                                                    debug!("client {} RpcDataResponse receive succeeded", mb.addr);
                                                }
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
    connect_full_message_future(host, addr3, access_key, read_tx, write_rx).await;
}

async fn auth(target: String, addr: String, access_key: String, stream: &mut TcpStream) -> Result<(), ProcessError> {
    let route = Route {
        source: Participator::Service(addr.clone()),
        spec: RouteSpec::Simple,
        points: vec![Participator::Service(addr.clone())]
    };  

    let (dto, msg_meta_size, payload_size, attachments_size) = rpc_dto_with_sizes(addr.clone(), target.to_owned(), "Auth".to_owned(), json!({
        "access_key": access_key
    }), route, None, None).unwrap();

    write_to_stream(get_stream_id_onetime(&addr), dto, msg_meta_size, payload_size, attachments_size, stream).await
}


async fn connect_stream_future(host: &str, addr: String, access_key: String, mut read_tx: UnboundedSender<ClientMsg>, mut write_rx: UnboundedReceiver<StreamUnit>) {    
    let server = "Server".to_owned();

    let mut write_stream = TcpStream::connect(host).await.expect("connection to host failed");
    auth(server.clone(), addr.clone(), access_key.clone(), &mut write_stream).await.expect("write stream authorization failed");

    let mut read_stream = TcpStream::connect(host).await.expect("connection to host failed");
    auth(server.clone(), addr.clone(), access_key, &mut read_stream).await.expect("read stream authorization failed");

    let res = process_message_stream(addr, write_stream, read_stream, read_tx, write_rx).await;

    info!("{:?}", res);
}

async fn connect_full_message_future(host: &str, addr: String, access_key: String, mut read_tx: UnboundedSender<ClientMsg>, mut write_rx: UnboundedReceiver<StreamUnit>) {    
    let server = "Server".to_owned();
    
    let mut write_stream = TcpStream::connect(host).await.expect("connection to host failed");
    auth(server.clone(), addr.clone(), access_key.clone(), &mut write_stream).await.expect("wrtie stream authorization failed");

    let mut read_stream = TcpStream::connect(host).await.expect("connection to host failed");
    auth(server.clone(), addr.clone(), access_key, &mut read_stream).await.expect("read stream authorization failed");

    let res = process_full_message(addr, write_stream, read_stream, read_tx, write_rx).await;

    info!("{:?}", res);
}

async fn process_message_stream(addr: String, mut write_stream: TcpStream, mut read_stream: TcpStream, mut read_tx: UnboundedSender<ClientMsg>, mut write_rx: UnboundedReceiver<StreamUnit>) -> Result<(), ProcessError> {    
    //let (auth_msg_meta, auth_payload, auth_attachments) = read_full(&mut socket_read).await?;
    //let auth_payload: Value = from_slice(&auth_payload)?;    

    //println!("auth {:?}", auth_msg_meta);
    //println!("auth {:?}", auth_payload);        
        
    let mut state = State::new(addr.clone());    

    tokio::spawn(async move {
        let res = write_loop(addr, write_rx, &mut write_stream).await;
        error!("{:?}", res);
    });

    loop {
        match read(&mut state, &mut read_stream).await? {
            ReadResult::MsgMeta(stream_id, msg_meta, _) => read_tx.send(ClientMsg::MsgMeta(stream_id, msg_meta))?,
            ReadResult::PayloadData(stream_id, n, buf) => read_tx.send(ClientMsg::PayloadData(stream_id, n, buf))?,
            ReadResult::PayloadFinished(stream_id, n, buf) => read_tx.send(ClientMsg::PayloadFinished(stream_id, n, buf))?,
            ReadResult::AttachmentData(stream_id, index, n, buf) => read_tx.send(ClientMsg::AttachmentData(stream_id, index, n, buf))?,
            ReadResult::AttachmentFinished(stream_id, index, n, buf) => read_tx.send(ClientMsg::AttachmentFinished(stream_id, index, n, buf))?,
            ReadResult::MessageFinished(stream_id, finish_bytes) => {
                match finish_bytes {
                    MessageFinishBytes::Payload(n, buf) => {
                        read_tx.send(ClientMsg::PayloadFinished(stream_id, n, buf))?;
                    }
                    MessageFinishBytes::Attachment(index, n, buf) => {
                        read_tx.send(ClientMsg::AttachmentFinished(stream_id, index, n, buf))?;
                    }                            
                }
                read_tx.send(ClientMsg::MessageFinished(stream_id))?;
            }
            ReadResult::MessageAborted(stream_id) => {
                read_tx.send(ClientMsg::MessageAborted(stream_id))?;
            }
        }
    }
}

async fn process_full_message(addr: String, mut write_stream: TcpStream, mut read_stream: TcpStream, mut read_tx: UnboundedSender<ClientMsg>, mut write_rx: UnboundedReceiver<StreamUnit>) -> Result<(), ProcessError> {    
    //let (auth_msg_meta, auth_payload, auth_attachments) = read_full(&mut socket_read).await?;
    //let auth_payload: Value = from_slice(&auth_payload)?;    

    //println!("auth {:?}", auth_msg_meta);
    //println!("auth {:?}", auth_payload);
            
    let mut stream_layouts = HashMap::new();
    let mut state = State::new(addr.clone());

    tokio::spawn(async move {
        let res = write_loop(addr, write_rx, &mut write_stream).await;
        error!("{:?}", res);
    });
    
    loop {
        match read(&mut state, &mut read_stream).await? {
            ReadResult::MsgMeta(stream_id, msg_meta, _) => {
                stream_layouts.insert(stream_id, StreamLayout {
                    id: stream_id,
                    msg_meta,
                    payload: vec![],
                    attachments_data: vec![]
                });
            }
            ReadResult::PayloadData(stream_id, n, buf) => {
                let stream_layout = stream_layouts.get_mut(&stream_id).ok_or(ProcessError::StreamLayoutNotFound)?;
                stream_layout.payload.extend_from_slice(&buf[..n]);

            }
            ReadResult::PayloadFinished(stream_id, n, buf) => {
                let stream_layout = stream_layouts.get_mut(&stream_id).ok_or(ProcessError::StreamLayoutNotFound)?;
                stream_layout.payload.extend_from_slice(&buf[..n]);
            }
            ReadResult::AttachmentData(stream_id, _, n, buf) => {
                let stream_layout = stream_layouts.get_mut(&stream_id).ok_or(ProcessError::StreamLayoutNotFound)?;
                stream_layout.attachments_data.extend_from_slice(&buf[..n]);
            }
            ReadResult::AttachmentFinished(stream_id, _, n, buf) => {
                let stream_layout = stream_layouts.get_mut(&stream_id).ok_or(ProcessError::StreamLayoutNotFound)?;
                stream_layout.attachments_data.extend_from_slice(&buf[..n]);
            }
            ReadResult::MessageFinished(stream_id, finish_bytes) => {
                let stream_layout = stream_layouts.get_mut(&stream_id).ok_or(ProcessError::StreamLayoutNotFound)?;
                match finish_bytes {
                    MessageFinishBytes::Payload(n, buf) => {
                        stream_layout.payload.extend_from_slice(&buf[..n]);
                    }
                    MessageFinishBytes::Attachment(_, n, buf) => {
                        stream_layout.attachments_data.extend_from_slice(&buf[..n]);
                    }                            
                }
                let stream_layout = stream_layouts.remove(&stream_id).ok_or(ProcessError::StreamLayoutNotFound)?;
                read_tx.send(ClientMsg::Message(stream_id, stream_layout.msg_meta, stream_layout.payload, stream_layout.attachments_data))?;
            }
            ReadResult::MessageAborted(stream_id) => {
                match stream_id {
                    Some(stream_id) => {
                        let _ = stream_layouts.remove(&stream_id).ok_or(ProcessError::StreamLayoutNotFound)?;
                    }
                    None => {}
                }
                read_tx.send(ClientMsg::MessageAborted(stream_id))?;
            }
        };
    }    
}

/// Starts a stream based client based on provided config. Creates new runtime and blocks.
/// Config must have "addr" key, this will be used as address for endpoint, and "host" key - network addr for the server (in host:port format)
/// Config must have "access_key" key, this will be send for optional authorization, more information about this feature will be provided later.
/// process_stream is used for stream of incoming data processing.
/// startup is executed on the start of this function.
/// restream_rx can be used for restreaming data somewhere else, for example returning data for incoming web request
/// The protocol message format is in sp-dto crate.
pub fn start_stream<T: 'static, R: 'static>(config: HashMap<String, String>, process_stream: ProcessStream<T>, startup: Startup<R>, restream_rx: Option<UnboundedReceiver<RestreamMsg>>) 
where 
    T: Future<Output = ()> + Send,
    R: Future<Output = ()> + Send
{        
    let addr = config.get("addr").expect("missing addr config value").to_owned();
    let host = config.get("host").expect("missing host config value").to_owned();    
    let access_key = config.get("access_key").expect("missing access_key config value").to_owned();
    let mut rt = Runtime::new().expect("failed to create runtime");
    rt.block_on(stream_mode(&host, &addr, &access_key, process_stream, startup, config, restream_rx));
}

/// Starts a message based client based on provided config. Creates new runtime and blocks.
/// Config must have "addr" key, this will be used as address for endpoint, and "host" key - network addr for the server (in host:port format)
/// process_event is used for processing incoming message, which are marked as events via message kind.
/// process_rpc is used for processing incoming message, which are marked as rpc request via message kind.
/// startup is executed on the start of this function.
/// The protocol message format is in sp-dto crate.
pub fn start<T: 'static, Q: 'static, R: 'static>(config: HashMap<String, String>, process_event: ProcessEvent<T, Value>, process_rpc: ProcessRpc<Q, Value>, startup: Startup<R>) 
where 
    T: Future<Output = Result<(), Box<dyn Error>>> + Send,
    Q: Future<Output = Result<Response<Value>, Box<dyn Error>>> + Send,
    R: Future<Output = ()> + Send
{    
    let addr = config.get("addr").expect("missing addr config value").to_owned();
    let host = config.get("host").expect("missing host config value").to_owned();
    let access_key = config.get("access_key").expect("missing access_key config value").to_owned();
    let mut rt = Runtime::new().expect("failed to create runtime");
    rt.block_on(full_message_mode(&host, &addr, &access_key, process_event, process_rpc, startup, config));
}