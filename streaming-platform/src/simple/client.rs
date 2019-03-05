use std::{collections::HashMap, fmt::Debug};
use log::*;
use sp_dto::bytes::Buf;
use ws::{Request, Builder, Handler, Sender, Message, Handshake, CloseCode};
use sp_dto::uuid::Uuid;
use cookie::Cookie;
use sp_dto::{MsgMeta, MsgKind};
use crate::{AuthData, Config};
use crate::proto::{ClientKind, ServerMsg, ClientMsg, MagicBall, MagicBall2};
use crate::error::Error;

struct WsClient {
    addr: Option<String>,
    out: Sender,
    events_tx: crossbeam::channel::Sender<(MsgMeta, usize, Vec<u8>)>,
    rpc_request_tx: crossbeam::channel::Sender<(MsgMeta, usize, Vec<u8>)>,
    //RequestRpc(Uuid),    
    rpc_tx: crossbeam::channel::Sender<ClientMsg>,
    //Rpc(Uuid, crossbeam::channel::Sender<(MsgMeta, usize, Vec<u8>)>)
    rpc_rx: crossbeam::channel::Receiver<ClientMsg>
}

impl Handler for WsClient {
    fn on_open(&mut self, _: Handshake) -> ws::Result<()> {
        Ok(()) 
    }

    fn on_message(&mut self, msg: Message) -> ws::Result<()> {
        
        info!("Client got message");

        match msg {
            Message::Text(data) => {},
            Message::Binary(data) => {

                let len = {
                    let mut buf = std::io::Cursor::new(&data);
                    buf.get_u32_be() as usize
                };

                match serde_json::from_slice::<MsgMeta>(&data[4..len + 4]) {
                    Ok(msg_meta) => {
                        info!("Received message, {:#?}", msg_meta);

                        match msg_meta.kind {
                            MsgKind::Event => {
                                self.events_tx.send((msg_meta, len, data));
                            }
                            MsgKind::RpcRequest => {
                                match msg_meta.correlation_id {
                                    Some(correlation_id) => {
                                        self.rpc_request_tx.send((msg_meta, len, data));
                                    }
                                    None => error!("Missing correlation id for RpcRequest msg {:?}", msg_meta)
                                }                                                                
                            }
                            MsgKind::RpcResponse => {
                                match msg_meta.correlation_id {

                                    Some(correlation_id) => {

                                        self.rpc_tx.send(ClientMsg::RpcDataRequest(correlation_id));

                                        match self.rpc_rx.recv() {
                                            Ok(msg) => {
                                                info!("Debugging {:?}", msg);
                                                match msg {
                                                    ClientMsg::RpcDataResponse(received_correlation_id, rpc_tx) => {
                                                        match received_correlation_id == correlation_id {
                                                            true => {
                                                                info!("Sending to rpc_tx {:?}", msg_meta);
                                                                rpc_tx.send((msg_meta, len, data));
                                                            }
                                                            false => error!("received_correlation_id not equals correlation_id: {}, {}", received_correlation_id, correlation_id)
                                                        }
                                                    }
                                                    _ => error!("Client handler: wrong ClientMsg")
                                                }
                                            }
                                            Err(err) => error!("Error on self.rpc_rx.recv(): {:?}", err)
                                        }
                                    }
                                    None => error!("Missing correlation id for RpcResponse msg {:?}", msg_meta)
                                }
                            }

                        }
                                                            
                    }
                    Err(err) => {
                        error!("Error on message deserialization: {:?}", err);
                    }
                }
                
            }
        }

        Ok(())
    }

    fn build_request(&mut self, url: &url::Url) -> ws::Result<Request> {
        let mut req = Request::from_url(url)?;

        match &self.addr {
            Some(addr) => {
                req.headers_mut().push(("Service".into(), addr.clone().into()));
            }
            None => {
                info!("Client with empty addr.");
            }
        }        

        Ok(req)
    }
}

pub fn connect<T, R>(addr: String, host: String) -> Result<(std::thread::JoinHandle<()>, MagicBall<T, R>), Error> where T: Debug, T: Send, T: 'static, T: serde::Serialize, for<'de> T: serde::Deserialize<'de>, R: Debug, R: Send, R: 'static, R: serde::Serialize, for<'de> R: serde::Deserialize<'de> {

    let (events_tx, events_rx) = crossbeam::channel::unbounded();
    let (rpc_request_tx, rpc_request_rx) = crossbeam::channel::unbounded();
    let (rpc_tx, rpc_rx) = crossbeam::channel::unbounded();
    let rpc_rx2 = rpc_rx.clone();
    let rpc_tx2 = rpc_tx.clone();
    let rpc_tx3 = rpc_tx.clone();
    let (tx2, rx2) = crossbeam::channel::unbounded();

    let rpc = std::thread::Builder::new()
        .name("rpc".to_owned())
        .spawn(move || {
            let mut rpcs = HashMap::new();            

            //rpcs: HashMap<Uuid, crossbeam::channel::Sender<(MsgMeta, usize, Vec<u8>)>>

            loop {
                let msg = rpc_rx2.recv().unwrap();

                match msg {
                    ClientMsg::AddRpc(correlation_id, rpc_client_tx) => {
                        info!("Adding rpc {}", correlation_id);
                        rpcs.insert(correlation_id, rpc_client_tx);
                    }
                    ClientMsg::RemoveRpc(correlation_id) => {
                        rpcs.remove(&correlation_id);
                    }
                    ClientMsg::RpcDataRequest(correlation_id) => {
                        match rpcs.get(&correlation_id) {
                            Some(rpc_client_tx) => {
                                rpc_tx2.send(ClientMsg::RpcDataResponse(correlation_id, rpc_client_tx.clone()));
                            }
                            None => error!("Rpc not found: {}", correlation_id)
                        }                        
                    }
                    _ => error!("Rpcs: wrong ClientMsg")
                }
            }
        })
        .unwrap();

    let handle = std::thread::Builder::new()
        .name(addr.clone())
        .spawn(move || {
            ws::connect(host, |out| {
                tx2.send(MagicBall::new(addr.clone(), out.clone(), events_rx.clone(), rpc_request_rx.clone(), rpc_tx3.clone()));

                WsClient {
                    addr: Some(addr.clone()),
                    out,
                    events_tx: events_tx.clone(),
                    rpc_request_tx: rpc_request_tx.clone(),
                    rpc_tx: rpc_tx.clone(),
                    rpc_rx: rpc_rx.clone()
                }
            });
        })?;

    let sender = rx2.recv()?;

    Ok((handle, sender))
}

pub fn connect2(addr: String, host: String) -> Result<(std::thread::JoinHandle<()>, MagicBall2), Error> {

    let (events_tx, events_rx) = crossbeam::channel::unbounded();
    let (rpc_request_tx, rpc_request_rx) = crossbeam::channel::unbounded();
    let (rpc_tx, rpc_rx) = crossbeam::channel::unbounded();
    let rpc_rx2 = rpc_rx.clone();
    let rpc_tx2 = rpc_tx.clone();
    let rpc_tx3 = rpc_tx.clone();
    let (tx2, rx2) = crossbeam::channel::unbounded();

    let rpc = std::thread::Builder::new()
        .name("rpc".to_owned())
        .spawn(move || {
            let mut rpcs = HashMap::new();            

            //rpcs: HashMap<Uuid, crossbeam::channel::Sender<(MsgMeta, usize, Vec<u8>)>>

            loop {
                let msg = rpc_rx2.recv().unwrap();

                info!("Debugging {:?}", msg);

                match msg {
                    ClientMsg::AddRpc(correlation_id, rpc_client_tx) => {
                        info!("Adding rpc {}", correlation_id);
                        rpcs.insert(correlation_id, rpc_client_tx);
                    }
                    ClientMsg::RemoveRpc(correlation_id) => {
                        rpcs.remove(&correlation_id);
                    }
                    ClientMsg::RpcDataRequest(correlation_id) => {
                        match rpcs.get(&correlation_id) {
                            Some(rpc_client_tx) => {
                                rpc_tx2.send(ClientMsg::RpcDataResponse(correlation_id, rpc_client_tx.clone()));
                                info!("rpc_client_tx sent {}", correlation_id);
                            }
                            None => error!("Rpc not found: {}", correlation_id)
                        }                        
                    }
                    _ => error!("Rpcs: wrong ClientMsg")
                }
            }
        })
        .unwrap();

    let handle = std::thread::Builder::new()
        .name(addr.clone())
        .spawn(move || {
            ws::connect(host, |out| {
                tx2.send(MagicBall2::new(addr.clone(), out.clone(), events_rx.clone(), rpc_request_rx.clone(), rpc_tx3.clone()));

                WsClient {
                    addr: Some(addr.clone()),
                    out,
                    events_tx: events_tx.clone(),
                    rpc_request_tx: rpc_request_tx.clone(),
                    rpc_tx: rpc_tx.clone(),
                    rpc_rx: rpc_rx.clone()
                }
            });
        })?;

    let sender = rx2.recv()?;

    Ok((handle, sender))
}

#[test]
fn test_scenarios() {
    let server = std::thread::Builder::new()
        .name("server".to_owned())
        .spawn(|| {
            start("0.0.0.0".to_owned(), 60000, Config {})
        })
        .unwrap();

    let host = "ws://127.0.0.1:60000";

    let (tx, rx) = crossbeam::channel::unbounded();

    let (handle, sender) = connect("hello".to_owned(), host.to_owned(), tx).unwrap();

    handle.join().unwrap();
}
