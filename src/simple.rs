use std::{collections::HashMap, fmt::Debug};
use log::*;
use bytes::{Buf};
use ws::{Request, Builder, Handler, Sender, Message, Handshake, CloseCode};
use uuid::Uuid;
use crate::{AuthData, Config};
use crate::proto::{ClientKind, ServerMsg, ClientMsg, MsgMeta, MagicBall, MagicBall2};
use crate::error::Error;

struct WsServer {
    net_addr: Option<String>,
    auth_data: Option<AuthData>,
    ws: Sender,
    config: Config,
    tx: crossbeam::channel::Sender<ServerMsg>,
    client_kind: Option<ClientKind>,
    addr: Option<String>
}

impl Handler for WsServer {

    fn on_open(&mut self, hs: Handshake) -> ws::Result<()> {

        info!("got client {}", self.ws.connection_id());

        match hs.request.header("Service") {            
            Some(addr) => {
                self.client_kind = Some(ClientKind::Service);

                let addr = std::str::from_utf8(addr)?;
                self.addr = Some(addr.to_owned());
                self.tx.send(ServerMsg::AddClient(addr.to_owned(), self.ws.clone()));
            }
            None => {
                self.client_kind = Some(ClientKind::App);
            }
        }

        /*

        if let Some(cookie) = hs.request.header("Cookie") {
            match Cookie::parse_header(&cookie.to_vec().into()) {
                Ok(cookie_header) => {
                    self.auth_data = get_auth_data(Some(&cookie_header));
                    match self.auth_data {
                        None => {
                            info!("ws auth attempt failed, sending close.");
                            //self.ws.close(CloseCode::Normal);
                        }
                        _ => {}
                    }
                }
                Err(e) => error!("ws cookie parse error. {}", e)
            }
        }
        */

        if let Some(net_addr) = hs.remote_addr()? {
            self.net_addr = Some(net_addr.clone());
            info!("Connection with {} now open", net_addr);
        }        

        Ok(())
    }

    fn on_message(&mut self, msg: Message) -> ws::Result<()> {

        info!("got message");

        match &self.addr {
            Some(addr) => {
                match msg {
                    Message::Text(data) => {},
                    Message::Binary(data) => {
                        
                        let res = {
                            let mut buf = std::io::Cursor::new(&data);
                            let len = buf.get_u32_be() as usize;

                            serde_json::from_slice::<MsgMeta>(&data[4..len + 4])
                        };

                        match res {
                            Ok(msg_meta) => {
                                info!("Sending message: {:#?}", msg_meta);
                                self.tx.send(ServerMsg::SendMsg(msg_meta.rx, data));
                            }
                            Err(err) => {
                                error!("MsgMeta deserialization failed!")
                            }
                        }
                    }
                }
            }
            None => {
                info!("Client is unauthorized.");
            }
        }

        Ok(())
    }

    fn on_close(&mut self, code: CloseCode, reason: &str) {

        info!("closed");

        match code {

            CloseCode::Normal => {}//info!("The client is done with the connection."),

            CloseCode::Away   => {}//info!("The client is leaving the site."),

            _ => {}//info!("The client encountered an error: {}", reason),

        }

    }

    fn on_error(&mut self, err: ws::Error) {
        //info!("The server encountered an error: {:?}", err);
    }

}

pub fn start(host: String, port: u16, config: Config) {

    let (tx, rx) = crossbeam::channel::unbounded();

    let mut server = Builder::new().build(|ws| {

        WsServer {
            net_addr: None,
            auth_data: None,
            ws,
            config: config.clone(),
            tx: tx.clone(),
            client_kind: None,
            addr: None
        }

    }).unwrap();

    let clients = std::thread::Builder::new()
        .name("clients".to_owned())
        .spawn(move || {
            let mut clients = HashMap::new();            

            loop {
                let msg = rx.recv().unwrap();

                match msg {
                    ServerMsg::AddClient(addr, sender) => {
                        info!("Adding client {}", &addr);
                        clients.insert(addr, sender);                                
                    }
                    ServerMsg::SendMsg(addr, res) => {
                        match clients.get(&addr) {
                            Some(sender) => {
                                info!("Sending message to client {} {:?}", &addr, res);
                                sender.send(res);                                
                            }
                            None => {
                                info!("Client not found: {}", &addr);
                            }
                        }
                    }
                    _ => {}
                }                
            }
        })
        .unwrap();

    server.listen(format!("{}:{}", host, port));
}

struct WsClient {
    addr: Option<String>,
    out: Sender,
    events_tx: crossbeam::channel::Sender<(MsgMeta, usize, Vec<u8>)>,
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
        
        info!("Client got message {}", msg);

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

                        match msg_meta.correlation_id {

                            Some(correlation_id) => {

                                self.rpc_tx.send(ClientMsg::RpcDataRequest(correlation_id));

                                match self.rpc_rx.recv() {
                                    Ok(msg) => {
                                        match msg {
                                            ClientMsg::RpcDataResponse(received_correlation_id, rpc_tx) => {
                                                match received_correlation_id == correlation_id {
                                                    true => {
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

                            None => {
                                self.events_tx.send((msg_meta, len, data));
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
    let (rpc_tx, rpc_rx) = crossbeam::channel::unbounded();
    let (tx2, rx2) = crossbeam::channel::unbounded();

    let handle = std::thread::Builder::new()
        .name(addr.clone())
        .spawn(move || {
            ws::connect(host, |out| {
                tx2.send(MagicBall::new(addr.clone(), out.clone(), events_rx.clone()));

                WsClient {
                    addr: Some(addr.clone()),
                    out,
                    events_tx: events_tx.clone(),
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
                tx2.send(MagicBall2::new(addr.clone(), out.clone(), events_rx.clone(), rpc_tx3.clone()));

                WsClient {
                    addr: Some(addr.clone()),
                    out,
                    events_tx: events_tx.clone(),
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
