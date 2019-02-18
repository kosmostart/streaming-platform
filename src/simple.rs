use std::collections::HashMap;
use std::fs::File;
use std::rc::Rc;
use std::io::Read;
use log::*;
use bytes::{Buf, BufMut};
use ws::{Request, Builder, Settings, Handler, Sender, Message, Handshake, CloseCode};
use ws::util::TcpStream;
use crate::{AuthData, Config};
use crate::proto::{ClientKind, ServerMsg, MsgMeta, MagicBall};
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
                                self.tx.send(ServerMsg::SendMsg(msg_meta.addr, data));
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

struct WsClient<T> where T: serde::Serialize, for<'de> T: serde::Deserialize<'de> {
    addr: Option<String>,
    out: Sender,
    tx: crossbeam::channel::Sender<(MsgMeta, T)>
}

impl<T> Handler for WsClient<T> where T: serde::Serialize, for<'de> T: serde::Deserialize<'de> {
    fn on_open(&mut self, _: Handshake) -> ws::Result<()> {
        Ok(()) 
    }

    fn on_message(&mut self, msg: Message) -> ws::Result<()> {
        info!("Client got message '{}'. ", msg);
        //self.out.send("Hello");

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

pub fn connect(addr: String, host: String, tx: crossbeam::channel::Sender<(MsgMeta, Vec<u8>)>) -> Result<(std::thread::JoinHandle<()>, MagicBall), Error> {
    let (tx2, rx2) = crossbeam::channel::unbounded();

    let handle = std::thread::Builder::new()
        .name(addr.clone())
        .spawn(move || {
            ws::connect(host, |out| {
                tx2.send(MagicBall::new(out.clone()));

                WsClient {
                    addr: Some(addr.clone()),
                    out,
                    tx: tx.clone()
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
