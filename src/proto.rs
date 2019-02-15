use log::*;
use std::collections::HashMap;
use std::fs::File;
use std::rc::Rc;
use std::io::Read;
use ws::{Request, Builder, Settings, Handler, Sender, Message, Handshake, CloseCode};
use ws::util::TcpStream;
use openssl::ssl::{SslAcceptor, SslAcceptorBuilder, SslMethod, SslStream, SslConnectorBuilder, SslConnector, SslVerifyMode};
use openssl::pkey::PKey;
use openssl::x509::{X509, X509Ref};
use crate::{AuthData, Config};

/*
pub fn get_auth_data(cookie_header: Option<&Cookie>) -> Option<AuthData> {

    info!("Cookie header: {:?}", cookie_header);

    match cookie_header {

        Some(ref cookie) => {

            match cookie.get("solarinc-token") {

                Some(received_token) => {

                    match verify(received_token) {
                        Ok(d) => return Some(d),
                        Err(e) => error!("token::verify error {}", e)
                    }


                }

                None => info!("Failed auth attempt")

            }

        }

        None => info!("Failed auth attempt")

    }

    None
}
*/

struct WssServer {
    net_addr: Option<String>,
    auth_data: Option<AuthData>,
    ws: Sender,
    route_msg: fn(Config, String, AuthData) -> Route,
    ssl: Rc<SslAcceptor>,
    config: Config,
    addr: Option<String>
}

struct WsServer {
    net_addr: Option<String>,
    auth_data: Option<AuthData>,
    ws: Sender,
    route_msg: fn(Config, String, AuthData) -> Route,
    config: Config,
    tx: crossbeam::channel::Sender<ServerMsg>,
    addr: Option<String>
}

enum ServerMsg {
    AddClient(String, Sender),
    RemoveClient(String),
    SendMsg(String, Message)
}

pub enum Route {
    ToSender(String),
    ToClient(String, String),
    Broadcast(String),
    Disconnect
}

impl Handler for WsServer {

    fn on_open(&mut self, hs: Handshake) -> ws::Result<()> {

        info!("got client {}", self.ws.connection_id());

        match hs.request.header("Service") {
            Some(addr) => {
                let addr = std::str::from_utf8(addr)?;
                match addr {
                    "ReportService" => {
                        self.addr = Some(addr.to_owned());
                        self.tx.send(ServerMsg::AddClient(addr.to_owned(), self.ws.clone()));
                    }
                    _ => {}
                }
            }
            None => {}
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

        match msg {
            Message::Text(data) => {

                let route_msg = self.route_msg;

                match route_msg(self.config.clone(), data, AuthData {}) {
                    Route::ToSender(res) => {
                        self.ws.send(res);
                    }
                    Route::ToClient(addr, res) => {
                        self.tx.send(ServerMsg::SendMsg(addr, Message::Text(res)));
                    }
                    Route::Broadcast(res) => {
                        self.ws.broadcast(res);
                    }
                    Route::Disconnect => {}                            
                }
            },
            Message::Binary(data) => {}
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

impl Handler for WssServer {

    fn on_open(&mut self, hs: Handshake) -> ws::Result<()> {        

        /*
        if let Some(cookie) = hs.request.header("Cookie") {
            match Cookie::parse_header(&cookie.to_vec().into()) {
                Ok(cookie_header) => {
                    self.auth_data = get_auth_data(Some(&cookie_header));
                    match self.auth_data {
                        None => {
                            info!("ws auth attempt failed, sending close.");
                            self.ws.close(CloseCode::Normal);
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
            //info!("Connection with {} now open", net_addr);
        }

        //self.ws.send("hi".to_string());

        Ok(())
    }

    fn on_message(&mut self, msg: Message) -> ws::Result<()> {

        match self.auth_data {
            Some(ref d) => {
                match msg {
                    Message::Text(data) => {

                        let route_msg = self.route_msg;

                        match route_msg(self.config.clone(), data, d.clone()) {
                            Route::ToSender(res) => {
                                self.ws.send(res);
                            }
                            Route::ToClient(client, res) => {
                                self.ws.send(res);
                            }
                            Route::Broadcast(res) => {
                                self.ws.broadcast(res);
                            }
                            Route::Disconnect => {}
                        }
                    },
                    Message::Binary(data) => {}
                }
            }
            None => {}

        }

        Ok(())
    }

    fn on_close(&mut self, code: CloseCode, reason: &str) {

        info!("closed {}", self.ws.connection_id());

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

fn read_file(name: &str) -> std::io::Result<Vec<u8>> {
    let mut file = File::open(name)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    Ok(buf)
}

pub fn start(host: String, port: u16, route_msg: fn(Config, String, AuthData) -> Route, config: Config, tx: crossbeam::channel::Sender<Message>) {

    let (tx, rx) = crossbeam::channel::unbounded();

    let mut server = Builder::new().build(|ws| {

        WsServer {
            net_addr: None,
            auth_data: None,
            ws,
            route_msg,
            config: config.clone(),
            tx: tx.clone(),
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

pub fn start_tls(host: String, port: u16, route_msg: fn(Config, String, AuthData) -> Route, config: Config) {
    let settings = Settings::default();

    let cert = {
        let data = read_file("sample.pem").unwrap();
        X509::from_pem(data.as_ref()).unwrap()
    };

    let pkey = {
        let data = read_file("sample.rsa").unwrap();
        PKey::private_key_from_pem(data.as_ref()).unwrap()
    };

    let acceptor = Rc::new({
        let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
        builder.set_private_key(&pkey).unwrap();
        builder.set_certificate(&cert).unwrap();

        builder.build()
    });

    let mut server = Builder::new().with_settings(settings).build(|ws| {

        WssServer {
            net_addr: None,
            auth_data: None,
            ws,
            route_msg,
            ssl: acceptor.clone(),
            config: config.clone(),
            addr: None
        }

    }).unwrap();

    server.listen(format!("{}:{}", host, port));
}

struct WssClient {
    out: ws::Sender,
}

impl ws::Handler for WssClient {
    fn on_open(&mut self, _: Handshake) -> ws::Result<()> {
        Ok(()) 
    }

    fn on_message(&mut self, msg: ws::Message) -> ws::Result<()> {
        info!("Client got message '{}'. ", msg);
        //self.out.send("Hello");

        Ok(())
    }

    fn upgrade_ssl_client(
        &mut self,
        sock: TcpStream,
        _: &url::Url,
    ) -> ws::Result<SslStream<TcpStream>> {
        let mut builder = SslConnector::builder(SslMethod::tls()).map_err(|e| {
            ws::Error::new(
                ws::ErrorKind::Internal,
                format!("Failed to upgrade client to SSL: {}", e),
            )
        })?;
        builder.set_verify(SslVerifyMode::empty());

        let connector = builder.build();
        connector
            .configure()
            .unwrap()
            .use_server_name_indication(false)
            .verify_hostname(false)
            .connect("", sock)
            .map_err(From::from)
    }
}

struct WsClient {
    out: Sender,
    tx: crossbeam::channel::Sender<Message>
}

impl Handler for WsClient {
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

        req.headers_mut().push(("Service".into(), "ReportService".into()));

        Ok(req)
    }
}

pub fn connect_tls(host: String) {
    ws::connect(host, |out| {        
        WssClient { 
            out            
        }
    });
}

pub fn connect(thread_name: String, host: String, tx: crossbeam::channel::Sender<Message>) -> Result<(std::thread::JoinHandle<()>, Sender), Error> {    
    let (tx1, rx) = crossbeam::channel::unbounded();

    let handle = std::thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            ws::connect(host, |out| {                
                tx1.send(out.clone());
                WsClient { 
                    out,
                    tx: tx.clone()
                }
            });
        })?;

    let sender = rx.recv()?;

    Ok((handle, sender))
}

#[derive(Debug)]
pub enum Error {    
	Io(std::io::Error),
    ChannelReceive(crossbeam::channel::RecvError)
}

impl From<std::io::Error> for Error {
	fn from(err: std::io::Error) -> Error {
		Error::Io(err)
	}
}

impl From<crossbeam::channel::RecvError> for Error {
	fn from(err: crossbeam::channel::RecvError) -> Error {
		Error::ChannelReceive(err)
	}
}

pub fn route_message(config: Config, data: String, auth_data: AuthData) -> Route {
    Route::ToSender("Error".to_string())
}

#[test]
fn test_scenarios() {
    let server = std::thread::Builder::new()
        .name("server".to_owned())
        .spawn(|| {
            start("0.0.0.0".to_owned(), 60000, route_message, Config {})
        })
        .unwrap();

    let host = "ws://127.0.0.1:60000";

    let (tx, rx) = crossbeam::channel::unbounded();

    let (handle, sender) = connect("hello".to_owned(), host.to_owned(), tx).unwrap();

    handle.join().unwrap();
}

/*
pub fn route_example_path(payload: ExampleDomain) -> Response {
    match payload {
        ExampleDomain::ExamplePath(payload) => WsResponse::Search("asd".to_string())
    }
}
*/
