use std::fs::File;
use std::rc::Rc;
use std::io::Read;
use ws::{Builder, Settings, Handler, Sender, Message, Handshake, CloseCode};
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
    addr: Option<String>,
    auth_data: Option<AuthData>,
    ws: Sender,
    route_msg: fn(Config, String, AuthData) -> Route,
    ssl: Rc<SslAcceptor>,
    config: Config
}

struct WsServer {
    addr: Option<String>,
    auth_data: Option<AuthData>,
    ws: Sender,
    route_msg: fn(Config, String, AuthData) -> Route,
    config: Config
}

pub enum Route {
    ToSender(String),
    ToClient(String, String),
    Broadcast(String),
    Disconnect
}

impl Handler for WsServer {

    fn on_open(&mut self, hs: Handshake) -> ws::Result<()> {

        println!("got client");

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

        if let Some(addr) = hs.remote_addr()? {
            self.addr = Some(addr.clone());
            //info!("Connection with {} now open", addr);
        }

        let q = self.ws.clone();

        q.send("hi".to_string());

        println!("sent hi");

        Ok(())
    }

    fn on_message(&mut self, msg: Message) -> ws::Result<()> {

        println!("got message");

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
            None => {
                match msg {
                    Message::Text(data) => {

                        let route_msg = self.route_msg;

                        match route_msg(self.config.clone(), data, AuthData {}) {
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

        }

        Ok(())
    }

    fn on_close(&mut self, code: CloseCode, reason: &str) {

        println!("closed");

        match code {

            CloseCode::Normal => {}//println!("The client is done with the connection."),

            CloseCode::Away   => {}//println!("The client is leaving the site."),

            _ => {}//println!("The client encountered an error: {}", reason),

        }

    }

    fn on_error(&mut self, err: ws::Error) {
        //println!("The server encountered an error: {:?}", err);
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

        if let Some(addr) = hs.remote_addr()? {
            self.addr = Some(addr.clone());
            //println!("Connection with {} now open", addr);
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

        //println!("closed");

        match code {

            CloseCode::Normal => {}//println!("The client is done with the connection."),

            CloseCode::Away   => {}//println!("The client is leaving the site."),

            _ => {}//println!("The client encountered an error: {}", reason),

        }

    }

    fn on_error(&mut self, err: ws::Error) {
        //println!("The server encountered an error: {:?}", err);
    }

}

fn read_file(name: &str) -> std::io::Result<Vec<u8>> {
    let mut file = File::open(name)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    Ok(buf)
}

pub fn start(host: String, port: u16, route_msg: fn(Config, String, AuthData) -> Route, config: Config) {

    let mut server = Builder::new().build(|ws| {

        WsServer {
            addr: None,
            auth_data: None,
            ws,
            route_msg,
            config: config.clone()
        }

    }).unwrap();

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
            addr: None,
            auth_data: None,
            ws,
            route_msg,
            ssl: acceptor.clone(),
            config: config.clone()
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
        println!("Client got message '{}'. ", msg);
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
        println!("Client got message '{}'. ", msg);
        //self.out.send("Hello");

        Ok(())
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
        .spawn(move || {
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
