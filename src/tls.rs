use std::collections::HashMap;
use std::fs::File;
use std::rc::Rc;
use std::io::Read;
use log::*;
use ws::{Request, Builder, Settings, Handler, Sender, Message, Handshake, CloseCode};
use ws::util::TcpStream;
use openssl::ssl::{SslAcceptor, SslAcceptorBuilder, SslMethod, SslStream, SslConnectorBuilder, SslConnector, SslVerifyMode};
use openssl::pkey::PKey;
use openssl::x509::{X509, X509Ref};
use crate::{AuthData, Config};
use crate::proto::Route;
use crate::error::Error;

struct WssServer {
    net_addr: Option<String>,
    auth_data: Option<AuthData>,
    ws: Sender,
    route_msg: fn(Config, String, AuthData) -> Route,
    ssl: Rc<SslAcceptor>,
    config: Config,
    addr: Option<String>
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

pub fn connect_tls(host: String) {
    ws::connect(host, |out| {        
        WssClient { 
            out            
        }
    });
}

pub fn route_message(config: Config, data: String, auth_data: AuthData) -> Route {
    Route::ToSender("Error".to_string())
}
