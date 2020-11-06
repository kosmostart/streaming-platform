#![feature(proc_macro_hygiene)]
#![feature(async_closure)]
use std::collections::HashMap;
use std::net::SocketAddr;
use log::*;
use serde_json::{json, Value, to_vec};
use warp::{Filter, http::{Response, header::SET_COOKIE}};
use streaming_platform::{MagicBall, tokio::{io::AsyncReadExt}};
use streaming_platform::sp_dto::{Message, resp};
use streaming_platform::sp_dto::MsgMeta;
use sp_auth::verify_auth_token;

mod authorize;
mod hub;

pub async fn process_event(_config: HashMap<String, String>, mut _mb: MagicBall, _msg: Message<Value>) -> Result<(), Box<dyn std::error::Error>>  {
    Ok(())
}

pub async fn process_rpc(_config: HashMap<String, String>, mut _mb: MagicBall, _msg: Message<Value>) -> Result<streaming_platform::sp_dto::Response<Value>, Box<dyn std::error::Error>> {    
    resp(json!({}))    
}

pub async fn startup(config: HashMap<String, String>, mb: MagicBall, _startup_data: Option<Value>) {
    let listen_addr = config.get("listen_addr").expect("missing listen_addr config value");    
    let cert_path = config.get("cert_path");
    let key_path = config.get("key_path");
    let aca_origin = config.get("aca_origin").map(|x| x.to_owned());
    let aca_origin2 = aca_origin.clone();
    let mb2 = mb.clone();

    let listen_addr = listen_addr.parse::<SocketAddr>().expect("incorrect listen addr passed");

    let auth_key = config.get("auth_key").map(|x| x.to_owned()).unwrap();

    let routes =                    
        warp::path("authorize")
            .and(warp::post())                
            .and(warp::body::bytes())
            .and_then(move |body: warp::hyper::body::Bytes| {
                let aca_origin = aca_origin.clone();
                let mb = mb.clone();
                
                crate::authorize::go(aca_origin, body, mb)
            }
        )
        .or(
            warp::path("app")
                .and(warp::path::param())
                .and(warp::header::optional("cookie"))
                .and(warp::path::end())                                
                .map(move |_prm: String, _cookie_header: Option<String>| {

                    Response::builder()
                        .header("content-type", "text/html")
                        .body("")
                }
            )
        )
        .or(
            warp::path("app")
                .and(warp::path::param())
                .and(warp::header::optional("cookie"))
                .and(warp::path::param())                                
                .and_then(move |_prm: String, _cookie_header: Option<String>, _prm2: String| {

                    async move {
                        let mut file = streaming_platform::tokio::fs::File::open("").await.unwrap();

                        let mut file_buf = [0; 1024];

                        let _stream = loop {
                            match file.read(&mut file_buf).await.unwrap() {
                                0 => break,
                                _n => {

                                }
                            }
                        };                                

                        //let body = hyper::Body::wrap_stream(stream);

                        let res = Response::builder().body(vec![]).expect("failed to build response");
                        Ok::<Response<Vec<u8>>, warp::Rejection>(res)
                    }
                }
            )
        )
        .or(
            warp::path("hub")
                .and(warp::post())                
                .and(warp::body::bytes())
                .and_then(move |body: warp::hyper::body::Bytes| {
                    let aca_origin = aca_origin2.clone();
                    let auth_key = auth_key.clone();
                    let mb = mb2.clone();
                    
                    crate::hub::go(aca_origin, auth_key, body, mb)
                }
            )  
        )
        ;



    if cert_path.is_some() && key_path.is_some() {
        warp::serve(routes)
        .tls()
        .cert_path(cert_path.unwrap())
        .key_path(key_path.unwrap())
        .run(listen_addr)
        .await;	
    } else {
        warp::serve(routes)        
        .run(listen_addr)
        .await;
    }
}

pub fn response(aca_origin: String, data: Vec<u8>) -> Result<Response<Vec<u8>>, warp::http::Error> {
    Response::builder()
        .header("Access-Control-Allow-Credentials", "true")
        .header("Access-Control-Allow-Origin", aca_origin)        
        .body(data) 
}

pub fn response_with_cookie(aca_origin: String, cookie_header: &str, data: Vec<u8>) -> Result<Response<Vec<u8>>, warp::http::Error> {
    Response::builder()
        .header("Access-Control-Allow-Credentials", "true")
        .header("Access-Control-Allow-Origin", aca_origin)        
        .header(SET_COOKIE, cookie_header)
        .body(data) 
}

pub fn check_auth_token_vec(auth_key: &[u8], msg_meta: &MsgMeta) -> Result<Value, Vec<u8>> {
    let res = match &msg_meta.auth_token {
        Some(data) => {
            match verify_auth_token(auth_key, &data) {
                Ok(auth_data) => Some(auth_data),
                Err(e) => {
                    warn!("auth token verification failed, {:?}", e);
                    None
                }
            }
        }
        None => None
    };

    match res {
        Some(auth_data) => Ok(auth_data),
        None => Err(to_vec("here comes the error").expect("failed to serialize check_cookie_vec error response"))        
    }
}