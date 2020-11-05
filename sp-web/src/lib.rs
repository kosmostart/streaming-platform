#![feature(proc_macro_hygiene)]
#![feature(async_closure)]
use std::collections::HashMap;
use std::io::prelude::*;
use std::net::SocketAddr;
use std::env::current_dir;
use log::*;
use serde_json::{json, Value, to_vec, to_string, from_value, from_slice};
use cookie::Cookie;
use warp::{hyper, Filter, path, http::{Response, StatusCode, header::SET_COOKIE}, reply::{Reply, with::header}, query, fs::{dir, File}, http::header::HeaderValue};
use streaming_platform::{MagicBall, client::stream_mode, ClientMsg, StreamCompletion, tokio::{self, io::AsyncReadExt, sync::{mpsc::{self, UnboundedReceiver, UnboundedSender}, oneshot}}, RestreamMsg, StreamLayout};
use streaming_platform::{futures::{self, Future, stream::{Stream, StreamExt}}};
use streaming_platform::sp_dto::reply_to_rpc_dto2_sizes;
use streaming_platform::sp_dto::{Message, resp, rpc_dto_with_correlation_id_sizes, Route, RouteSpec};
use streaming_platform::sp_dto::{MsgMeta, get_msg_meta, get_payload, MsgKind, uuid::Uuid, get_msg, get_msg_meta_and_payload, reply_to_rpc_dto, Participator, RpcResult};
use sp_auth::verify_auth_token;

mod authorize;
mod hub;

pub async fn process_event(config: HashMap<String, String>, mut mb: MagicBall, msg: Message<Value>) -> Result<(), Box<dyn std::error::Error>>  {
    Ok(())
}

pub async fn process_rpc(config: HashMap<String, String>, mut mb: MagicBall, msg: Message<Value>) -> Result<streaming_platform::sp_dto::Response<Value>, Box<dyn std::error::Error>> {    
    resp(json!({}))    
}

pub async fn startup(config: HashMap<String, String>, mut mb: MagicBall, startup_data: Option<Value>) {
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
                let mut mb = mb.clone();
                
                crate::authorize::go(aca_origin, body, mb)
            }
        )
        .or(
            warp::path("app")
                .and(warp::path::param())
                .and(warp::header::optional("cookie"))
                .and(warp::path::end())                                
                .map(move |prm: String, cookie_header: Option<String>| {

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
                .and_then(move |prm: String, cookie_header: Option<String>, prm2: String| {

                    async move {
                        let mut file = streaming_platform::tokio::fs::File::open("").await.unwrap();

                        let mut file_buf = [0; 1024];

                        let stream = loop {
                            match file.read(&mut file_buf).await.unwrap() {
                                0 => break,
                                n => {

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
                    let mut mb = mb2.clone();
                    
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