#![feature(proc_macro_hygiene)]
#![feature(async_closure)]
use std::collections::HashMap;
use std::net::SocketAddr;
use std::env::current_dir;
use log::*;
use serde_json::{json, Value, to_vec};
use warp::{Filter, http::{Response, header::SET_COOKIE}};
use streaming_platform::{MagicBall, tokio::{io::AsyncReadExt}};
use streaming_platform::sp_dto::{Message, resp};
use streaming_platform::sp_dto::MsgMeta;
use sp_auth::verify_auth_token;
pub use streaming_platform;

mod authorize;
mod hub;

pub async fn process_event(_config: HashMap<String, String>, mut _mb: MagicBall, _msg: Message<Value>, _: ()) -> Result<(), Box<dyn std::error::Error>>  {
    Ok(())
}

pub async fn process_rpc(_config: HashMap<String, String>, mut _mb: MagicBall, _msg: Message<Value>, _: ()) -> Result<streaming_platform::sp_dto::Response<Value>, Box<dyn std::error::Error>> {
    resp(json!({}))    
}

pub async fn startup(config: HashMap<String, String>, mb: MagicBall, startup_data: Option<Value>, _: ()) {
    let listen_addr = config.get("listen_addr").expect("Missing listen_addr config value");
    let cert_path = config.get("cert_path");
    let key_path = config.get("key_path");
    let aca_origin = config.get("aca_origin").map(|x| x.to_owned());
    let aca_origin2 = aca_origin.clone();
    let mb2 = mb.clone();

    let listen_addr = listen_addr.parse::<SocketAddr>().expect("Incorrect listen addr passed");

    let auth_token_key = config.get("auth_token_key").map(|x| x.to_owned()).expect("Missing auth_token_key config value");
    let auth_token_key1 = auth_token_key.clone();
    let auth_token_key2 = auth_token_key.clone();

    let mut app_indexes = HashMap::new();
    let mut app_paths = HashMap::new();

    match startup_data {
        Some(startup_data) =>
            match startup_data["apps"].as_array() {
                Some(apps) => {                    
                    for app in apps {
                        match app["name"].as_str() {
                            Some(app_name) => {
                                match app["index"].as_str() {
                                    Some(app_index) => {
                                        app_indexes.insert(app_name.to_owned(), (app["allow_unauthorized"].as_bool(), app_index.to_owned()));
                                    }
                                    None => {}
                                }
                                match app["path"].as_str() {
                                    Some(app_path) => {
                                        app_paths.insert(app_name.to_owned(), (app["allow_unauthorized"].as_bool(), app_path.to_owned()));
                                    }
                                    None => {}
                                }
                            }
                            None => {}
                        }                        
                    }                    
                }
                None => {}
            }
        None => {}
    };

    let deploy_path = match config.get("deploy_path") {
        Some(path) => path.to_owned(),
        None => current_dir().expect("failed to get current dir").to_str().expect("failed to get current dir str (for deploy path)").to_owned()
    };

    let routes =                    
        warp::path("hi")
            .map(move || {
                Response::builder()
                    .header("content-type", "text/html")
                    .body("hi")
            })
        .or(
            warp::path("authorize")
                .and(warp::post())                
                .and(warp::body::bytes())
                .and_then(move |body: warp::hyper::body::Bytes| {
                    let aca_origin = aca_origin.clone();
                    let mb = mb.clone();
                    
                    crate::authorize::go(aca_origin, body, mb)
                }
            )
        )
        .or(
            warp::path("app")
                .and(warp::path::param())
                .and(warp::header::optional("cookie"))
                .and(warp::path::end())                                
                .map(move |app_name: String, cookie_header: Option<String>| {
                    let auth_token_key = auth_token_key.clone();
                    let mut app_indexes = app_indexes.clone();

                    match app_indexes.remove(&app_name) {
                        Some((allow_unauthorized, index)) =>
                        
                            match allow_unauthorized == Some(true) {
                                true => Response::builder()
                                    .header("content-type", "text/html")
                                    .body(index),
                                false =>
                                    match check_auth_token(auth_token_key.as_bytes(), cookie_header) {
                                        Some(auth_data) => 
                                            Response::builder()
                                                .header("content-type", "text/html")
                                                .body(index),
                                        None => {
                                            warn!("Unauthorized access attempt, app name: {}", app_name);

                                            Response::builder()
                                                .header("content-type", "text/html")
                                                .body("Here comes the error".to_owned())
                                        }
                                    }
                            }
                        None => 
                            Response::builder()
                                .header("content-type", "text/html")
                                .body("Here comes the error".to_owned())
                    }
                }
            )
        )
        .or(
            warp::path("app")
                .and(warp::path::param())
                .and(warp::header::optional("cookie"))
                .and(warp::path::tail())                                
                .map(move |app_name: String, cookie_header: Option<String>, tail: warp::path::Tail| {
                    let auth_token_key = auth_token_key1.clone();
                    let deploy_path = deploy_path.clone();
                    let mut app_paths = app_paths.clone();

                    match app_paths.remove(&app_name) {
                        Some((allow_unauthorized, app_path)) => {

                            match allow_unauthorized == Some(true) {
                                true => process_static_file_request(deploy_path, app_path, tail.as_str().to_owned()),
                                false => 
                                    match check_auth_token(auth_token_key.as_bytes(), cookie_header) {
                                        Some(auth_data) => process_static_file_request(deploy_path, app_path, tail.as_str().to_owned()),
                                        None => {
                                            warn!("Unauthorized file access attempt, app name: {}, tail: {}", app_name, tail.as_str());
                
                                            Response::builder()
                                                .header("content-type", "text/html")
                                                .body(warp::hyper::body::Body::from("Here comes the error"))
                                        }
                                    }                                
                            }                                                                                 
                        }
                        None => Response::builder()
                            .header("content-type", "text/html")
                            .body(warp::hyper::body::Body::from("Here comes the error"))
                    }                    
                }
            )
        )
        .or(
            warp::path("hub")
                .and(warp::post())
                .and(warp::header::optional("cookie"))
                .and(warp::body::bytes())
                .and_then(move |cookie_header: Option<String>, body: warp::hyper::body::Bytes| {
                    let auth_token_key = auth_token_key2.clone();
                    let aca_origin = aca_origin2.clone();                    
                    let mb = mb2.clone();

                    crate::hub::go(aca_origin, auth_token_key, cookie_header, body, mb)
                }
            )
        )
    ;

    if cert_path.is_some() && key_path.is_some() {
        warp::serve(routes)
        .tls()
        .cert_path(cert_path.expect("Certificate path failed"))
        .key_path(key_path.expect("Key path failed"))
        .run(listen_addr)
        .await;	
    } else {
        warp::serve(routes)        
        .run(listen_addr)
        .await;
    }
}

fn check_auth_token(auth_token_key: &[u8], cookie_header: Option<String>) -> Option<Value> {
    match cookie_header {
        Some(cookie_string) => {
			let split: Vec<&str> = cookie_string.split(";").collect();

			for pair in split {
				let pair = pair.trim();				

				if pair.len() > 13 && &pair[..13] == "skytfs-token=" {
					match verify_auth_token(auth_token_key, &pair[13..]) {
						Ok(auth_data) => return Some(auth_data),
						Err(e) => {
							warn!("Auth token verification failed, {:?}", e);
							return None;
						}
					}
				}
			}

			None
        }
        None => None
    }
}

fn process_static_file_request(deploy_path: String, app_path: String, tail: String) -> Result<Response<warp::hyper::body::Body>, warp::http::Error> {
    let mime = mime_guess::from_path(&tail).first();

    let (mut tx, body) = warp::hyper::body::Body::channel();

    streaming_platform::tokio::spawn(async move {                                
        let file_path = deploy_path + "/" + &app_path + "/" + &tail;
        let mut file = streaming_platform::tokio::fs::File::open(&file_path).await.expect(&("File not found ".to_owned() + &file_path));                                
        let mut file_buf = [0; 1024];

        loop {
            match file.read(&mut file_buf).await.expect("File read failed") {
                0 => break,
                n =>                                 
                    match tx.send_data(warp::hyper::body::Bytes::copy_from_slice(&file_buf[..n])).await {
                        Ok(_) => {}
                        Err(_) => break
                    }
            }
        }
    });                            

    match mime {
        Some(mime_type) => Response::builder()                                    
            .header("content-type", mime_type.essence_str())
            .body(body),
        None => Response::builder()                                    
            .body(body)
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