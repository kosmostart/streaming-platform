use std::collections::HashMap;
use serde_json::{json, Value, from_slice, to_vec};
use log::*;
use tokio::{io::AsyncWriteExt, fs::File, sync::mpsc::UnboundedReceiver};
use streaming_platform::{client::stream_mode, tokio::{self, runtime::Runtime, io::AsyncReadExt}, DATA_BUF_SIZE, MagicBall, ClientMsg, RestreamMsg, StreamLayout, StreamUnit, sp_dto::{MsgMeta, MsgKind, reply_to_rpc_dto2_sizes, rpc_dto_with_correlation_id_sizes, Route, Participator, RouteSpec, uuid::Uuid, RpcResult}};
use sp_pack_core::unpack;

mod cfg;

struct FileStreamLayout {
    stream: StreamLayout,
    payload: Option<Value>,    
    file: Option<File>,
    rpc_result: RpcResult
}

fn main() {    
    env_logger::init();
    let config = cfg::get_config();    
    let access_key = "";
    let mut rt = Runtime::new().expect("failed to create runtime");
    let mut hm_config = HashMap::new();
    hm_config.insert("access_key".to_owned(), config.access_key.clone());
    hm_config.insert("path".to_owned(), config.path.clone());
    rt.block_on(stream_mode(&config.host, &config.addr, access_key, process_stream, startup, hm_config, None, None));
}

pub async fn startup(config: HashMap<String, String>, mut mb: MagicBall, startup_data: Option<Value>) {
    let access_key = config.get("access_key").expect("access key is empty");
    let (correlation_id, dto, msg_meta_size, payload_size, attachments_sizes) = rpc_dto_with_correlation_id_sizes(
        mb.addr.clone(),
        "SpFileSvc".to_owned(),
        "Download".to_owned(),
        json!({
            "access_key": access_key
        }),
        Route {
            source: Participator::Service(mb.addr.clone()),
            spec: RouteSpec::Simple,
            points: vec![Participator::Service(mb.addr.clone())]
        },
        mb.auth_token.clone(), 
        mb.auth_data.clone()
    ).expect("failed to create download rpc dto");    
    let stream_id = mb.get_stream_id();
    mb.write_vec(
        stream_id,
        dto, 
        msg_meta_size, 
        payload_size, 
        attachments_sizes
    ).await.expect("failed to write download rpc dto");
}

pub async fn process_stream(config: HashMap<String, String>, mut mb: MagicBall, mut rx: UnboundedReceiver<ClientMsg>, _: Option<UnboundedReceiver<RestreamMsg>>) {
    let path = config.get("path").expect("path is empty");
    let mut stream_layouts = HashMap::new();
    loop {        
        let client_msg = rx.recv().await.expect("connection issues acquired");
        let stream_id = client_msg.get_stream_id();
        match process_client_msg(&mut mb, &mut stream_layouts, client_msg, path).await {
            Ok(()) => {}
            Err(e) => {
                match stream_id {
                    Some(stream_id) => {
                        match stream_layouts.remove(&stream_id) {
                            Some(stream_layout) => {
                                match stream_layout.stream.msg_meta.kind {
                                    MsgKind::RpcRequest => {
                                        let mut route = stream_layout.stream.msg_meta.route.clone();
                                        route.points.push(Participator::Service(mb.addr.clone()));
                                        let (res, msg_meta_size, payload_size, attachments_size) = reply_to_rpc_dto2_sizes(
                                            mb.addr.clone(), 
                                            stream_layout.stream.msg_meta.tx.clone(), 
                                            stream_layout.stream.msg_meta.key.clone(), 
                                            stream_layout.stream.msg_meta.correlation_id, 
                                            vec![],
                                            vec![], vec![],
                                            RpcResult::Err,
                                            route,
                                            mb.auth_token.clone(),
                                            mb.auth_data.clone()
                                        ).expect("failed to create rpc reply");
                                        mb.write_vec(stream_layout.stream.id, res, msg_meta_size, payload_size, attachments_size).await.expect("failed to write response to upload");
                                    }
                                    _ => {}
                                }                        
                            }
                            None => {}
                        }
                        error!("{:?}", e);
                    }
                    None => {}
                }
            }
        }
    }
}

async fn process_client_msg(mb: &mut MagicBall, stream_layouts: &mut HashMap<u64, FileStreamLayout>, client_msg: ClientMsg, path: &str) -> Result<(), Error> {
    match client_msg {
        ClientMsg::MsgMeta(stream_id, msg_meta) => {            
            stream_layouts.insert(stream_id, FileStreamLayout {
                stream: StreamLayout {
                    id: stream_id,
                    msg_meta,
                    payload: vec![],
                    attachments_data: vec![]
                },
                payload: None,                
                file: None,
                rpc_result: RpcResult::Ok
            });
        }
        ClientMsg::PayloadData(stream_id, n, buf) => {
            let stream_layout = stream_layouts.get_mut(&stream_id).ok_or(Error::CustomError("not found stream for payload data".to_owned()))?;
            stream_layout.stream.payload.extend_from_slice(&buf[..n]);            
        }
        ClientMsg::PayloadFinished(stream_id, n, buf) => {
            let stream_layout = stream_layouts.get_mut(&stream_id).ok_or(Error::CustomError("not found stream for payload finish".to_owned()))?;
            stream_layout.stream.payload.extend_from_slice(&buf[..n]);
            match stream_layout.stream.msg_meta.key.as_ref() {
                "Download" => {
                    // This is file name attachment vs payload issue
                    let attachment = stream_layout.stream.msg_meta.attachments.iter().nth(0).ok_or(Error::CustomError("no attachment found in msg meta for upload key".to_owned()))?;
                    let payload: Value = from_slice(&stream_layout.stream.payload)?;
                    let file_name = payload["file_name"].as_str().ok_or(Error::CustomError("file name is empty in payload".to_owned()))?;
                    stream_layout.file = Some(File::create(path.to_owned() + "/" + file_name).await?);
                    stream_layout.payload = Some(payload);
                }                
                _ => {}
            }                        
        }
        ClientMsg::AttachmentData(stream_id, index, n, buf) => {
            let stream_layout = stream_layouts.get_mut(&stream_id).ok_or(Error::CustomError("not found stream for attachment data".to_owned()))?;
            match stream_layout.stream.msg_meta.key.as_ref() {
                "Download" => {
                    let file = stream_layout.file.as_mut().ok_or(Error::CustomError("file is empty for attachment data".to_owned()))?;
                    file.write_all(&buf[..n]).await?;
                }
                _ => {}
            }                                                
        }
        ClientMsg::AttachmentFinished(stream_id, index, n, buf) => {
            let stream_layout = stream_layouts.get_mut(&stream_id).ok_or(Error::CustomError("not found stream for attachment finish".to_owned()))?;
            match stream_layout.stream.msg_meta.key.as_ref() {
                "Download" => {
                    let file = stream_layout.file.as_mut().ok_or(Error::CustomError("file is empty for attachment data".to_owned()))?;
                    file.write_all(&buf[..n]).await?;
                    stream_layout.file = None;
                    let payload = stream_layout.payload.as_ref().ok_or(Error::CustomError("payload is empty".to_owned()))?;
                    let file_name = payload["file_name"].as_str().ok_or(Error::CustomError("file name is empty in payload".to_owned()))?;                    
                    println!("file {} download complete", file_name);
                    unpack(path.to_owned(), file_name.to_owned());                    
                }
                _ => {}
            }                                        
        }
        ClientMsg::MessageFinished(stream_id) => {                                
            let stream_layout = stream_layouts.remove(&stream_id).ok_or(Error::CustomError("not found stream for message finish".to_owned()))?;            
        }
        _ => {}
    }
    Ok(())
}

async fn download_file(mut mb: MagicBall, msg_meta: MsgMeta, path: String, file_name: String) -> Result<(), Error> {
    let mut file = File::open(&path).await?;
    let size = file.metadata().await?.len();
    let (dto, msg_meta_size, payload_size, _) = reply_to_rpc_dto2_sizes(mb.addr.clone(), msg_meta.tx.clone(), msg_meta.key.clone(), msg_meta.correlation_id, vec![], vec![(file_name, size)], vec![], RpcResult::Ok, msg_meta.route.clone(), mb.auth_token.clone(), mb.auth_data.clone())?;
    let stream_id = mb.get_stream_id();
    mb.write_vec(stream_id, dto, msg_meta_size, payload_size, vec![]).await?;        
    match size {
        0 => {
            mb.write_tx.send(StreamUnit::Empty(stream_id))?;
        }
        _ => {
            let mut file_buf = [0; DATA_BUF_SIZE];
            loop {
                match file.read(&mut file_buf).await? {
                    0 => break,
                    n => {                
                        mb.write_tx.send(StreamUnit::Array(stream_id, n, file_buf))?;
                    }
                }
            }
        }
    }        
    Ok(())
}

#[derive(Debug)]
pub enum Error {    
	Io(std::io::Error),	
    SerdeJson(serde_json::Error),
    StreamingPlatform(streaming_platform::ProcessError),
    SendStreamUnit,
    OptionIsNone,
    CustomError(String)
}

impl From<std::io::Error> for Error {
	fn from(e: std::io::Error) -> Error {
		Error::Io(e)
	}
}

impl From<serde_json::Error> for Error {
	fn from(e: serde_json::Error) -> Error {
		Error::SerdeJson(e)
	}
}

impl From<streaming_platform::ProcessError> for Error {
    fn from(e: streaming_platform::ProcessError) -> Error {
        Error::StreamingPlatform(e)
    }
}

impl From<tokio::sync::mpsc::error::SendError<streaming_platform::StreamUnit>> for Error {
    fn from(e: tokio::sync::mpsc::error::SendError<streaming_platform::StreamUnit>) -> Error {
        Error::SendStreamUnit
    }
}




/*
fn process_msg(msg: ClientMsg, save_path: String) {
    match msg {
        ClientMsg::FileReceiveComplete(name) => unpack(save_path, name),
        _ => {}
    }
}
*/