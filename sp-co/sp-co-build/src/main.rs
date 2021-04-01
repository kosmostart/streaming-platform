use std::{collections::HashMap, process::ExitStatus};
use std::io::Read;
use serde_json::{json, Value, from_value, to_vec};
use log::*;
use streaming_platform::{MAX_FRAME_PAYLOAD_SIZE, tokio::{self, fs::File, io::AsyncReadExt}, client, MagicBall, sp_dto::{Key, MsgMeta, Message, Response, resp}};
use sp_pack_core::pack;

mod flow;
mod repository {
    pub mod status;
    pub mod pull;
    pub mod add;
    pub mod commit;
}

#[derive(Debug)]
enum Error {
    Io(std::io::Error),
    SerdeJson(serde_json::Error),
    StreamingPlatform(streaming_platform::ProcessError),
    IncorrectKeyInRequest,
    SendFrame
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SuperErrorError is here!")
    }
}

impl std::error::Error for Error {}

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

impl From<tokio::sync::mpsc::error::SendError<streaming_platform::Frame>> for Error {
    fn from(_: tokio::sync::mpsc::error::SendError<streaming_platform::Frame>) -> Error {
        Error::SendFrame
    }
}

pub async fn process_event(config: HashMap<String, String>, mut mb: MagicBall, msg: Message<Value>, _: ()) -> Result<(), Box<dyn std::error::Error>>  {
    //info!("{:#?}", msg);
    
    Ok(())
}

pub async fn process_rpc(config: HashMap<String, String>, mut mb: MagicBall, msg: Message<Value>, _: ()) -> Result<Response<Value>, Box<dyn std::error::Error>> {   
    info!("{:#?}", msg);

    let res = match msg.meta.key.action.as_ref() {
        "Deploy" => {
            tokio::spawn(async move {                
                let build_path = "d:/src/cfg-if/Cargo.toml";

                let cmd = "cargo";
                
                let args = [
                    "build",
                    "--release",
                    "--manifest-path",
                    build_path
                ];

                //let args = ["--help"];

				mb.stream_event(Key::new("DeployStream", "Deploy", "Deploy"), json!({})).await.unwrap();
                
                let path = "d:/src/cfg-if";
                let remote_name = "origin";
                let remote_branch = "master";

                let res = repository::pull::go(path, remote_name, remote_branch);

                let mut payload = format!("Pull result is {:?}", res).as_bytes().to_vec();
                payload.push(0x0D);
                payload.push(0x0A);
                payload.push(0x0D);
                payload.push(0x0A);

                mb.send_frame(&payload, payload.len()).unwrap();

                let mut handle = std::process::Command::new(cmd)
                    .args(&args)
                    //.stdin(std::process::Stdio::piped())
                    .stdout(std::process::Stdio::piped())
                    //.stderr(std::process::Stdio::piped())
                    .spawn()
                    .expect("");

                let mut stdout = handle.stdout.take().unwrap();		
                
                let mut buf = [0; 100];

                loop {
					let n = stdout.read(&mut buf).unwrap();

					match n {
						0 => break,
						_ => mb.send_frame(&buf[..n], n).unwrap()
					}
				}
            
                let ecode = handle.wait().expect("failed to wait on child");
            
                info!("{:?}", ecode);

                let mut payload = format!("Exit code is {:?}", ecode).as_bytes().to_vec();
                payload.push(0x0D);
                payload.push(0x0A);
                payload.push(0x0D);
                payload.push(0x0A);

                mb.send_frame(&payload, payload.len()).unwrap();

                let mut build_result_msg;

                match ecode.success() {
                    true => {
                        build_result_msg = "Build result is Ok";
                    }
                    false => {
                        build_result_msg = "Build result is Err";
                    }
                }

                info!("{}", build_result_msg);

                let mut payload = build_result_msg.as_bytes().to_vec();
                payload.push(0x0D);
                payload.push(0x0A);
                payload.push(0x0D);
                payload.push(0x0A);

                mb.send_frame(&payload, payload.len()).unwrap();

                match ecode.success() {
                    true => {
                        use sp_pack_core::TargetFile;

                        let build_config = sp_pack_core::Config {
                            result_file_tag: "hello".to_owned(),
                            dirs: None,
                            files: vec![
                                TargetFile {
                                    path: "d:/src/cfg-if/target/release/libcfg_if.rlib".to_owned()
                                }
                            ]
                        };

                        let mut pack_result_msg;

                        match pack(build_config) {
                            Ok(pack_result_path) => {
                                pack_result_msg = "Pack result is Ok, path to file is {}".to_owned() + &pack_result_path;

                                let q = mb.clone();
                            }
                            Err(e) => {
                                pack_result_msg = format!("Pack result is Err, {:?}", e);
                            }
                        }

                        info!("{}", pack_result_msg);

                        let mut payload = pack_result_msg.as_bytes().to_vec();
                        payload.push(0x0D);
                        payload.push(0x0A);
                        payload.push(0x0D);
                        payload.push(0x0A);

                        mb.send_frame(&payload, payload.len()).unwrap();
                    }
                    false => {
                        
                    }
                }

                mb.complete_stream().unwrap();
            });

            json!({
            })
        }
        _ => return Err(Box::new(Error::IncorrectKeyInRequest))
    };

    resp(res)
}

pub async fn startup(config: HashMap<String, String>, mut mb: MagicBall, startup_data: Option<Value>, _: ()) {
}

async fn send_file(mut mb: MagicBall, msg_meta: MsgMeta, path: std::path::PathBuf, file_name: String) -> Result<(), Error> {
    let mut file = File::open(&path).await?;
    let size = file.metadata().await?.len();

    let payload = to_vec(&json!({
        "file_name": file_name
    }))?;

    mb.stream_rpc_response(msg_meta, json!({})).await?;

    match size {
        0 => {
        }
        _ => {
            let mut buf = [0; MAX_FRAME_PAYLOAD_SIZE];

            loop {
                match file.read(&mut buf).await? {
                    0 => break,
                    n => {                
                        mb.send_frame(&buf, n)?;
                    }
                }
            }
        }
    }

    mb.complete_stream()?;

    Ok(())
}

pub fn main() {
    env_logger::init();

    //flow::start_ui();

    let mut config = HashMap::new();

    config.insert("addr".to_owned(), "Build".to_owned());
    config.insert("host".to_owned(), "127.0.0.1:11001".to_owned());
    config.insert("access_key".to_owned(), "".to_owned());
 
    client::start(config, process_event, process_rpc, startup, None, ());
 }