use std::{collections::HashMap, process::ExitStatus};
use std::io::Read;
use serde_json::{json, Value, from_value, to_vec};
use log::*;
use streaming_platform::{MAX_FRAME_PAYLOAD_SIZE, tokio::{self, fs::File, io::AsyncReadExt}, client, MagicBall, sp_dto::{Key, MsgMeta, Message, Response, resp}};
use sp_build_core::{pack, DeployConfig, DeployUnitConfig, TargetFile, RunConfig, RunUnit};

mod flow;
mod repository {
    pub mod status;
    pub mod pull;
    pub mod add;
    pub mod commit;
}

pub async fn process_event(config: Value, mut mb: MagicBall, msg: Message<Value>, _: ()) -> Result<(), Box<dyn std::error::Error>>  {
    //info!("{:#?}", msg);
    
    Ok(())
}

pub async fn process_rpc(config: Value, mut mb: MagicBall, msg: Message<Value>, _: ()) -> Result<Response<Value>, Box<dyn std::error::Error>> {   
    info!("{:#?}", msg);

    let res = match msg.meta.key.action.as_ref() {
        "Deploy" => {
            tokio::spawn(async move {
                let msg = mb.rpc(Key::new("Get", "Cfg", "Cfg"), json!({
                    "key": "DeployHello"
                })).await.unwrap();

                let deploy_config: DeployConfig = from_value(msg.payload).expect("Failed to deserialize build config");

				mb.start_event_stream(Key::new("DeployStream", "Deploy", "Deploy"), json!({})).await.unwrap();

                let mut build_success = false;

                for build_config in deploy_config.build_configs {
                    let mut pull_result_msg;

                    match build_config.pull_config {
                        Some(pull_config) => {
                            let res = repository::pull::go(&pull_config.repository_path, &pull_config.remote_name, &pull_config.remote_branch);
                            pull_result_msg = format!("pull result is {:?}", res);
                        }
                        None => {
                            pull_result_msg = "pull config not passed".to_owned();
                        }
                    }

                    let mut payload = format!("Build {}, {}", build_config.build_name, pull_result_msg).as_bytes().to_vec();
                    payload.push(0x0D);
                    payload.push(0x0A);
                    payload.push(0x0D);
                    payload.push(0x0A);

                    mb.send_frame(&payload, payload.len()).unwrap();

                    let mut handle = match build_config.args {
                        Some(args) => {
                            std::process::Command::new(build_config.build_cmd)
                                .args(&args)
                                //.stdin(std::process::Stdio::piped())
                                .stdout(std::process::Stdio::piped())
                                //.stderr(std::process::Stdio::piped())
                                .spawn()
                                .expect("Failed to start build command")
                        }
                        None => {
                            std::process::Command::new(build_config.build_cmd)
                                //.stdin(std::process::Stdio::piped())
                                .stdout(std::process::Stdio::piped())
                                //.stderr(std::process::Stdio::piped())
                                .spawn()
                                .expect("Failed to start build command")
                        }
                    };

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
                    build_success = ecode.success();
                
                    info!("{:?}", ecode);

                    let mut payload = format!("Build {}, exit code is {:?}", build_config.build_name, ecode).as_bytes().to_vec();
                    payload.push(0x0D);
                    payload.push(0x0A);
                    payload.push(0x0D);
                    payload.push(0x0A);

                    mb.send_frame(&payload, payload.len()).unwrap();

                    let mut build_result_msg;

                    match ecode.success() {
                        true => {
                            build_result_msg = "Ok";
                        }
                        false => {
                            build_result_msg = "Err";
                        }
                    }

                    info!("Build {}, build result is {}", build_config.build_name, build_result_msg);

                    let mut payload = build_result_msg.as_bytes().to_vec();
                    payload.push(0x0D);
                    payload.push(0x0A);
                    payload.push(0x0D);
                    payload.push(0x0A);

                    mb.send_frame(&payload, payload.len()).unwrap();
                }

                match build_success {
                    true => {
                        let mut pack_result_msg;

                        match pack(deploy_config.deploy_unit_config) {
                            Ok(deploy_unit_path) => {
                                pack_result_msg = "Pack result is Ok, path to deploy unit is ".to_owned() + &deploy_unit_path;
                                info!("{}", pack_result_msg);

                                let msg = deploy_unit(mb.clone(), &deploy_unit_path, &deploy_unit_path, deploy_config.run_config).await.unwrap();

                                let deploy_unit_msg = format!("{:#?}", msg.payload);
                                info!("{}", deploy_unit_msg);

                                let mut payload = deploy_unit_msg.as_bytes().to_vec();
                                payload.push(0x0D);
                                payload.push(0x0A);
                                payload.push(0x0D);
                                payload.push(0x0A);

                                mb.send_frame(&payload, payload.len()).unwrap();
                            }
                            Err(e) => {
                                pack_result_msg = format!("Pack result is Err, {:?}", e);
                                error!("{}", pack_result_msg);
                            }
                        }

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

pub async fn startup(initial_config: Value, target_config: Value, mut mb: MagicBall, startup_data: Option<Value>, _: ()) {
}

async fn deploy_unit(mut mb: MagicBall, path: &str, deploy_unit_name: &str, run_config: Option<RunConfig>) -> Result<Message<Value>, Error> {
    info!("Opening file {}", path);

    let mut file = File::open(&path).await?;
    let size = file.metadata().await?.len();

    let correlation_id = mb.start_rpc_stream(Key::new("DeployUnit", "Deploy", "Deploy"), json!({
        "deploy_unit_name": deploy_unit_name,
        "run_config": run_config
    })).await?;

    match size {
        0 => {
        }
        _ => {
            let mut buf = [0; MAX_FRAME_PAYLOAD_SIZE];

            loop {
                match file.read(&mut buf).await? {
                    0 => {
                        mb.complete_attachment()?;
                        break;
                    }
                    n => {                
                        mb.send_frame(&buf, n)?;
                    }
                }
            }
        }
    }

    Ok(mb.complete_rpc_stream(correlation_id).await?)
}

pub fn main() {
    env_logger::init();

    //flow::start_ui();

    let config = json!({
        "cfg_host": "127.0.0.1:11002",
        "cfg_token": "Build"
    });
 
    client::start_full_message(config, process_event, process_rpc, startup, None, ());
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