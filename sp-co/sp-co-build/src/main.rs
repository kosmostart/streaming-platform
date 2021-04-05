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

pub struct DeployConfig {
    pub build_configs: Vec<BuildConfig>,
    pub deploy_unit_config: sp_pack_core::DeployUnitConfig,
    pub run_config: Option<RunConfig>
}

pub struct BuildConfig {
    pub build_cmd: String,
    pub args: Option<Vec<String>>,
    pub pull_config: Option<PullConfig>
}

pub struct PullConfig {
    pub repository_path: String,
    pub remote_name: String,
    pub remote_branch: String
}

pub struct RunConfig {
    pub run_units: Vec<RunUnit>
}

pub struct RunUnit {
    pub path: String,
    pub config: Option<HashMap<String, String>>
}

pub async fn process_rpc(config: HashMap<String, String>, mut mb: MagicBall, msg: Message<Value>, _: ()) -> Result<Response<Value>, Box<dyn std::error::Error>> {   
    info!("{:#?}", msg);

    let res = match msg.meta.key.action.as_ref() {
        "Deploy" => {
            tokio::spawn(async move {
                let pull_config = PullConfig {
                    repository_path: "d:/src/cfg-if".to_owned(),
                    remote_name: "origin".to_owned(),
                    remote_branch: "master".to_owned()
                };

                let build_config = BuildConfig {
                    build_cmd: "cargo".to_owned(),
                    args: Some(vec![
                        "build".to_owned(),
                        "--release".to_owned(),
                        "--manifest-path".to_owned(),
                        "d:/src/cfg-if/Cargo.toml".to_owned()
                    ]),
                    pull_config: Some(pull_config)
                };

                let deploy_unit_config = sp_pack_core::DeployUnitConfig {
                    result_file_tag: "hello".to_owned(),
                    dirs: None,
                    files: Some(vec![
                        sp_pack_core::TargetFile {
                            path: "d:/src/cfg-if/target/release/libcfg_if.rlib".to_owned()
                        }
                    ])
                };

                let hello_cfg = HashMap::new();

                hello_cfg.insert("arg1".to_owned(), "value1".to_owned());
                hello_cfg.insert("arg2".to_owned(), "value2".to_owned());

                let run_config = RunConfig {
                    run_units: vec![
                        RunUnit {
                            path: "d:/src/hello/target/debug/hello.exe",
                            config: Some(hello_cfg)
                        }
                    ]
                };

                let deploy_config = DeployConfig {
                    build_configs: vec![
                        build_config
                    ],
                    deploy_unit_config,
                    run_config: None
                };

				mb.stream_event(Key::new("DeployStream", "Deploy", "Deploy"), json!({})).await.unwrap();

                for build_config in deploy_config.build_configs {

                    let mut pull_result_msg;

                    match build_config.pull_config {
                        Some(pull_config) => {
                            let res = repository::pull::go(path, remote_name, remote_branch);
                            pull_result_msg = format!("Pull result is {:?}", res);
                        }
                        None => {
                            pull_result_msg = "Pull config not passed".to_owned();
                        }
                    }

                    let mut payload = pull_result_msg.as_bytes().to_vec();
                    payload.push(0x0D);
                    payload.push(0x0A);
                    payload.push(0x0D);
                    payload.push(0x0A);

                    mb.send_frame(&payload, payload.len()).unwrap();

                    let mut handle = match build_config.args {
                        Some(args) => {
                            std::process::Command::new(cmd)
                                .args(&args)
                                //.stdin(std::process::Stdio::piped())
                                .stdout(std::process::Stdio::piped())
                                //.stderr(std::process::Stdio::piped())
                                .spawn()
                                .expect("Failed to start build command")
                        }
                        None => {
                            std::process::Command::new(cmd)
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
                }

                match ecode.success() {
                    true => {
                        use sp_pack_core::TargetFile;

                        let build_config = sp_pack_core::DeployUnitConfig {
                            result_file_tag: "hello".to_owned(),
                            dirs: None,
                            files: Some(vec![
                                TargetFile {
                                    path: "d:/src/cfg-if/target/release/libcfg_if.rlib".to_owned()
                                }
                            ])
                        };

                        let mut pack_result_msg;

                        match pack(build_config) {
                            Ok(pack_result_path) => {
                                pack_result_msg = "Pack result is Ok, path to pack is ".to_owned() + &pack_result_path;
                                info!("{}", pack_result_msg);

                                let msg = send_file(mb.clone(), &pack_result_path, &pack_result_path).await.unwrap();

                                let deploy_pack_result_msg = format!("{:#?}", msg.payload);
                                info!("{}", deploy_pack_result_msg);

                                let mut payload = deploy_pack_result_msg.as_bytes().to_vec();
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

pub async fn startup(config: HashMap<String, String>, mut mb: MagicBall, startup_data: Option<Value>, _: ()) {
}

async fn send_file(mut mb: MagicBall, path: &str, file_name: &str) -> Result<Message<Value>, Error> {
    info!("Opening file {}", path);

    let mut file = File::open(&path).await?;
    let size = file.metadata().await?.len();

    let correlation_id = mb.stream_rpc(Key::new("DeployPack", "Deploy", "Deploy"), json!({
        "file_name": file_name
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

    let mut config = HashMap::new();

    config.insert("addr".to_owned(), "Build".to_owned());
    config.insert("host".to_owned(), "127.0.0.1:11001".to_owned());
    config.insert("access_key".to_owned(), "".to_owned());
 
    client::start(config, process_event, process_rpc, startup, None, ());
 }