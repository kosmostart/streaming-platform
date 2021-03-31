use std::collections::HashMap;
use std::io::Read;
use serde_json::{json, Value, from_value};
use log::*;
use streaming_platform::{tokio, client, MagicBall, sp_dto::{Key, MsgMeta, Message, Response, resp}};

mod flow;
mod repository {
    pub mod status;
    pub mod pull;
    pub mod add;
    pub mod commit;
}

#[derive(Debug)]
enum SideKick {
    IncorrectKeyInRequest,
    //SerdeJson(serde_json::Error)
}

impl std::fmt::Display for SideKick {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SuperErrorSideKick is here!")
    }
}

impl std::error::Error for SideKick {}

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

				mb.stream_event(Key::new("DeployStream", "Build", "Build"), json!({})).await.unwrap();
                
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

                mb.complete_stream().unwrap();

                //pack();
            });

            json!({
            })
        }
        _ => return Err(Box::new(SideKick::IncorrectKeyInRequest))
    };

    resp(res)
}

pub async fn startup(config: HashMap<String, String>, mut mb: MagicBall, startup_data: Option<Value>, _: ()) {
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