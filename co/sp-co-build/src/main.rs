use std::collections::HashMap;
use std::io::Read;
use serde_json::{json, Value, from_value};
use log::*;
use streaming_platform::{tokio, client, MagicBall, sp_dto::{MsgMeta, Message, Response, resp}};

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
                let q = mb;
                let path = "d:/src/streaming-platform/Cargo.toml";

                let cmd = "cargo";
                
                let args = [
                    "build",
                    "--release",
                    "--manifest-path",
                    path
                ];

                let args = ["--help"];

                let mut handle = std::process::Command::new(cmd)
                    .args(&args)
                    //.stdin(std::process::Stdio::piped())
                    .stdout(std::process::Stdio::piped())
                    //.stderr(std::process::Stdio::piped())
                    .spawn()
                    .expect("");

                let mut stdout = handle.stdout.take().unwrap();
            
                let mut buffer = [0; 10];
            
                // read up to 10 bytes
                while stdout.read(&mut buffer).unwrap() > 0 {
                    println!("{:?}", buffer);
                }
            
                let ecode = handle.wait().expect("failed to wait on child");
            
                println!("{:?}", ecode);

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