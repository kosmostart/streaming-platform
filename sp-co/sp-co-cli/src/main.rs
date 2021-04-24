use std::io::BufReader;
use std::io::prelude::*;
use serde_json::{Value, json, from_str};
use tokio::runtime::Runtime;
use sp_dto::{Key, Route, rpc_dto, get_msg};
use error::Error;

mod error;

fn main() -> Result<(), Error> {
    let rt = Runtime::new().expect("failed to create runtime");

    //cfg_add(&rt);
    //cfg_get(&rt);

    println!("");
    println!("Available commands:");
    println!("");
    println!("add");
    println!("get all");
    println!("get");
    println!("load file");
    println!("deploy");
    println!("");

    println!("Enter command:");
    println!("");

    let mut buf = String::new();
    let _ = std::io::stdin().read_line(&mut buf)?;
    let cmd = buf.lines().nth(0).ok_or(Error::None)?;

    println!("");

    match cmd {
        "add" => {
            buf.clear();

            println!("Enter value:");
            println!("");

            let mut payload = None;

            loop {
                let _ = std::io::stdin().read_line(&mut buf)?;
                let line = buf.lines().last().ok_or(Error::None)?;

                match line {
                    "done" => {
                        
                        loop {
                            match buf.pop() {
                                Some(char) => {
                                    if char == '}' {
                                        buf.push('}');
                                        break;
                                    }
                                }
                                None => break
                            }                            
                        }

                        match from_str::<Value>(&buf) {
                            Ok(value) => {
                                match value.is_object() {
                                    true => {
                                        println!("");
                                        println!("Received value:");
                                        println!("");
                                        println!("{:#?}", value);
                                        println!("");
                                
                                        payload = Some(value);

                                        println!("Proceed?");
                                        println!("");
                                        println!("Enter yes or enter any character to restart value input");

                                        buf.clear();

                                        let _ = std::io::stdin().read_line(&mut buf)?;
                                        let line = buf.lines().nth(0).ok_or(Error::None)?;

                                        match line {
                                            "yes" => {
                                                buf.clear();
                                                break;
                                            }
                                            _ => {
                                                buf.clear();
                                                payload = None;

                                                println!("");
                                                println!("Previous input cleared");
                                                println!("");
                                                println!("Enter value:");
                                                println!("");
                                            }
                                        }
                                    }
                                    false => {
                                        println!("");
                                        println!("Provided value is not an object");
                                        println!("");
                                        println!("Previous input cleared");
                                        println!("");

                                        buf.clear();
                                    }
                                }
                            }
                            Err(e) => {
                                println!("");
                                println!("Incorrect value provided:");
                                println!("");
                                println!("{:?}", e);
                                println!("");
                                println!("Previous input cleared");
                                println!("");

                                buf.clear();
                            }
                        }    
                    }
                    "clear" =>  {
                        buf.clear();
                        println!("");
                        println!("Previous input cleared");
                        println!("");
                    }
                    _ => {}
                }
            }

            cfg_add(&rt, payload.ok_or(Error::None)?);
        }
        "get all" => {
            cfg_get_all(&rt);
        }
        "get" => {
			println!("Enter domain:");
            println!("");

            buf.clear();
            let _ = std::io::stdin().read_line(&mut buf)?;
            let domain = buf.lines().nth(0).ok_or(Error::None)?.to_owned();

            println!("Enter key:");
            println!("");

            buf.clear();
            let _ = std::io::stdin().read_line(&mut buf)?;
            let key = buf.lines().nth(0).ok_or(Error::None)?;			

            cfg_get(&rt, json!({
				"domain": domain,
                "key": key
            }));
        }
        "load file" => {
            println!("Enter path to file:");
            println!("");

            buf.clear();
            let _ = std::io::stdin().read_line(&mut buf)?;
            let path = buf.lines().nth(0).ok_or(Error::None)?;

            let file = std::fs::File::open(path)?;
    
            let mut buf_reader = BufReader::new(file);
            buf.clear();
        
            buf_reader.read_to_string(&mut buf)?;

            let data: Value = from_str(&buf)?;

            match data {
                Value::Array(cfg_list) => {
                    for cfg_unit in cfg_list.iter() {
						if !cfg_unit["domain"].is_string() {
                            return Err(Error::custom("Empty domain in one of provided values"));
                        }

                        if !cfg_unit["key"].is_string() {
                            return Err(Error::custom("Empty key in one of provided values"));
                        }
                    }

                    for cfg_unit in cfg_list {
                        cfg_add(&rt, cfg_unit);
                    }
                }
                Value::Object(_) => {
					if !data["domain"].is_string() {
						return Err(Error::custom("Empty domain in one of provided values"));
					}

                    if !data["key"].is_string() {
                        return Err(Error::custom("Empty key in provided value"));
                    }

                    cfg_add(&rt, data);
                }
                _ => {
                    return Err(Error::custom("Provided value in file is not an array nor an object"));
                }
            }
        }
        "deploy" => {
            let payload = json!({});
            
            deploy(&rt, payload);
        }
        _ => {}
    }

    Ok(())
}

fn cfg_add(rt: &Runtime, payload: Value) {
    let auth_url = "http://127.0.0.1:12345/authorize";
    let hub_url = "http://127.0.0.1:12345/hub";

    let auth_payload = json!({
    });

    let (_, dto) = rpc_dto("Cli".to_owned(), Key::new("Auth", "Auth", "Auth"), auth_payload, Route::new_cli_with_service_client("Cli", "Web"), None, None).unwrap();

    let client = reqwest::Client::new();
    
    let res = rt.block_on(client.post(auth_url)
        .body(dto)
        .send()).unwrap();

    println!("{:#?}", res);

    let split: Vec<&str> = res.headers().get("set-cookie").unwrap().to_str().unwrap().split(";").collect();

    println!("{}", split[0]);

	let qq = split[0].to_owned();

    let (_, dto) = rpc_dto("Cli".to_owned(), Key::new("Add", "Cfg", "Cfg"), payload, Route::new_cli_with_service_client("Cli", "Web"), None, None).unwrap();

    let client = reqwest::Client::new();
    
    let res = rt.block_on(client.post(hub_url)
        .body(dto)
        .header("cookie", split[0])
        .send()).unwrap();

    let data = rt.block_on(res.bytes()).unwrap();

    let msg: (_, Value, _) = get_msg(&data).unwrap();

    println!("{:#?}", msg);
}

fn cfg_get_all(rt: &Runtime) {
    let auth_url = "http://127.0.0.1:12345/authorize";
    let hub_url = "http://127.0.0.1:12345/hub";	

    let auth_payload = json!({
    });

    let (_, dto) = rpc_dto("Cli".to_owned(), Key::new("Auth", "Auth", "Auth"), auth_payload, Route::new_cli_with_service_client("Cli", "Web"), None, None).unwrap();

    let client = reqwest::Client::new();
    
    let res = rt.block_on(client.post(auth_url)
        .body(dto)
        .send()).unwrap();

    println!("{:#?}", res);

    let split: Vec<&str> = res.headers().get("set-cookie").unwrap().to_str().unwrap().split(";").collect();

    println!("{}", split[0]);

	let qq = split[0].to_owned();

    let (_, dto) = rpc_dto("Cli".to_owned(), Key::new("GetAll", "Cfg", "Cfg"), json!({}), Route::new_cli_with_service_client("Cli", "Web"), None, None).unwrap();

    let client = reqwest::Client::new();
    
    let res = rt.block_on(client.post(hub_url)
        .body(dto)
        .header("cookie", split[0])
        .send()).unwrap();

    let data = rt.block_on(res.bytes()).unwrap();

    let msg: (_, Value, _) = get_msg(&data).unwrap();

    println!("{:#?}", msg);
}

fn cfg_get(rt: &Runtime, payload: Value) {
    let auth_url = "http://127.0.0.1:12345/authorize";
    let hub_url = "http://127.0.0.1:12345/hub";	

    let auth_payload = json!({
    });

    let (_, dto) = rpc_dto("Cli".to_owned(), Key::new("Auth", "Auth", "Auth"), auth_payload, Route::new_cli_with_service_client("Cli", "Web"), None, None).unwrap();

    let client = reqwest::Client::new();
    
    let res = rt.block_on(client.post(auth_url)
        .body(dto)
        .send()).unwrap();

    println!("{:#?}", res);

    let split: Vec<&str> = res.headers().get("set-cookie").unwrap().to_str().unwrap().split(";").collect();

    println!("{}", split[0]);

	let qq = split[0].to_owned();

    let (_, dto) = rpc_dto("Cli".to_owned(), Key::new("Get", "Cfg", "Cfg"), payload, Route::new_cli_with_service_client("Cli", "Web"), None, None).unwrap();

    let client = reqwest::Client::new();
    
    let res = rt.block_on(client.post(hub_url)
        .body(dto)
        .header("cookie", split[0])
        .send()).unwrap();

    let data = rt.block_on(res.bytes()).unwrap();

    let msg: (_, Value, _) = get_msg(&data).unwrap();

    println!("{:#?}", msg);
}

fn deploy(rt: &Runtime, payload: Value) {
    let auth_url = "http://127.0.0.1:12345/authorize";
    let hub_url = "http://127.0.0.1:12345/hub";	

    let auth_payload = json!({
    });

    let (_, dto) = rpc_dto("Cli".to_owned(), Key::new("Auth", "Auth", "Auth"), auth_payload, Route::new_cli("Cli"), None, None).unwrap();

    let client = reqwest::Client::new();
    
    let res = rt.block_on(client.post(auth_url)
        .body(dto)
        .send()).unwrap();

    println!("{:#?}", res);

    let split: Vec<&str> = res.headers().get("set-cookie").unwrap().to_str().unwrap().split(";").collect();

    println!("{}", split[0]);

	let qq = split[0].to_owned();

    
    rt.spawn(async {
		let res = q(qq).await;
		println!("{:?}", res);
	});

    let (_, dto) = rpc_dto("Cli".to_owned(), Key::new("Deploy", "Deploy", "Deploy"), payload, Route::new_cli("Cli"), None, None).unwrap();

    let client = reqwest::Client::new();
    
    let res = rt.block_on(client.post(hub_url)
        .body(dto)
        .header("cookie", split[0])
        .send()).unwrap();

    let data = rt.block_on(res.bytes()).unwrap();

    let msg: (_, Value, _) = get_msg(&data).unwrap();

    println!("{:#?}", msg);	

	std::thread::park();

   /*
    let event_source = sse_client::EventSource::new("http://localhost:12345/events").unwrap();

    for event in event_source.receiver().iter() {
        println!("New Message: {}", event.data);
    }
	*/
}

async fn q(cookie: String) -> Result<(), Box<dyn std::error::Error>> {
	let client = reqwest::Client::new();

    let mut res = client
		.get("http://127.0.0.1:12345/downstream")
		.header("cookie", cookie)
		.send()
		.await?;

	while let Some(chunk) = res.chunk().await? {
		print!("{}", String::from_utf8(chunk.to_vec()).unwrap());
	}	

	println!("Downstream end");

    Ok(())
}

/*
use futures::stream::StreamExt;
use reqwest::Client;
use reqwest_eventsource::RequestBuilderExt;

async fn events() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    let mut stream = client
        .get("http://127.0.0.1:12345/events")
        .eventsource()?;

    println!("Waiting");

    while let Some(event) = stream.next().await {
        match event {
            Ok(event) => println!(
                "received: {:?}: {}",
                event.event,
                String::from_utf8_lossy(&event.data)
            ),
            Err(e) => eprintln!("error occured: {}", e),
        }
    }

    Ok(())
}
*/