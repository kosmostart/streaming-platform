use serde_json::{Value, json};
use tokio::runtime::Runtime;
use sp_dto::{Key, Route, rpc_dto, get_msg};

fn main() {
    let rt = Runtime::new().expect("failed to create runtime");

    let auth_url = "http://127.0.0.1:12345/authorize";
    let hub_url = "http://127.0.0.1:12345/hub";	

    let payload = json!({
    });

    let dto = rpc_dto("Cli".to_owned(), Key::new("Auth", "Auth", "Auth"), payload, Route::new_cli("Cli"), None, None).unwrap();

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

    let payload = json!({
    });

    let dto = rpc_dto("Cli".to_owned(), Key::new("Deploy", "Build", "Build"), payload, Route::new_cli("Cli"), None, None).unwrap();

    let client = reqwest::Client::new();
    
    let res = rt.block_on(client.post(hub_url)
        .body(dto)
        .header("cookie", split[0])
        .send()).unwrap();

    let data = rt.block_on(res.bytes()).unwrap();

    let msg: (_, Value, _) = get_msg(&data).unwrap();

    println!("{:#?}", msg);	

	rt.block_on(async {
		loop {
            
        }
	});
    

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
		println!("{:?}", chunk);
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