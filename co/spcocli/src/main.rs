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
}