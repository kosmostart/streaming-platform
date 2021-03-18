use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().expect("failed to create runtime"); 
    let client = reqwest::Client::new();
    
    let res = rt.block_on(client.post("http://httpbin.org/post")
        .body("the exact body that is sent")
        .send());

    println!("{:?}", res);
}