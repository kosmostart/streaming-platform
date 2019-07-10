use std::collections::HashMap;
use serde_derive::{Serialize, Deserialize};
use serde_json::Value;
use yew::callback::Callback;
use yew::prelude::worker::*;
use yew::services::console::ConsoleService;
use yew::agent::HandlerId;
use sp_dto::MsgMeta;

pub struct Worker {
    link: AgentLink<Worker>,
    clients: HashMap<String, HandlerId>,
    console: ConsoleService    
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Auth(String),
    Msg(MsgMeta, Value)
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    Msg(MsgMeta, Value)
}

impl Transferable for Request { }
impl Transferable for Response { }

pub enum Msg {    
}

impl Agent for Worker {
    // Available:
    // - `Job` (one per bridge)
    // - `Context` (shared in the same thread)
    // - `Public` (separate thread).
    type Reach = Context;
    type Message = Msg;
    type Input = Request;
    type Output = Response;

    // Create an instance with a link to agent's environment.
    fn create(link: AgentLink<Self>) -> Self {
        let mut console = ConsoleService::new(); 
        console.log("hub created");

        Worker { 
            link,
            clients: HashMap::new(),
            console            
        }
    }

    // Handle inner messages (of services of `send_back` callbacks)
    fn update(&mut self, msg: Self::Message) { /* ... */ 
        self.console.log("hub: got update");
    }

    // Handle incoming messages form components of other agents.
    fn handle(&mut self, msg: Self::Input, who: HandlerId) {
        self.console.log(&format!("hub: {:?}", msg));        
        match msg {
            Request::Auth(addr) => {
                self.clients.insert(addr, who);
            }
            Request::Msg(msg_meta, payload) => {
                match self.clients.get(&msg_meta.rx) {
                    Some(client_id) => self.link.response(*client_id, Response::Msg(msg_meta, payload)),
                    None => self.console.log(&format!("hub: missing client {}", msg_meta.rx))
                }
            }            
        }
    }
}
