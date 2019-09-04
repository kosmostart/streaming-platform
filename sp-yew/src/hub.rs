use std::collections::HashMap;
use serde_derive::{Serialize, Deserialize};
use serde_json::Value;
use yew::callback::Callback;
use yew::prelude::worker::*;
use yew::services::console::ConsoleService;
use yew::agent::HandlerId;
use sp_dto::{Participator, MsgKind, uuid::Uuid, MsgMeta, Route, RouteSpec, CmpSpec};

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

pub struct Hub {
    hub: Box<Bridge<Worker>>,
    spec: CmpSpec
}

impl Hub {    
    pub fn new(spec: CmpSpec, callback: Callback<Response>) -> Hub {
        let mut hub = Worker::bridge(callback);

        hub.send(Request::Auth(spec.rx.clone()));

        Hub {
            hub,
            spec
        }        
    }
    pub fn new_no_auth(spec: CmpSpec, callback: Callback<Response>) -> Hub {
        Hub {
            hub: Worker::bridge(callback),
            spec
        }        
    }
    pub fn auth(&mut self, spec: CmpSpec) {
        self.spec = spec;

        self.hub.send(Request::Auth(self.spec.rx.clone()));
    }
    pub fn send_event(&mut self, rx: &str, key: &str, payload: Value) {
        self.hub.send(Request::Msg(
            MsgMeta {
                tx: self.spec.rx.clone(),
                rx: rx.to_owned(),
                key: key.to_owned(),
                kind: MsgKind::Event,
                correlation_id: Uuid::new_v4(),
                route: Route {
                    source: Participator::Component(self.spec.clone()),
                    spec: RouteSpec::Simple
                },
                payload_size: 0,
                attachments: vec![]
            }, 
            payload
        ));
    }
    pub fn send_rpc(&mut self, rx: &str, key: &str, payload: Value) {
        self.hub.send(Request::Msg(
            MsgMeta {
                tx: self.spec.rx.clone(),
                rx: rx.to_owned(),
                key: key.to_owned(),
                kind: MsgKind::RpcRequest,
                correlation_id: Uuid::new_v4(),
                route: Route {
                    source: Participator::Component(self.spec.clone()),
                    spec: RouteSpec::Simple
                },
                payload_size: 0,
                attachments: vec![]
            },
            payload
        ));
    }    
    pub fn proxy_msg(&mut self, rx: &str, msg_meta: MsgMeta, payload: Value) {
        self.hub.send(Request::Msg(
            MsgMeta {
                tx: self.spec.rx.clone(),
                rx: rx.to_owned(),
                key: msg_meta.key,
                kind: msg_meta.kind,
                correlation_id: msg_meta.correlation_id,
                route: msg_meta.route,
                payload_size: 0,
                attachments: vec![]
            },
            payload
        ));
    }
    pub fn send_event_tx(&mut self, key: &str, payload: Value) {
        self.hub.send(Request::Msg(
            MsgMeta {
                tx: self.spec.rx.clone(),
                rx: self.spec.tx.clone(),
                key: key.to_owned(),
                kind: MsgKind::Event,
                correlation_id: Uuid::new_v4(),
                route: Route {
                    source: Participator::Component(self.spec.clone()),
                    spec: RouteSpec::Simple
                },
                payload_size: 0,
                attachments: vec![]
            }, 
            payload
        ));
    }    
    pub fn send_rpc_tx(&mut self, key: &str, payload: Value) {
        self.hub.send(Request::Msg(
            MsgMeta {
                tx: self.spec.rx.clone(),
                rx: self.spec.tx.clone(),
                key: key.to_owned(),
                kind: MsgKind::RpcRequest,
                correlation_id: Uuid::new_v4(),
                route: Route {
                    source: Participator::Component(self.spec.clone()),
                    spec: RouteSpec::Simple
                },
                payload_size: 0,
                attachments: vec![]
            },
            payload
        ));
    }    
    pub fn proxy_msg_tx(&mut self, msg_meta: MsgMeta, payload: Value) {
        self.hub.send(Request::Msg(
            MsgMeta {
                tx: self.spec.rx.clone(),
                rx: self.spec.tx.clone(),
                key: msg_meta.key,
                kind: msg_meta.kind,
                correlation_id: msg_meta.correlation_id,
                route: msg_meta.route,
                payload_size: 0,
                attachments: vec![]
            },
            payload
        ));
    }
    pub fn send_event_app(&mut self, key: &str, payload: Value) {
        self.hub.send(Request::Msg(
            MsgMeta {
                tx: self.spec.rx.clone(),
                rx: self.spec.app_addr.clone(),
                key: key.to_owned(),
                kind: MsgKind::Event,
                correlation_id: Uuid::new_v4(),
                route: Route {
                    source: Participator::Component(self.spec.clone()),
                    spec: RouteSpec::Simple
                },
                payload_size: 0,
                attachments: vec![]
            }, 
            payload
        ));
    }    
    pub fn send_rpc_app(&mut self, key: &str, payload: Value) {
        self.hub.send(Request::Msg(
            MsgMeta {
                tx: self.spec.rx.clone(),
                rx: self.spec.app_addr.clone(),
                key: key.to_owned(),
                kind: MsgKind::RpcRequest,
                correlation_id: Uuid::new_v4(),
                route: Route {
                    source: Participator::Component(self.spec.clone()),
                    spec: RouteSpec::Simple
                },
                payload_size: 0,
                attachments: vec![]
            },
            payload
        ));
    }    
    pub fn proxy_msg_app(&mut self, msg_meta: MsgMeta, payload: Value) {
        self.hub.send(Request::Msg(
            MsgMeta {
                tx: self.spec.rx.clone(),
                rx: self.spec.app_addr.clone(),
                key: msg_meta.key,
                kind: msg_meta.kind,
                correlation_id: msg_meta.correlation_id,
                route: msg_meta.route,
                payload_size: 0,
                attachments: vec![]
            },
            payload
        ));
    }
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
