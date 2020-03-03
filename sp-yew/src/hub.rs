use std::collections::HashMap;
use anyhow::Error;
use serde_derive::{Serialize, Deserialize};
use serde_json::Value;
use yew::callback::Callback;
use yew::prelude::worker::*;
use yew::services::console::ConsoleService;
use yew::services::fetch::{self, FetchService, FetchTask};
use yew::agent::HandlerId;
use sp_dto::{Participator, MsgKind, uuid::Uuid, MsgMeta, Route, RouteSpec, CmpSpec, rpc_dto_with_correlation_id, get_msg};

pub struct Worker {
    link: AgentLink<Worker>,
    clients: HashMap<String, HandlerId>,
    console: ConsoleService,
    fetch_service: FetchService,
    fetch_cb: yew::Callback<fetch::Response<Result<Vec<u8>, Error>>>,
    fetch_tasks: HashMap<Uuid, FetchTask>
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Auth(String),
    Msg(MsgMeta, Value),
    Rpc(String, Uuid, Vec<u8>)
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    Msg(MsgMeta, Value)    
}

pub struct Hub {
    hub: Box<dyn Bridge<Worker>>,
    pub spec: CmpSpec,
    pub cfg: HubCfg
}

#[derive(Clone, PartialEq)]
pub struct HubCfg {
    pub app_addr: Option<String>,
    pub client_addr: Option<String>,
    pub host: Option<String>,
    pub fetch_url: Option<String>,
    pub ws_url: Option<String>,
    pub domain: Option<String>
}

impl Default for HubCfg {
    fn default() -> Self {
        HubCfg {
            app_addr: None,
            client_addr: None,
            host: None,
            fetch_url: None,
            ws_url: None,
            domain: None
        }        
    }
}

impl Hub {    
    pub fn new(spec: CmpSpec, cfg: HubCfg, callback: Callback<Response>) -> Hub {
        let mut hub = Worker::bridge(callback);
        hub.send(Request::Auth(spec.addr.clone()));
        Hub {
            hub,
            spec,
            cfg
        }        
    }
    pub fn new_no_auth(spec: CmpSpec, cfg: HubCfg, callback: Callback<Response>) -> Hub {
        Hub {
            hub: Worker::bridge(callback),
            spec,
            cfg
        }        
    }
    pub fn auth(&mut self, spec: CmpSpec, cfg: HubCfg) {
        self.spec = spec;
        self.cfg = cfg;
        self.hub.send(Request::Auth(self.spec.addr.clone()));
    }
    pub fn rpc(&mut self, addr: &str, key: &str, payload: Value) {
        let route = Route {
            source: Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone()),
            spec: RouteSpec::Simple,
            points: vec![Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone())]
        };
        let (correlation_id, dto) = rpc_dto_with_correlation_id(self.spec.addr.clone(), addr.to_owned(), key.to_owned(), payload, route).expect("failed to create rpc dto with correlation id on server rpc");
        let url = self.cfg.fetch_url.clone().expect("fetch host is empty on server rpc");
        self.hub.send(Request::Rpc(url, correlation_id, dto));
    }
    pub fn rpc_with_client(&mut self, addr: &str, key: &str, payload: Value, client_addr: String) {
        let route = Route {
            source: Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone()),
            spec: RouteSpec::Client(Participator::Component(client_addr, self.cfg.app_addr.clone(), self.cfg.client_addr.clone())),
            points: vec![Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone())]
        };
        let (correlation_id, dto) = rpc_dto_with_correlation_id(self.spec.addr.clone(), addr.to_owned(), key.to_owned(), payload, route).expect("failed to create rpc dto with correlation id on server rpc");
        let host = self.cfg.fetch_url.clone().expect("fetch host is empty on server rpc");
        self.hub.send(Request::Rpc(host, correlation_id, dto));
    }
    pub fn rpc_with_domain(&mut self, addr: &str, key: &str, payload: Value) {
        match &self.cfg.domain {
            Some(domain) => {
                let addr = addr.to_owned() + "." + domain;
                let route = Route {
                    source: Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone()),
                    spec: RouteSpec::Simple,
                    points: vec![Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone())]
                };
                let (correlation_id, dto) = rpc_dto_with_correlation_id(self.spec.addr.clone(), addr, key.to_owned(), payload, route).expect("failed to create rpc dto with correlation id on server rpc");
                let host = self.cfg.fetch_url.clone().expect("fetch host is empty on server rpc");

                self.hub.send(Request::Rpc(host, correlation_id, dto));
            }
            None => panic!(format!("domain is empty on server rpc with domain call {}", self.spec.addr))
        }        
    }
    pub fn rpc_with_segment(&mut self, segment: &str, addr: &str, key: &str, payload: Value) {
        let route = Route {
            source: Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone()),
            spec: RouteSpec::Simple,
            points: vec![Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone())]
        };
        let (correlation_id, dto) = rpc_dto_with_correlation_id(self.spec.addr.clone(), addr.to_owned(), key.to_owned(), payload, route).expect("failed to create rpc dto with correlation id on server rpc");
        let url = self.cfg.host.clone().expect("fetch host is empty on server rpc") + "/" + segment + "/";
        self.hub.send(Request::Rpc(url, correlation_id, dto));
    }
    pub fn send_event_local(&mut self, addr: &str, key: &str, payload: Value) {
        self.hub.send(Request::Msg(
            MsgMeta {
                tx: self.spec.addr.clone(),
                rx: addr.to_owned(),
                key: key.to_owned(),
                kind: MsgKind::Event,
                correlation_id: Uuid::new_v4(),
                route: Route {
                    source: Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone()),
                    spec: RouteSpec::Simple,
                    points: vec![Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone())]
                },
                payload_size: 0,
                attachments: vec![]
            }, 
            payload
        ));
    }
    pub fn send_rpc_local(&mut self, rx: &str, key: &str, payload: Value) {
        self.hub.send(Request::Msg(
            MsgMeta {
                tx: self.spec.addr.clone(),
                rx: rx.to_owned(),
                key: key.to_owned(),
                kind: MsgKind::RpcRequest,
                correlation_id: Uuid::new_v4(),
                route: Route {
                    source: Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone()),
                    spec: RouteSpec::Simple,
                    points: vec![Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone())]
                },
                payload_size: 0,
                attachments: vec![]
            },
            payload
        ));
    }    
    pub fn proxy_msg_local(&mut self, rx: &str, mut msg_meta: MsgMeta, payload: Value) {
        msg_meta.route.points.push(Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone()));

        self.hub.send(Request::Msg(
            MsgMeta {
                tx: self.spec.addr.clone(),
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
                tx: self.spec.addr.clone(),
                rx: self.spec.tx.clone(),
                key: key.to_owned(),
                kind: MsgKind::Event,
                correlation_id: Uuid::new_v4(),
                route: Route {
                    source: Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone()),
                    spec: RouteSpec::Simple,
                    points: vec![Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone())]
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
                tx: self.spec.addr.clone(),
                rx: self.spec.tx.clone(),
                key: key.to_owned(),
                kind: MsgKind::RpcRequest,
                correlation_id: Uuid::new_v4(),
                route: Route {
                    source: Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone()),
                    spec: RouteSpec::Simple,
                    points: vec![Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone())]
                },
                payload_size: 0,
                attachments: vec![]
            },
            payload
        ));
    }    
    pub fn proxy_msg_tx(&mut self, mut msg_meta: MsgMeta, payload: Value) {
        msg_meta.route.points.push(Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone()));
        self.hub.send(Request::Msg(
            MsgMeta {
                tx: self.spec.addr.clone(),
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
}

pub enum Msg {    
    FetchReady(Vec<u8>),
    FetchError(Error)
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
        let fetch_cb = link.callback(move |response: fetch::Response<Result<Vec<u8>, Error>>| {
            let (meta, data) = response.into_parts();
            println!("{:?}", meta);
            match data {
                Ok(data) => Msg::FetchReady(data),
                Err(err) => Msg::FetchError(err)
            }            
        });
        Worker { 
            link,
            clients: HashMap::new(),            
            console,
            fetch_service: FetchService::new(),
            fetch_cb,
            fetch_tasks: HashMap::new()
        }
    }
    // Handle inner messages (of services of `send_back` callbacks)
    fn update(&mut self, msg: Self::Message) { /* ... */ 
        //self.console.log("hub: got update");
        match msg {
            Msg::FetchReady(data) => {
                let (msg_meta, payload, _) = get_msg::<Value>(&data).expect("failed to get msg on FetchReady");
                self.fetch_tasks.remove(&msg_meta.correlation_id);
                //self.console.log(&msg_meta.display());
                match msg_meta.kind {
                    MsgKind::RpcResponse(_) => {
                        match msg_meta.route.spec {
                            RouteSpec::Simple => {
                                match msg_meta.source_cmp_addr() {
                                    Some(addr) => {
                                        match self.clients.get(addr) {
                                            Some(client_id) => self.link.respond(*client_id, Response::Msg(msg_meta, payload)),
                                            None => self.console.log(&format!("hub: missing client {}", msg_meta.rx))
                                        }
                                    }
                                    None => {
                                        self.console.log("error: source cmp empty for this rpc");
                                    }
                                }
                            }
                            RouteSpec::Client(_) => {
                                match msg_meta.client_cmp_addr() {
                                    Some(addr) => {
                                        match self.clients.get(&addr) {
                                            Some(client_id) => self.link.respond(*client_id, Response::Msg(msg_meta, payload)),
                                            None => self.console.log(&format!("hub: missing client {}", addr))
                                        }
                                    }
                                    None => {
                                        self.console.log("error: client cmp empty for this rpc");
                                    }
                                }
                            }
                        }                                                
                    }
                    _ => {}
                }                
            }
            Msg::FetchError(err) => {
                self.console.log(&format!("error: fetch, {:?}", err));
            }
        }
    }
    // Handle incoming messages form components of other agents.
    fn handle_input(&mut self, msg: Self::Input, who: HandlerId) {
        //self.console.log(&format!("hub: {:?}", msg));        
        match msg {
            Request::Auth(addr) => {
                self.console.log(&format!("hub auth: {}", addr));
                self.clients.insert(addr, who);
            }
            Request::Msg(msg_meta, payload) => {
                match self.clients.get(&msg_meta.rx) {
                    Some(client_id) => self.link.respond(*client_id, Response::Msg(msg_meta, payload)),
                    None => self.console.log(&format!("hub: missing client {}", msg_meta.rx))
                }
            }
            Request::Rpc(url, correlation_id, data) => {
                let request = fetch::Request::post(url)        
                    .body(Ok(data))
                    .expect("Failed to build request.");
                match self.fetch_service.fetch_binary(request, self.fetch_cb.clone()) {
                    Ok(task) => {
                        let _ = self.fetch_tasks.insert(correlation_id, task);
                    }
                    Err(e) => self.console.log(&format!("error: error on fetch, {}", e))
                }
            }
        }
    }
}
