use std::collections::HashMap;
use anyhow::Error;
use serde_derive::{Serialize, Deserialize};
use serde_json::Value;
use yew::callback::Callback;
use yew::prelude::worker::*;
use yew::services::console::ConsoleService;
use yew::services::fetch::{self, FetchService, FetchTask};
use yew::agent::HandlerId;
use yew::format::Nothing;
use sp_dto::{Participator, MsgType, uuid::Uuid, MsgMeta, Route, RouteSpec, CmpSpec, rpc_dto_with_correlation_id, get_msg};

pub struct Worker {
    link: AgentLink<Worker>,
    clients: HashMap<String, HandlerId>,
    subscribes: HashMap<String, Vec<String>>,
    fetch_tasks: HashMap<Uuid, FetchTask>
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Auth(String),
    SetSubscribes(HashMap<String, Vec<String>>),
    Msg(MsgMeta, Value),
    Rpc(String, Uuid, Vec<u8>),
    PostStringRpc(String, Uuid, String),
    PostBinaryRpc(String, Uuid, Vec<u8>),
    GetStringRpc(String, Uuid),
    GetBinaryRpc(String, Uuid)
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    Msg(MsgMeta, Value),
    StringRpc(Uuid, String),
    BinaryRpc(Uuid, Vec<u8>)
}

pub enum Msg {    
    RpcReady(Vec<u8>, Uuid, String),
    StringRpcReady(String, Uuid, String),
    BinaryRpcReady(Vec<u8>, Uuid, String),
    FetchError(Error)
}

impl Worker {
    pub fn get_cb(&self, correlation_id: Uuid, client_addr: String) -> yew::Callback<fetch::Response<Result<Vec<u8>, Error>>> {

        self.link.callback(move |response: fetch::Response<Result<Vec<u8>, Error>>| {            
            let (_, data) = response.into_parts();
            match data {
                Ok(data) => Msg::RpcReady(data, correlation_id, client_addr.clone()),
                Err(err) => Msg::FetchError(err)
            }            
        })
    }
    pub fn get_string_cb(&self, correlation_id: Uuid, client_addr: String) -> yew::Callback<fetch::Response<Result<String, Error>>> {

        self.link.callback(move |response: fetch::Response<Result<String, Error>>| {            
            let (_, data) = response.into_parts();
            match data {
                Ok(data) => Msg::StringRpcReady(data, correlation_id, client_addr.clone()),
                Err(err) => Msg::FetchError(err)
            }            
        })
    }
    pub fn get_binary_cb(&self, correlation_id: Uuid, client_addr: String) -> yew::Callback<fetch::Response<Result<Vec<u8>, Error>>> {

        self.link.callback(move |response: fetch::Response<Result<Vec<u8>, Error>>| {
            let (_, data) = response.into_parts();
            match data {
                Ok(data) => Msg::BinaryRpcReady(data, correlation_id, client_addr.clone()),
                Err(err) => Msg::FetchError(err)
            }            
        })
    }
}

impl Agent for Worker {
    // Available:
    // - `Job` (one per bridge)
    // - `Context` (shared in the same thread)
    // - `Public` (separate thread).
    type Reach = Context<Self>;
    type Message = Msg;
    type Input = Request;
    type Output = Response;
    // Create an instance with a link to agent's environment.
    fn create(link: AgentLink<Self>) -> Self {        
        ConsoleService::log("Hub created");

        Worker {
            link,
            clients: HashMap::new(),
            subscribes: HashMap::new(),
            fetch_tasks: HashMap::new()
        }
    }
    // Handle inner messages (of services of `send_back` callbacks)
    fn update(&mut self, msg: Self::Message) {
        //self.console.log("hub: got update");
        match msg {
            Msg::RpcReady(data, correlation_id, addr) => {
                let (msg_meta, payload, _) = get_msg::<Value>(&data).expect("failed to get msg on FetchReady");
                self.fetch_tasks.remove(&correlation_id);                
                match msg_meta.msg_type {
                    MsgType::RpcResponse(_) => {
                        match msg_meta.route.spec {
                            RouteSpec::Simple => {
                                match msg_meta.source_cmp_addr() {
                                    Some(source_addr) => {
                                        match source_addr == addr {
                                            true => {
                                                match self.clients.get(&addr) {
                                                    Some(client_id) => self.link.respond(*client_id, Response::Msg(msg_meta, payload)),
                                                    None => ConsoleService::log(&format!("Hub: missing client {}", addr))
                                                }
                                            }
                                            false => {
                                                ConsoleService::log(&format!("error: message source addr differ from real source, message not delivered, {} vs {} (real one). This is possible security issue, please note.", source_addr, addr));
                                            }
                                        }                                        
                                    }
                                    None => {
                                        ConsoleService::log("Error: source cmp empty for this rpc");
                                    }
                                }
                            }
                            RouteSpec::Client(_) => {
                                match msg_meta.client_cmp_addr() {
                                    Some(addr) => {
                                        match self.clients.get(&addr) {
                                            Some(client_id) => self.link.respond(*client_id, Response::Msg(msg_meta, payload)),
                                            None => ConsoleService::log(&format!("Hub: missing client {}", addr))
                                        }
                                    }
                                    None => {
                                        ConsoleService::log("Error: client cmp empty for this rpc");
                                    }
                                }
                            }
                        }                                                
                    }
                    _ => {}
                }                
            }
            Msg::StringRpcReady(data, correlation_id, addr) => {
                self.fetch_tasks.remove(&correlation_id);
                match self.clients.get(&addr) {
                    Some(client_id) => self.link.respond(*client_id, Response::StringRpc(correlation_id, data)),
                    None => ConsoleService::log(&format!("Hub: missing client {}", addr))
                }
            }
            Msg::BinaryRpcReady(data, correlation_id, addr) => {
                self.fetch_tasks.remove(&correlation_id);
                match self.clients.get(&addr) {
                    Some(client_id) => self.link.respond(*client_id, Response::BinaryRpc(correlation_id, data)),
                    None => ConsoleService::log(&format!("Hub: missing client {}", addr))
                }
            }
            Msg::FetchError(err) => {
                ConsoleService::log(&format!("Error: fetch, {:?}", err));
            }
        }
    }
    // Handle incoming messages form components of other agents.
    fn handle_input(&mut self, msg: Self::Input, who: HandlerId) {
        //self.console.log(&format!("hub: {:?}", msg));        
        match msg {
            Request::Auth(addr) => {
                ConsoleService::log(&format!("Hub auth: {}", addr));
                self.clients.insert(addr, who);
            }
            Request::SetSubscribes(subscribes) => {
                self.subscribes = subscribes;
                ConsoleService::log("Subscribes set");
            }
            Request::Msg(msg_meta, payload) => {
                match self.subscribes.get(&msg_meta.key) {
                    Some(targets) => {
                        for target in targets {     
                            match self.clients.get(target) {
                                Some(client_id) => self.link.respond(*client_id, Response::Msg(msg_meta.clone(), payload.clone())),
                                None => ConsoleService::log(&format!("Hub: missing client {}", target))
                            }
                        }
                    }
                    None => ConsoleService::log(&format!("No subscribes found for key {}", msg_meta.key))
                }                
            }
            Request::Rpc(url, correlation_id, data) => {
                match self.clients.iter().find(|(_, x)| **x == who) {
                    Some((addr, _)) => {

                        let request = fetch::Request::post(url)        
                            .body(Ok(data))
                            .expect("Failed to build request.");

                        match FetchService::fetch_binary(request, self.get_cb(correlation_id, addr.clone())) {
                            Ok(task) => {
                                let _ = self.fetch_tasks.insert(correlation_id, task);
                            }
                            Err(e) => ConsoleService::log(&format!("error: error on fetch, {}", e))
                        }
                    }
                    None => {
                        ConsoleService::log("error: client not found by handler id")
                    }
                }                
            }
            Request::PostStringRpc(url, correlation_id, data) => {
                match self.clients.iter().find(|(_, x)| **x == who) {
                    Some((addr, _)) => {

                        let request = fetch::Request::post(url)        
                            .body(Ok(data))
                            .expect("Failed to build request.");
                            
                        match FetchService::fetch(request, self.get_string_cb(correlation_id, addr.clone())) {
                            Ok(task) => {
                                let _ = self.fetch_tasks.insert(correlation_id, task);
                            }
                            Err(e) => ConsoleService::log(&format!("error: error on fetch, simple string rpc, {}", e))
                        }
                    }
                    None => {
                        ConsoleService::log("error: client not found by handler id")
                    }
                }
            }
            Request::PostBinaryRpc(url, correlation_id, data) => {
                match self.clients.iter().find(|(_, x)| **x == who) {
                    Some((addr, _)) => {

                        let request = fetch::Request::post(url)        
                            .body(Ok(data))
                            .expect("Failed to build request.");

                        match FetchService::fetch_binary(request, self.get_binary_cb(correlation_id, addr.clone())) {
                            Ok(task) => {
                                let _ = self.fetch_tasks.insert(correlation_id, task);
                            }
                            Err(e) => ConsoleService::log(&format!("error: error on fetch, simple string rpc, {}", e))
                        }
                    }
                    None => {
                        ConsoleService::log("error: client not found by handler id")
                    }
                }
            }
            Request::GetStringRpc(url, correlation_id) => {
                match self.clients.iter().find(|(_, x)| **x == who) {
                    Some((addr, _)) => {

                        let request = fetch::Request::get(url)
                            .body(Nothing)
                            .expect("Failed to build request.");
                            
                        match FetchService::fetch(request, self.get_string_cb(correlation_id, addr.clone())) {
                            Ok(task) => {
                                let _ = self.fetch_tasks.insert(correlation_id, task);
                            }
                            Err(e) => ConsoleService::log(&format!("error: error on fetch, simple string rpc, {}", e))
                        }
                    }
                    None => {
                        ConsoleService::log("error: client not found by handler id")
                    }
                }
            }
            Request::GetBinaryRpc(url, correlation_id) => {
                match self.clients.iter().find(|(_, x)| **x == who) {
                    Some((addr, _)) => {

                        let request = fetch::Request::post(url)        
                            .body(Nothing)
                            .expect("Failed to build request.");

                        match FetchService::fetch_binary(request, self.get_binary_cb(correlation_id, addr.clone())) {
                            Ok(task) => {
                                let _ = self.fetch_tasks.insert(correlation_id, task);
                            }
                            Err(e) => ConsoleService::log(&format!("error: error on fetch, simple string rpc, {}", e))
                        }
                    }
                    None => {
                        ConsoleService::log("error: client not found by handler id")
                    }
                }
            }
        }
    }
}

/// This struct is attached to components and used for communication between components and also a sever.
pub struct Hub {
    hub: Box<dyn Bridge<Worker>>,
    pub spec: CmpSpec,
    pub cfg: HubCfg    
}

/// Configuration for various communication scenarios.
#[derive(Clone, PartialEq)]
pub struct HubCfg {
    pub app_addr: Option<String>,
    pub client_addr: Option<String>,
    pub host: Option<String>,
    pub fetch_url: Option<String>,
    pub ws_url: Option<String>,    
    pub auth_token: Option<String>,
    pub auth_data: Option<Value>
}

impl Default for HubCfg {
    fn default() -> Self {
        HubCfg {
            app_addr: None,
            client_addr: None,
            host: None,
            fetch_url: None,
            ws_url: None,            
            auth_token: None,
            auth_data: None
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
    pub fn set_subscribes(&mut self, subscribes: HashMap<String, Vec<String>>) {
        self.hub.send(Request::SetSubscribes(subscribes));
    }
    /// Sends rpc request to the server
    pub fn rpc(&mut self, key: &str, payload: Value) {
        let route = Route {
            source: Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone()),
            spec: RouteSpec::Simple,
            points: vec![Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone())]
        };
        let (correlation_id, dto) = rpc_dto_with_correlation_id(self.spec.addr.clone(), key.to_owned(), payload, route, self.cfg.auth_token.clone(), self.cfg.auth_data.clone()).expect("failed to create rpc dto with correlation id on server rpc");
        let url = self.cfg.fetch_url.clone().expect("fetch host is empty on server rpc");
        self.hub.send(Request::Rpc(url, correlation_id, dto));
    }
    /// Sends rpc request to the server, but result will forwarded to component with client_addr
    pub fn rpc_with_client(&mut self, key: &str, payload: Value, client_addr: String) {
        let route = Route {
            source: Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone()),
            spec: RouteSpec::Client(Participator::Component(client_addr, self.cfg.app_addr.clone(), self.cfg.client_addr.clone())),
            points: vec![Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone())]
        };
        let (correlation_id, dto) = rpc_dto_with_correlation_id(self.spec.addr.clone(), key.to_owned(), payload, route, self.cfg.auth_token.clone(), self.cfg.auth_data.clone()).expect("failed to create rpc dto with correlation id on server rpc");
        let host = self.cfg.fetch_url.clone().expect("fetch host is empty on server rpc");
        self.hub.send(Request::Rpc(host, correlation_id, dto));
    }    
    /// Sends rpc request to the server, iserting url segment to the resulting url.
    pub fn rpc_with_segment(&mut self, segment: &str,  key: &str, payload: Value) {
        let route = Route {
            source: Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone()),
            spec: RouteSpec::Simple,
            points: vec![Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone())]
        };
        let (correlation_id, dto) = rpc_dto_with_correlation_id(self.spec.addr.clone(), key.to_owned(), payload, route, self.cfg.auth_token.clone(), self.cfg.auth_data.clone()).expect("failed to create rpc dto with correlation id on server rpc");
        let url = self.cfg.host.clone().expect("fetch host is empty on server rpc") + "/" + segment + "/";
        self.hub.send(Request::Rpc(url, correlation_id, dto));
    }
    /// Sends message marked as event to other component.
    pub fn send_event_local(&mut self, key: &str, payload: Value) {
        self.hub.send(Request::Msg(
            MsgMeta {
                tx: self.spec.addr.clone(),                
                key: key.to_owned(),
                msg_type: MsgType::Event,
                correlation_id: Uuid::new_v4(),
                route: Route {
                    source: Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone()),
                    spec: RouteSpec::Simple,
                    points: vec![Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone())]
                },
                payload_size: 0,
                auth_token: self.cfg.auth_token.clone(),
                auth_data: self.cfg.auth_data.clone(),
                attachments: vec![]
            }, 
            payload
        ));
    }
    /// Sends message marked as rpc request to other component.
    pub fn send_rpc_local(&mut self, key: &str, payload: Value) {
        self.hub.send(Request::Msg(
            MsgMeta {
                tx: self.spec.addr.clone(),                
                key: key.to_owned(),
                msg_type: MsgType::RpcRequest,
                correlation_id: Uuid::new_v4(),
                route: Route {
                    source: Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone()),
                    spec: RouteSpec::Simple,
                    points: vec![Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone())]
                },
                payload_size: 0,
                auth_token: self.cfg.auth_token.clone(),
                auth_data: self.cfg.auth_data.clone(),
                attachments: vec![]
            },
            payload
        ));
    }
    /// Forwards current message to other component.
    pub fn proxy_msg_local(&mut self, mut msg_meta: MsgMeta, payload: Value) {
        msg_meta.route.points.push(Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone()));

        self.hub.send(Request::Msg(
            MsgMeta {
                tx: self.spec.addr.clone(),                
                key: msg_meta.key,
                msg_type: msg_meta.msg_type,
                correlation_id: msg_meta.correlation_id,
                route: msg_meta.route,
                payload_size: 0,
                auth_token: self.cfg.auth_token.clone(),
                auth_data: self.cfg.auth_data.clone(),
                attachments: vec![]
            },
            payload
        ));
    }
    /// Sends message marked as event to component with addr held inside tx variable of component spec (which is stored in a hub struct).
    pub fn send_event_tx(&mut self, key: &str, payload: Value) {
        self.hub.send(Request::Msg(
            MsgMeta {
                tx: self.spec.addr.clone(),                
                key: key.to_owned(),
                msg_type: MsgType::Event,
                correlation_id: Uuid::new_v4(),
                route: Route {
                    source: Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone()),
                    spec: RouteSpec::Simple,
                    points: vec![Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone())]
                },
                payload_size: 0,
                auth_token: self.cfg.auth_token.clone(),
                auth_data: self.cfg.auth_data.clone(),
                attachments: vec![]
            }, 
            payload
        ));
    }
    /// Sends message marked as rpc request to component with addr held inside tx variable of component spec (which is stored in a hub struct).
    pub fn send_rpc_tx(&mut self, key: &str, payload: Value) {
        self.hub.send(Request::Msg(
            MsgMeta {
                tx: self.spec.addr.clone(),                
                key: key.to_owned(),
                msg_type: MsgType::RpcRequest,
                correlation_id: Uuid::new_v4(),
                route: Route {
                    source: Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone()),
                    spec: RouteSpec::Simple,
                    points: vec![Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone())]
                },
                payload_size: 0,
                auth_token: self.cfg.auth_token.clone(),
                auth_data: self.cfg.auth_data.clone(),
                attachments: vec![]
            },
            payload
        ));
    }
    /// Forwards current message to component with addr held inside tx variable of component spec (which is stored in a hub struct).
    pub fn proxy_msg_tx(&mut self, mut msg_meta: MsgMeta, payload: Value) {
        msg_meta.route.points.push(Participator::Component(self.spec.addr.clone(), self.cfg.app_addr.clone(), self.cfg.client_addr.clone()));
        self.hub.send(Request::Msg(
            MsgMeta {
                tx: self.spec.addr.clone(),                
                key: msg_meta.key,
                msg_type: msg_meta.msg_type,
                correlation_id: msg_meta.correlation_id,
                route: msg_meta.route,
                payload_size: 0,
                auth_token: self.cfg.auth_token.clone(),
                auth_data: self.cfg.auth_data.clone(),
                attachments: vec![]
            },
            payload
        ));
    }

    /// Function for making request without using protocol described in sp-dto crate. Simple wrapper around fetch API.
    pub fn rpc_post_string(&mut self, payload: String) {
        let url = self.cfg.fetch_url.clone().expect("fetch host is empty on server rpc");
        self.hub.send(Request::PostStringRpc(url, Uuid::new_v4(), payload));
    }

    /// Function for making request without using protocol described in sp-dto crate. Simple wrapper around fetch API.
    pub fn rpc_post_binary(&mut self, payload: Vec<u8>) {
        let url = self.cfg.fetch_url.clone().expect("fetch host is empty on server rpc");
        self.hub.send(Request::PostBinaryRpc(url, Uuid::new_v4(), payload));
    }

    /// Function for making request without using protocol described in sp-dto crate. Simple wrapper around fetch API.
    pub fn rpc_post_string_custom_url(&mut self, url: String, payload: String) {        
        self.hub.send(Request::PostStringRpc(url, Uuid::new_v4(), payload));
    }

    /// Function for making request without using protocol described in sp-dto crate. Simple wrapper around fetch API.
    pub fn rpc_post_binary_custom_url(&mut self, url: String, payload: Vec<u8>) {        
        self.hub.send(Request::PostBinaryRpc(url, Uuid::new_v4(), payload));
    }

    /// Function for making request without using protocol described in sp-dto crate. Simple wrapper around fetch API.
    pub fn rpc_get_string_custom_url(&mut self, url: String) {
        self.hub.send(Request::GetStringRpc(url, Uuid::new_v4()));
    }

    /// Function for making request without using protocol described in sp-dto crate. Simple wrapper around fetch API.
    pub fn rpc_get_binary_custom_url(&mut self, url: String) {        
        self.hub.send(Request::GetBinaryRpc(url, Uuid::new_v4()));
    }
}