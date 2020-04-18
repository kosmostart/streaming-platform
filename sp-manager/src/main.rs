use std::{thread, time};
use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use std::net::SocketAddr;
use serde_derive::Deserialize;
use sysinfo::{ProcessExt, SystemExt};
use tokio::runtime::Runtime;
use warp::Filter;

#[derive(Debug, Deserialize)]
struct Hub {
    file_name: Option<String>,
    config: Option<toml::Value>
}

#[derive(Debug, Deserialize)]
struct Service {
    file_name: Option<String>,
    config: Option<toml::Value>
}

struct Process {
    pub id: usize,
    pub name: String
}

struct StartedProcess {    
    pub name: String,
    pub r#type: ProcessType,
    pub instance: std::process::Child
}

#[derive(Debug, Clone)]
enum ProcessType {
    Hub,
    Service
}

enum Msg {
    StartServiceProcess(String, crossbeam::channel::Sender<String>),
    StartHubProcess(String, crossbeam::channel::Sender<String>),
    StopProcess(String, crossbeam::channel::Sender<String>),
    StopAll(crossbeam::channel::Sender<String>)
}

/// This is what we're going to decode into. Each field is optional, meaning
/// that it doesn't have to be present in TOML.
#[derive(Debug, Deserialize)]
struct Config {
    hub_path: Option<String>,
    service_path: Option<String>,
    hubs: Option<Vec<Hub>>,
    services: Option<Vec<Service>>
}

fn main() {        
    let config_path = std::env::args().nth(1)
        .expect("path to config file not passed as argument");

    let file = File::open(config_path)
        .expect("failed to open config");

    let mut buf_reader = BufReader::new(file);

    let mut config = String::new();

    buf_reader.read_to_string(&mut config)
        .expect("failed to read config");

    let config: Config = toml::from_str(&config)
        .expect("failed to deserialize config");    

    println!("quering system data");
    let mut system = sysinfo::System::new();
    // First we update all information of our system struct.
    system.refresh_all();
    let mut running = vec![];
    for (pid, proc_) in system.get_processes() {
        //println!("{}:{} => status: {:?}", pid, proc_.name(), proc_.status());

        running.push(Process {
            id: *pid as usize,
            name: proc_.name().to_owned()
        });
    }
    println!("stopping started processes if any are running");

    fix_running_self(&running);

    match config.hubs {
        Some(ref hubs) => {
            let hub_path = config.hub_path.clone()
                .expect("hub path is empty, but hubs are present");

            for hub in hubs {
                match &hub.file_name {
                    Some(file_name) => {                        
                        fix_running(&running, &file_name);
                    }
                    None => {
                    }
                }
            }
        }
        None => {

        }
    }

    match config.services {
        Some(ref services) => {
            let service_path = config.service_path.clone()
                .expect("service path is empty, but services are present");

            for service in services {
                match &service.file_name {
                    Some(file_name) => {
                        fix_running(&running, &file_name);
                    }
                    None => {                        
                    }
                }
            }
        }
        None => {            
        }
    }
    
    println!("done");
    
    println!("waiting for socket clean up");

    thread::sleep(time::Duration::from_millis(5000));

    println!("done");

    let (started_tx, started_rx) = crossbeam::channel::unbounded();

    let start_tx = started_tx.clone();
    let start_hub_tx = started_tx.clone();
    let stop_tx = started_tx.clone();
    let stop_tx2 = started_tx.clone();

    let started_processes_handle = std::thread::Builder::new()
        .name("started-processes".to_owned())
        .spawn(move || {
            println!("quering system data");
            let mut system = sysinfo::System::new();
            // First we update all information of our system struct.
            system.refresh_all();
            let mut running = vec![];
            for (pid, proc_) in system.get_processes() {
                //println!("{}:{} => status: {:?}", pid, proc_.name(), proc_.status());

                running.push(Process {
                    id: *pid as usize,
                    name: proc_.name().to_owned()
                });
            }            
            let mut started = std::collections::HashMap::new();

            match config.hubs {
                Some(ref hubs) => {
                    let hub_path = config.hub_path.clone()
                        .expect("hub path is empty, but hubs are present");

                    for hub in hubs {

                        match &hub.file_name {
                            Some(file_name) => {
                                
                                fix_running(&running, &file_name);

                                println!("starting {}", file_name);

                                let instance = match &hub.config {
                                    Some(config) => {
                                        std::process::Command::new(hub_path.clone() + "/" + &file_name)
                                            .arg(toml::to_string(&config)
                                                .expect("serialization to TOML string failed, check hub config")
                                            )
                                            .spawn()
                                            .expect(&format!("{} command failed to start", file_name))
                                    }
                                    None => {
                                        std::process::Command::new(hub_path.clone() + "/" + &file_name)
                                            .spawn()
                                            .expect(&format!("{} command failed to start", file_name))
                                    }
                                };

                                started.insert(file_name.clone(), StartedProcess {
                                    name: file_name.clone(),
                                    r#type: ProcessType::Hub,
                                    instance
                                });

                                println!("done starting {}", file_name);
                            }
                            None => {
                                println!("hub with empty file name, please note");
                            }
                        }

                    }
                }
                None => {
                    println!("no hubs are configured to run");
                }
            }

            match config.services {
                Some(ref services) => {
                    let service_path = config.service_path.clone()
                        .expect("service path is empty, but services are present");

                    for service in services {
                        match &service.file_name {
                            Some(file_name) => {

                                fix_running(&running, &file_name);

                                println!("starting {}", file_name);

                                let instance = match &service.config {
                                    Some(config) => {
                                        std::process::Command::new(service_path.clone() + "/" + &file_name)
                                            .arg(toml::to_string(&config)
                                                .expect("serialization to TOML string failed, check service config")
                                            )
                                            .spawn()
                                            .expect(&format!("{} command failed to start", file_name))
                                    }
                                    None => {
                                        std::process::Command::new(service_path.clone() + "/" + &file_name)                                    
                                            .spawn()
                                            .expect(&format!("{} command failed to start", file_name))
                                    }
                                };

                                started.insert(file_name.clone(), StartedProcess {
                                    name: file_name.clone(),
                                    r#type: ProcessType::Service,
                                    instance
                                });

                                println!("done starting {}", file_name);
                            }
                            None => {
                                println!("service with empty file name, please note");
                            }
                        }
                    }

                }
                None => {
                    println!("no services are configured to run");
                }
            }

            loop {
                let msg = started_rx.recv().unwrap();

                match msg {
                    Msg::StartServiceProcess(name, reply_tx) => {
                        system.refresh_all();

                        let mut running = vec![];

                        for (pid, proc_) in system.get_processes() {
                            //println!("{}:{} => status: {:?}", pid, proc_.name(), proc_.status());

                            running.push(Process {
                                id: *pid as usize,
                                name: proc_.name().to_owned()
                            });
                        }

                        println!("starting {}", name);

                        match &config.services {
                            Some(services) => {
                                
                                let service_path = config.service_path.clone()
                                    .expect("service path is empty, but services are present");                                

                                match services.iter().find(|x| x.file_name == Some(name.clone())) {
                                    Some(service) => {

                                        fix_running(&running, &name);

                                        println!("starting {}", name);

                                        let instance = match &service.config {
                                            Some(config) => {
                                                std::process::Command::new(service_path.clone() + "/" + &name)
                                                    .arg(toml::to_string(&config)
                                                        .expect("serialization to TOML string failed, check service config")
                                                    )
                                                    .spawn()
                                                    .expect(&format!("{} command failed to start", name))
                                            }
                                            None => {
                                                std::process::Command::new(service_path.clone() + "/" + &name)                                    
                                                    .spawn()
                                                    .expect(&format!("{} command failed to start", name))
                                            }
                                        };

                                        let process_type = ProcessType::Service;

                                        started.insert(name.clone(), StartedProcess {
                                            name: name.clone(),
                                            r#type: process_type.clone(),
                                            instance
                                        });

                                        println!("done starting {:?} {}", process_type, name);
                                    }
                                    None => println!("service {} not found in config", name)
                                }
                            }
                            None => println!("empty config for services")
                        }

                        reply_tx.send("Ok".to_owned());
                    }
                    Msg::StartHubProcess(name, reply_tx) => {
                        system.refresh_all();

                        let mut running = vec![];

                        for (pid, proc_) in system.get_processes() {
                            //println!("{}:{} => status: {:?}", pid, proc_.name(), proc_.status());

                            running.push(Process {
                                id: *pid as usize,
                                name: proc_.name().to_owned()
                            });
                        }

                        println!("starting {}", name);

                        match &config.hubs {
                            Some(hubs) => {
                                
                                let hub_path = config.hub_path.clone()
                                    .expect("hub path is empty, but hubs are present");

                                match hubs.iter().find(|x| x.file_name == Some(name.clone())) {
                                    Some(hub) => {

                                        fix_running(&running, &name);

                                        println!("starting {}", name);

                                        let instance = match &hub.config {
                                            Some(config) => {
                                                std::process::Command::new(hub_path.clone() + "/" + &name)
                                                    .arg(toml::to_string(&config)
                                                        .expect("serialization to TOML string failed, check service config")
                                                    )
                                                    .spawn()
                                                    .expect(&format!("{} command failed to start", name))
                                            }
                                            None => {
                                                std::process::Command::new(hub_path.clone() + "/" + &name)
                                                    .spawn()
                                                    .expect(&format!("{} command failed to start", name))
                                            }
                                        };

                                        let process_type = ProcessType::Hub;

                                        started.insert(name.clone(), StartedProcess {
                                            name: name.clone(),
                                            r#type: process_type.clone(),
                                            instance
                                        });

                                        println!("done starting {:?} {}", process_type, name);
                                    }
                                    None => println!("hub {} not found in config", name)
                                }
                            }
                            None => println!("empty config for hubs")
                        }

                        reply_tx.send("Ok".to_owned());
                    }
                    Msg::StopProcess(name, reply_tx) => {
                        println!("stopping {}", name);

                        match started.get_mut(&name) {
                            Some(process) => {
                                println!("found {} in started", name);
                                let res = process.instance.kill();
                                println!("stop result for {} {:?}", name, res);
                            }
                            None => {
                                println!("{} not founded in started", name);
                            }
                        }

                        reply_tx.send("Ok".to_owned());
                    }
                    Msg::StopAll(reply_tx) => {
                        println!("stopping all started processes");

                        for (name, process) in started.iter_mut() {
                            println!("found {} in started", name);
                            let res = process.instance.kill();
                            println!("stop result for {} {:?}", name, res);
                        }
                            
                        reply_tx.send("Ok".to_owned());
                    }
                }
                
            }
        })
        .unwrap();

    match std::env::args().nth(2) {
        Some(_) => {
            println!("starting command server");

            let routes = 
                warp::path("hello")
                    .and(warp::header("user-agent"))
                    .map(|agent: String| {
                        format!("Hello, your agent is {}", agent)
                    })            
                .or(
                    warp::path("start")
                        .and(warp::path::param())
                        .map(move |name: String| {
                            let (reply_tx, reply_rx) = crossbeam::channel::unbounded();

                            start_tx.clone().send(Msg::StartServiceProcess(name, reply_tx));

                            let reply = reply_rx.recv_timeout(std::time::Duration::from_secs(30)).unwrap();

                            reply
                        })
                )
                .or(
                    warp::path("start-hub")
                        .and(warp::path::param())
                        .map(move |name: String| {
                            let (reply_tx, reply_rx) = crossbeam::channel::unbounded();

                            start_hub_tx.clone().send(Msg::StartHubProcess(name, reply_tx));

                            let reply = reply_rx.recv_timeout(std::time::Duration::from_secs(30)).unwrap();

                            reply
                        })
                )
                .or(
                    warp::path("stop")
                        .and(warp::path::param())
                        .map(move |name: String| {
                            let (reply_tx, reply_rx) = crossbeam::channel::unbounded();

                            stop_tx.send(Msg::StopProcess(name, reply_tx));

                            let reply = reply_rx.recv_timeout(std::time::Duration::from_secs(30)).unwrap();

                            reply
                        })
                )
                .or(
                    warp::path("stop-all")                
                        .map(move || {
                            let (reply_tx, reply_rx) = crossbeam::channel::unbounded();

                            stop_tx2.send(Msg::StopAll(reply_tx));

                            let reply = reply_rx.recv_timeout(std::time::Duration::from_secs(30)).unwrap();

                            reply
                        })
                )
                ;
            
            let addr = "0.0.0.0:49999".parse::<SocketAddr>().unwrap();        
            let mut rt = Runtime::new().expect("failed to create runtime");    

            rt.block_on(warp::serve(routes).run(addr));
        }
        None => {
            println!("started");
            std::thread::park();
        }
    }
}

fn fix_running_self(running: &Vec<Process>) {
    let name = std::env::current_exe()
        .expect("failed to get current_exe result")
        .file_name()
        .expect("empty file name for current_exe")
        .to_str()
        .expect("failed to convert file name OsStr to str")
        .to_owned();

    println!("fixing running processes for {}", name);

    let id = std::process::id() as usize;

    for process in running {
        if process.name == name && process.id != id {
            stop_process_by_id(process.id, &process.name);
        }
    }

    println!("done for {}", name);
}

fn fix_running(running: &Vec<Process>, name: &str) {
    println!("fixing running processes for {}", name);

    for process in running {
        if process.name == name {
            stop_process_by_id(process.id, &process.name);
        }
    }

    println!("done for {}", name);
}

fn stop_process_by_id(id: usize, name: &str) {
    println!("attempt to stop process {} with id {}", name, id);

    if cfg!(windows) {
        std::process::Command::new("taskkill")
            .arg("/F")
            .arg("/PID")
            .arg(id.to_string())
            .output()
            .expect(&format!("process stop failed, {}, id {}", name, id));
    }
    else {
        std::process::Command::new("kill")            
            .arg(id.to_string())
            .output()
            .expect(&format!("process stop failed, {}, id {}", name, id));
    }

    println!("process stop ok, {}, id {}", name, id);
}
