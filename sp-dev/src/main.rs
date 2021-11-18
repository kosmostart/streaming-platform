use std::time::Duration;
use std::process::Child;
use std::io::{
	BufReader, Read
};
use log::*;
use serde_derive::Deserialize;
use serde_json::{
	from_str
};
use sysinfo::{
	SystemExt, ProcessExt
};

#[derive(Debug, Deserialize)]
struct RunUnit {
	pub path: String,
	pub name: String
}

fn main() {
	env_logger::init();

	let config_path = std::env::args().nth(1).expect("Config path not passed.");

	info!("Received cfg path: {}", config_path);

	let file = std::fs::File::open(config_path).expect("Failed to open config file");

    let mut buf_reader = BufReader::new(file);
    let mut config = String::new();

    buf_reader.read_to_string(&mut config).expect("Failed to read config");

	let config: Vec<RunUnit> = from_str(&config).expect("Failed to deserialize config");

	info!("{:#?}", config);
	info!("Quering system data");

    let mut system = sysinfo::System::new();
    // First we update all information of our system struct.
    system.refresh_all();
    let mut running = vec![];

    for (id, process) in system.processes() {
        //info!("{}:{} status: {:?}", pid, proc_.name(), proc_.status());

        running.push((*id as usize, process.name().to_owned()));
    }

    info!("Processes running: {}", running.len());

	for run_unit in &config {
		start(&run_unit.path, &run_unit.name, &running);
	}
}

fn start(path: &str, name: &str, running: &Vec<(usize, String)>) -> Child {
	info!("Starting {}, {}", path, name);
	
	fix_running(name, running);

	let res = std::process::Command::new(path)
		//.stdin(std::process::Stdio::piped())
		.stdout(std::process::Stdio::piped())
		//.stderr(std::process::Stdio::piped())
		.spawn()
		.expect("Failed to start build command");

	std::thread::sleep(Duration::new(10, 0));

	res
}

fn fix_running(name: &str, running: &Vec<(usize, String)>) {
    info!("Fixing running processes for {}", name);    

    for (id, process_name) in running {
        if process_name == name {
            stop_process_by_id(*id, process_name);
        }
    }

    info!("Done for {}", name);
}

fn stop_process_by_id(id: usize, name: &str) {
    info!("Attempt to stop process {} with id {}", name, id);

    if cfg!(windows) {
        std::process::Command::new("taskkill")
            .arg("/F")
            .arg("/PID")
            .arg(id.to_string())
            .output()
            .expect(&format!("Process stop failed, name {}, id {}", name, id));
    }
    else {
        std::process::Command::new("kill")            
            .arg(id.to_string())
            .output()
            .expect(&format!("Process stop failed, name {}, id {}", name, id));
    }

    info!("Process stop result is Ok, name {}, id {}", name, id);
}