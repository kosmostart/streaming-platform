pub fn start_ui() {
    let path_list = vec![
        "d:/src/test"
    ];

    for path in path_list {
        crate::repository::status::start(path);
    }
}

/*
pub fn start_ci() {
    pull();
    build();
    deploy();
}

fn pull() {
    crate::repository::pull::start(path);
}

fn build() {
    let cmd = "";

    run_cmd(cmd);
}

fn deploy() {
    let source = "";
    let destination = "";

    let pack = pack(source);

    send(destination);
}
*/

/*
fn receive() {
    unpack();
    restart();
}

fn clean_task() {
    copy_to_arch();
    delete_previous();
}
*/