pub fn start() {
    let path_list = vec![
        "d:/src/test"
    ];

    for path in path_list {
        crate::repository::status::start(path);
    }
}