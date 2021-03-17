mod repository {
    pub mod status;
    pub mod pull;
    pub mod add;
    pub mod commit;
}

fn main() {
    repository::status::start();
    //pull::start();
    //add::start();
    //commit::start();
}