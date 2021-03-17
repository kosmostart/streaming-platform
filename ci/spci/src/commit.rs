use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use git2::{Repository, RepositoryInitOptions};

pub fn start() {
    let path = "d:/src/test";

    let mut opts = RepositoryInitOptions::new();
    //opts.initial_head("main");
    let repo = Repository::init_opts(&path, &opts).unwrap();

    //let mut config = repo.config().unwrap();
    //config.set_str("user.name", "name").unwrap();
    //config.set_str("user.email", "email").unwrap();

    let mut index = repo.index().unwrap();
    let id = index.write_tree().unwrap();

    let tree = repo.find_tree(id).unwrap();
    let sig = repo.signature().unwrap();

    let head_id = repo.refname_to_id("HEAD").unwrap();
    let parent = repo.find_commit(head_id).unwrap();

    repo.commit(Some("HEAD"), &sig, &sig, "initial", &tree, &[&parent])
        .unwrap();
}