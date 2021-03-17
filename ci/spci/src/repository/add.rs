use git2::{Index, IndexAddOption, Repository};

pub fn start() {
    let repo = Repository::open("d:/src/test").expect("failed to open");
    let mut index = repo.index().expect("cannot get the Index file");
    index.add_all(["*"].iter(), IndexAddOption::DEFAULT, None);
    index.write();
}