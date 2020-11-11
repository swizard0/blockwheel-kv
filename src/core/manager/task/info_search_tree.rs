
use o1::set::Ref;

use crate::{
    core::{
        search_tree,
    },
    Info,
};

pub struct Args {
    pub request_ref: Ref,
    pub search_tree_pid: search_tree::Pid,
}

pub struct Done {
    pub request_ref: Ref,
    pub info: Info,
}

#[derive(Debug)]
pub enum Error {
    SearchTreeInfo(ero::NoProcError),
}

pub async fn run(Args { request_ref, mut search_tree_pid, }: Args) -> Result<Done, Error> {
    let info = search_tree_pid.info().await
        .map_err(Error::SearchTreeInfo)?;
    Ok(Done { request_ref, info, })
}
