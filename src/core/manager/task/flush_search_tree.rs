
use o1::set::Ref;

use crate::{
    core::{
        search_tree,
    },
    Flushed,
};

pub struct Args {
    pub request_ref: Ref,
    pub search_tree_pid: search_tree::Pid,
}

pub struct Done {
    pub request_ref: Ref,
}

#[derive(Debug)]
pub enum Error {
    SearchTreeFlush(search_tree::FlushError),
}

pub async fn run(Args { request_ref, mut search_tree_pid, }: Args) -> Result<Done, Error> {
    let Flushed = search_tree_pid.flush().await
        .map_err(Error::SearchTreeFlush)?;
    Ok(Done { request_ref, })
}
