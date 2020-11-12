use crate::{
    core::{
        search_tree,
    },
};

pub struct Args {
    pub search_tree_pid: search_tree::Pid,
}

pub struct Done;

#[derive(Debug)]
pub enum Error {
    SearchTreeDemolish(search_tree::DemolishError),
}

pub async fn run(Args { mut search_tree_pid, }: Args) -> Result<Done, Error> {
    log::debug!("spawned task, requesting demolish");
    let search_tree::Demolished = search_tree_pid.demolish().await
        .map_err(Error::SearchTreeDemolish)?;
    log::debug!("task done");
    Ok(Done)
}
