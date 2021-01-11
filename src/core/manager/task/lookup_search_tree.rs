
use o1::set::Ref;

use crate::{
    kv,
    storage,
    core::{
        search_tree,
    },
};

pub struct Args {
    pub key: kv::Key,
    pub request_ref: Ref,
    pub search_tree_pid: search_tree::Pid,
}

pub struct Done {
    pub request_ref: Ref,
    pub found: Option<kv::ValueCell<storage::OwnedValueBlockRef>>,
}

#[derive(Debug)]
pub enum Error {
    SearchTreeLookup(search_tree::LookupError),
}

pub async fn run(Args { request_ref, key, mut search_tree_pid, }: Args) -> Result<Done, Error> {
    let search_tree_found = search_tree_pid.lookup(key).await
        .map_err(Error::SearchTreeLookup)?;
    Ok(Done { request_ref, found: search_tree_found, })
}
