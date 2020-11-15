
use o1::set::Ref;

use crate::{
    core::{
        search_tree,
        SearchRangeBounds,
    },
};

pub struct Args {
    pub range: SearchRangeBounds,
    pub request_ref: Ref,
    pub search_tree_pid: search_tree::Pid,
}

pub struct Done {
    pub request_ref: Ref,
    pub items_iter: search_tree::SearchTreeIterItemsRx,
}

#[derive(Debug)]
pub enum Error {
    SearchTreeIter(search_tree::IterError),
}

pub async fn run(Args { request_ref, range, mut search_tree_pid, }: Args) -> Result<Done, Error> {
    let items_iter = search_tree_pid.iter(range).await
        .map_err(Error::SearchTreeIter)?;
    Ok(Done { request_ref, items_iter, })
}
