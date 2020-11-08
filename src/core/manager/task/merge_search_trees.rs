
use o1::set::Ref;

use crate::{
    kv,
    wheels,
    core::{
        search_tree,
        BlockRef,
    },
};

pub struct Args {
    pub search_tree_a_ref: Ref,
    pub search_tree_b_ref: Ref,
    pub search_tree_a_pid: search_tree::Pid,
    pub search_tree_b_pid: search_tree::Pid,
    pub wheels_pid: wheels::Pid,
}

pub struct Done {
    pub search_tree_a_ref: Ref,
    pub search_tree_b_ref: Ref,
    pub root_block: BlockRef,
    pub items_count: usize,
}

#[derive(Debug)]
pub enum Error {
}

pub async fn run(
    Args {
        search_tree_a_ref,
        search_tree_b_ref,
        search_tree_a_pid,
        search_tree_b_pid,
        wheels_pid,
    }: Args,
)
    -> Result<Done, Error>
{

    let root_block = unimplemented!();
    let items_count = unimplemented!();

    Ok(Done {
        search_tree_a_ref,
        search_tree_b_ref,
        root_block,
        items_count,
    })
}
