use futures::{
    channel::{
        mpsc,
    },
};

use alloc_pool::{
    pool,
    Shared,
    Unique,
};

use crate::{
    kv,
    core::{
        merger,
        search_tree,
    },
    KeyValueStreamItem,
};

pub struct Args {
    pub key_values_tx: mpsc::Sender<KeyValueStreamItem>,
    pub iter_items: Shared<Vec<kv::KeyValuePair>>,
    pub merger_iters: Unique<Vec<merger::KeyValuesIter>>,
}

pub struct Done;

#[derive(Debug)]
pub enum Error {
    Merger(merger::Error),
}

pub async fn run(
    Args {
        key_values_tx,
        iter_items,
        merger_iters,
    }: Args,
)
    -> Result<Done, Error>
{

    unimplemented!();
    Ok(Done)
}
