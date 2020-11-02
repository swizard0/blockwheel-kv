use alloc_pool::bytes::Bytes;

use crate::{
    core::{
        BlockRef,
        search_tree::{
            task::{
                RequestsQueue,
                SearchOutcomes,
            },
        },
    },
};

pub struct Args {
    pub block_ref: BlockRef,
    pub block_bytes: Bytes,
    pub requests_queue: RequestsQueue,
    pub outcomes: SearchOutcomes,
}

pub struct Done {
    pub block_ref: BlockRef,
    pub outcomes: SearchOutcomes,
}

#[derive(Debug)]
pub enum Error {
}

pub async fn run(Args { block_ref, block_bytes, requests_queue, outcomes, }: Args) -> Result<Done, Error> {

    unimplemented!()
}
