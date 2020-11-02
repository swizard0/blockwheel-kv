use std::{
    collections::BinaryHeap,
};

use futures::{
    channel::{
        oneshot,
    },
};

use alloc_pool::Unique;

use crate::{
    kv::{
        self,
        ContainsKey,
    },
    core::{
        OrdKey,
        BlockRef,
        search_tree::{
            Found,
        },
    },
};

pub mod bootstrap;
pub mod load_block_lookup;
pub mod search_block;

pub type RequestsQueue = Unique<BinaryHeap<OrdKey<Lookup>>>;
pub type SearchOutcomes = Unique<Vec<SearchOutcome>>;

pub struct SearchOutcome {
    request: Lookup,
    outcome: Outcome,
}

enum Outcome {
    Found { kv: kv::KeyValue, },
    NotFound,
    Jump { block_ref: BlockRef, },
}

pub struct Lookup {
    pub key: kv::Key,
    pub reply_tx: oneshot::Sender<Result<Found, SearchTreeLookupError>>,
}

#[derive(Debug)]
pub enum SearchTreeLookupError {
}

impl ContainsKey for Lookup {
    fn key_data(&self) -> &[u8] {
        self.key.key_data()
    }
}

pub enum TaskArgs {
    Bootstrap(bootstrap::Args),
    LoadBlockLookup(load_block_lookup::Args),
    SearchBlock(search_block::Args),
}

pub enum TaskDone {
    Bootstrap(bootstrap::Done),
    LoadBlockLookup(load_block_lookup::Done),
    SearchBlock(search_block::Done),
}

#[derive(Debug)]
pub enum Error {
    Bootstrap(bootstrap::Error),
    LoadBlockLookup(load_block_lookup::Error),
    SearchBlock(search_block::Error),
}

pub async fn run_args(args: TaskArgs) -> Result<TaskDone, Error> {
    Ok(match args {
        TaskArgs::Bootstrap(args) =>
            TaskDone::Bootstrap(
                bootstrap::run(args).await
                    .map_err(Error::Bootstrap)?,
            ),
        TaskArgs::LoadBlockLookup(args) =>
            TaskDone::LoadBlockLookup(
                load_block_lookup::run(args).await
                    .map_err(Error::LoadBlockLookup)?,
            ),
        TaskArgs::SearchBlock(args) =>
            TaskDone::SearchBlock(
                search_block::run(args).await
                    .map_err(Error::SearchBlock)?,
            ),
    })
}
