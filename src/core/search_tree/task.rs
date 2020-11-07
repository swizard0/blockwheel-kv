use std::{
    cmp::Ordering,
    collections::BinaryHeap,
};

use futures::{
    channel::{
        oneshot,
    },
};

use alloc_pool::Unique;

use crate::{
    kv,
    core::{
        BlockRef,
    },
};

pub mod bootstrap;
pub mod load_block_lookup;
pub mod search_block;
pub mod iter_cache;

pub type RequestsQueueType = BinaryHeap<Lookup>;
pub type RequestsQueue = Unique<RequestsQueueType>;
pub type SearchOutcomes = Unique<Vec<SearchOutcome>>;

pub struct SearchOutcome {
    pub request: Lookup,
    pub outcome: Outcome,
}

pub enum Outcome {
    Found { value_cell: kv::ValueCell, },
    NotFound,
    Jump { block_ref: BlockRef, },
}

pub struct Lookup {
    pub key: kv::Key,
    pub reply_tx: oneshot::Sender<Result<Option<kv::ValueCell>, SearchTreeLookupError>>,
}

pub type ItersQueue = Unique<Vec<IterCursor>>;

pub struct IterCursor {
    dfs_stack: Vec<BlockIter>,
}

struct BlockIter {
    block_ref: BlockRef,
    item_index: usize,
}

#[derive(Debug)]
pub enum SearchTreeLookupError {
}

pub enum TaskArgs {
    Bootstrap(bootstrap::Args),
    LoadBlockLookup(load_block_lookup::Args),
    SearchBlock(search_block::Args),
    IterCache(iter_cache::Args),
}

pub enum TaskDone {
    Bootstrap(bootstrap::Done),
    LoadBlockLookup(load_block_lookup::Done),
    SearchBlock(search_block::Done),
    IterCache(iter_cache::Done),
}

#[derive(Debug)]
pub enum Error {
    Bootstrap(bootstrap::Error),
    LoadBlockLookup(load_block_lookup::Error),
    SearchBlock(search_block::Error),
    IterCache(iter_cache::Error),
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
        TaskArgs::IterCache(args) =>
            TaskDone::IterCache(
                iter_cache::run(args).await
                    .map_err(Error::IterCache)?,
            ),
    })
}

impl PartialEq for Lookup {
    fn eq(&self, other: &Lookup) -> bool {
        self.key == other.key
    }
}

impl Eq for Lookup { }

impl PartialOrd for Lookup {
    fn partial_cmp(&self, other: &Lookup) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Lookup {
    fn cmp(&self, other: &Lookup) -> Ordering {
        other.key.key_bytes.cmp(&self.key.key_bytes)
    }
}
