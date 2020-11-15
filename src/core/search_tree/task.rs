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
        search_tree::{
            SearchTreeIterItemsRx,
            SearchTreeIterBlockRefsRx,
        },
        BlockRef,
        SearchRangeBounds,
    },
};

pub mod bootstrap;
pub mod load_block;
pub mod search_block;
pub mod iter_cache;
pub mod iter_block;
pub mod demolish;

pub type LookupRequestsQueueType = BinaryHeap<LookupRequest>;
pub type LookupRequestsQueue = Unique<LookupRequestsQueueType>;
pub type SearchOutcomes = Unique<Vec<SearchOutcome>>;

pub struct SearchOutcome {
    pub request: LookupRequest,
    pub outcome: Outcome,
}

pub enum Outcome {
    Found { value_cell: kv::ValueCell, },
    NotFound,
    Jump { block_ref: BlockRef, },
}

pub struct LookupRequest {
    pub key: kv::Key,
    pub reply_tx: oneshot::Sender<Result<Option<kv::ValueCell>, SearchTreeLookupError>>,
}

pub type IterRequestsQueueType = Vec<IterRequest>;
pub type IterRequestsQueue = Unique<IterRequestsQueueType>;

pub struct IterRequest {
    pub block_ref: BlockRef,
    pub kind: IterRequestKind,
}

pub enum IterRequestKind {
    Items {
        range: SearchRangeBounds,
        reply_tx: oneshot::Sender<SearchTreeIterItemsRx>,
    },
    BlockRefs { reply_tx: oneshot::Sender<SearchTreeIterBlockRefsRx>, },
}

#[derive(Debug)]
pub enum SearchTreeLookupError {
}

pub enum TaskArgs {
    Bootstrap(bootstrap::Args),
    LoadBlock(load_block::Args),
    SearchBlock(search_block::Args),
    IterCache(iter_cache::Args),
    IterBlock(iter_block::Args),
    Demolish(demolish::Args),
}

pub enum TaskDone {
    Bootstrap(bootstrap::Done),
    LoadBlock(load_block::Done),
    SearchBlock(search_block::Done),
    IterCache(iter_cache::Done),
    IterBlock(iter_block::Done),
    Demolish(demolish::Done),
}

#[derive(Debug)]
pub enum Error {
    Bootstrap(bootstrap::Error),
    LoadBlock(load_block::Error),
    SearchBlock(search_block::Error),
    IterCache(iter_cache::Error),
    IterBlock(iter_block::Error),
    Demolish(demolish::Error),
}

pub async fn run_args(args: TaskArgs) -> Result<TaskDone, Error> {
    Ok(match args {
        TaskArgs::Bootstrap(args) =>
            TaskDone::Bootstrap(
                bootstrap::run(args).await
                    .map_err(Error::Bootstrap)?,
            ),
        TaskArgs::LoadBlock(args) =>
            TaskDone::LoadBlock(
                load_block::run(args).await
                    .map_err(Error::LoadBlock)?,
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
        TaskArgs::IterBlock(args) =>
            TaskDone::IterBlock(
                iter_block::run(args).await
                    .map_err(Error::IterBlock)?,
            ),
        TaskArgs::Demolish(args) =>
            TaskDone::Demolish(
                demolish::run(args).await
                    .map_err(Error::Demolish)?,
            ),
    })
}

impl PartialEq for LookupRequest {
    fn eq(&self, other: &LookupRequest) -> bool {
        self.key == other.key
    }
}

impl Eq for LookupRequest { }

impl PartialOrd for LookupRequest {
    fn partial_cmp(&self, other: &LookupRequest) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LookupRequest {
    fn cmp(&self, other: &LookupRequest) -> Ordering {
        other.key.key_bytes.cmp(&self.key.key_bytes)
    }
}
