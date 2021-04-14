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
    job,
    storage,
    core::{
        search_tree::{
            SearchTreeIterItemsTx,
        },
        BlockRef,
        SearchRangeBounds,
    },
};

pub mod bootstrap;
pub mod load_block;
pub mod search_cache;
pub mod search_block;
pub mod iter_cache;
pub mod iter_block;
pub mod iter_driver;
pub mod demolish;

pub type LookupRequestsQueueType = BinaryHeap<LookupRequest>;
pub type LookupRequestsQueue = Unique<LookupRequestsQueueType>;
pub type SearchOutcomes = Unique<Vec<SearchOutcome>>;

pub struct SearchOutcome {
    pub request: LookupRequest,
    pub outcome: Outcome,
}

pub enum Outcome {
    Found { value_cell: kv::ValueCell<storage::OwnedValueBlockRef>, },
    NotFound,
    Jump { block_ref: BlockRef, },
}

pub struct LookupRequest {
    pub key: kv::Key,
    pub reply_tx: oneshot::Sender<Result<Option<kv::ValueCell<storage::OwnedValueBlockRef>>, SearchTreeLookupError>>,
}

pub type IterRequestsQueueType = Vec<IterRequest>;
pub type IterRequestsQueue = Unique<IterRequestsQueueType>;

pub struct IterRequestData {
    pub range: SearchRangeBounds,
    pub iter_items_tx: SearchTreeIterItemsTx,
    pub repay_iter_items_tx: oneshot::Sender<SearchTreeIterItemsTx>,
}

pub struct IterRecRequest {
    pub maybe_block_ref: Option<BlockRef>,
    pub data: IterRequestData,
}

pub struct IterRequest {
    pub block_ref: BlockRef,
    pub data: IterRequestData,
}

pub enum BlockEntry {
    OnlyJump(BlockRef),
    OnlyEntry {
        key: kv::Key,
        value_cell: kv::ValueCell<storage::OwnedValueBlockRef>,
    },
    JumpAndEntry {
        jump: BlockRef,
        key: kv::Key,
        value_cell: kv::ValueCell<storage::OwnedValueBlockRef>,
    },
}

#[derive(Debug)]
pub enum SearchTreeLookupError {
}

pub enum TaskArgs<J> where J: edeltraud::Job {
    Bootstrap(bootstrap::Args<J>),
    LoadBlock(load_block::Args),
    SearchCache(search_cache::Args<J>),
    SearchBlock(search_block::Args<J>),
    IterCache(iter_cache::Args<J>),
    IterBlock(iter_block::Args<J>),
    IterDriver(iter_driver::Args),
    Demolish(demolish::Args),
}

pub enum TaskDone {
    Bootstrap(bootstrap::Done),
    LoadBlock(load_block::Done),
    SearchCache(search_cache::Done),
    SearchBlock(search_block::Done),
    IterCache(iter_cache::Done),
    IterBlock(iter_block::Done),
    IterDriver(iter_driver::Done),
    Demolish(demolish::Done),
}

#[derive(Debug)]
pub enum Error {
    Bootstrap(bootstrap::Error),
    LoadBlock(load_block::Error),
    SearchCache(search_cache::Error),
    SearchBlock(search_block::Error),
    IterCache(iter_cache::Error),
    IterBlock(iter_block::Error),
    IterDriver(iter_driver::Error),
    Demolish(demolish::Error),
}

pub async fn run_args<J>(args: TaskArgs<J>) -> Result<TaskDone, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
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
        TaskArgs::SearchCache(args) =>
            TaskDone::SearchCache(
                search_cache::run(args).await
                    .map_err(Error::SearchCache)?,
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
        TaskArgs::IterDriver(args) =>
            TaskDone::IterDriver(
                iter_driver::run(args).await
                    .map_err(Error::IterDriver)?,
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
