use std::{
    mem,
    sync::{
        Arc,
    },
};

use alloc_pool::{
    pool,
    Unique,
    bytes::{
        Bytes,
    },
};

use crate::{
    kv,
    storage,
    core::{
        merger,
        search_tree_walker,
        MemCache,
        BlockRef,
        KeyValueRef,
        SearchRangeBounds,
    },
};

pub struct RangesMergeCps {
    inner: Inner,
}

pub enum Kont {
    RequireBlockAsync(KontRequireBlockAsync),
    EmitDeprecated(KontEmitDeprecated),
    EmitItem(KontEmitItem),
    Finished,
}

pub struct KontRequireBlockAsync {
    pub block_ref: BlockRef,
    pub async_token: AsyncToken,
    pub next: KontRequireBlockAsyncNext,
}

pub struct AsyncToken {
    walker_next: search_tree_walker::KontRequireBlockNext,
}

pub struct KontRequireBlockAsyncNext {
    inner: Inner,
}

pub struct KontEmitDeprecated {
    pub item: kv::KeyValuePair<storage::OwnedValueBlockRef>,
    pub next: KontEmitDeprecatedNext,
}

pub struct KontEmitDeprecatedNext {
    merger_next: MergerKontEmitDeprecatedNext,
    inner: Inner,
}

pub struct KontEmitItem {
    pub item: kv::KeyValuePair<storage::OwnedValueBlockRef>,
    pub next: KontEmitItemNext,
}

pub struct KontEmitItemNext {
    merger_next: MergerKontEmitItemNext,
    inner: Inner,
}

#[derive(Debug)]
pub enum Error {
    SearchTreeWalker(search_tree_walker::Error),
}

struct Inner {
    state: State,
    await_iters: Vec<Source>,
    kv_pool: pool::Pool<Vec<kv::KeyValuePair<kv::Value>>>,
    block_entry_steps_pool: pool::Pool<Vec<search_tree_walker::BlockEntryStep>>,
}

pub enum Source {
    Butcher(SourceButcher),
    SearchTree(SourceSearchTree),
}

pub struct SourceButcher {
    source_state: SourceButcherState,
}

enum SourceButcherState {
    Init { search_range: SearchRangeBounds, memcache: Arc<MemCache>, },
    Next { kv_pairs_rev: Unique<Vec<kv::KeyValuePair<kv::Value>>>, },
    Done,
}

pub struct SourceSearchTree {
    source_state: SourceSearchTreeState,
}

enum SourceSearchTreeState {
    Init { search_range: SearchRangeBounds, root_block: BlockRef, },
    Step { walker_kont: search_tree_walker::Kont, },
    Done,
}

enum State {
    MergerStep { merger_kont: MergerKont, },
    AwaitIters { merger_next: MergerKontAwaitScheduledNext, },
    Emitted,
}

type MergerKont = merger::Kont<Unique<Vec<Source>>, Source>;
type MergerKontAwaitScheduledNext = merger::KontAwaitScheduledNext<Unique<Vec<Source>>, Source>;
type MergerKontEmitDeprecatedNext = merger::KontEmitDeprecatedNext<Unique<Vec<Source>>, Source>;
type MergerKontEmitItemNext = merger::KontEmitItemNext<Unique<Vec<Source>>, Source>;

impl RangesMergeCps {
    pub fn new<I>(
        sources_iter: I,
        sources_pool: pool::Pool<Vec<Source>>,
        kv_pool: pool::Pool<Vec<kv::KeyValuePair<kv::Value>>>,
        block_entry_steps_pool: pool::Pool<Vec<search_tree_walker::BlockEntryStep>>,
    )
        -> Self
    where I: IntoIterator<Item = Source>
    {
        let mut sources = sources_pool.lend(Vec::new);
        sources.clear();
        sources.extend(sources_iter.into_iter());
        sources.shrink_to_fit();
        let sources_count = sources.len();
        let merger = merger::ItersMergerCps::new(sources);

        Self {
            inner: Inner {
                state: State::MergerStep {
                    merger_kont: merger.step(),
                },
                await_iters: Vec::with_capacity(sources_count),
                kv_pool,
                block_entry_steps_pool,
            },
        }
    }

    pub fn step(mut self) -> Result<Kont, Error> {
        loop {
            match self.inner.state {

                State::MergerStep { merger_kont, } =>
                    match merger_kont {
                        merger::Kont::ScheduleIterAwait(merger::KontScheduleIterAwait { await_iter, next, }) => {
                            self.inner.await_iters.push(await_iter);
                            self.inner.state = State::MergerStep {
                                merger_kont: next.proceed(),
                            };
                        },
                        merger::Kont::AwaitScheduled(merger::KontAwaitScheduled { next, }) => {
                            self.inner.state = State::AwaitIters { merger_next: next, };
                        },
                        merger::Kont::EmitDeprecated(merger::KontEmitDeprecated { item, next, }) => {
                            self.inner.state = State::Emitted;
                            return Ok(Kont::EmitDeprecated(KontEmitDeprecated {
                                item,
                                next: KontEmitDeprecatedNext {
                                    merger_next: next,
                                    inner: self.inner,
                                },
                            }));
                        },
                        merger::Kont::EmitItem(merger::KontEmitItem { item, next, }) => {
                            self.inner.state = State::Emitted;
                            return Ok(Kont::EmitItem(KontEmitItem {
                                item,
                                next: KontEmitItemNext {
                                    merger_next: next,
                                    inner: self.inner,
                                },
                            }));
                        },
                        merger::Kont::Finished => {
                            assert!(self.inner.await_iters.is_empty());
                            return Ok(Kont::Finished);
                        },
                    },

                State::AwaitIters { merger_next, } => {
                    let mut await_iter = self.inner.await_iters.pop().unwrap();
                    loop {
                        match &mut await_iter {
                            Source::Butcher(SourceButcher { source_state, }) =>
                                match mem::replace(source_state, SourceButcherState::Done) {
                                    SourceButcherState::Init { search_range, memcache ,} => {
                                        let mut kv_pairs = self.inner.kv_pool.lend(Vec::new);
                                        kv_pairs.clear();
                                        kv_pairs.extend(memcache.range(search_range));
                                        kv_pairs.shrink_to_fit();
                                        kv_pairs.reverse();
                                        *source_state = SourceButcherState::Next {
                                            kv_pairs_rev: kv_pairs,
                                        };
                                    },
                                    SourceButcherState::Next { mut kv_pairs_rev, } =>
                                        match kv_pairs_rev.pop() {
                                            None =>
                                                (),
                                            Some(kv_pair) => {
                                                *source_state = SourceButcherState::Next {
                                                    kv_pairs_rev,
                                                };
                                                self.inner.state = State::MergerStep {
                                                    merger_kont: merger_next.proceed_with_item(
                                                        await_iter,
                                                        KeyValueRef::Item {
                                                            key: kv_pair.key,
                                                            value_cell: kv_pair.value_cell.into(),
                                                        },
                                                    ),
                                                };
                                                break;
                                            },
                                        },
                                    SourceButcherState::Done => {
                                        self.inner.state = State::MergerStep {
                                            merger_kont: merger_next.proceed_with_item(
                                                await_iter,
                                                KeyValueRef::NoMore,
                                            ),
                                        };
                                        break;
                                    },
                                },
                            Source::SearchTree(SourceSearchTree { source_state, }) =>
                                match mem::replace(source_state, SourceSearchTreeState::Done) {
                                    SourceSearchTreeState::Init { search_range, root_block, } => {
                                        let walker = search_tree_walker::WalkerCps::new(
                                            root_block,
                                            search_range,
                                            self.inner.block_entry_steps_pool.clone(),
                                        );
                                        *source_state = SourceSearchTreeState::Step {
                                            walker_kont: walker.step()
                                                .map_err(Error::SearchTreeWalker)?,
                                        };
                                    },
                                    SourceSearchTreeState::Step { walker_kont, } =>
                                        match walker_kont {
                                            search_tree_walker::Kont::RequireBlock(kont_require_block) => {
                                                self.inner.state = State::AwaitIters { merger_next, };
                                                return Ok(Kont::RequireBlockAsync(KontRequireBlockAsync {
                                                    block_ref: kont_require_block.block_ref,
                                                    async_token: AsyncToken {
                                                        walker_next: kont_require_block.next,
                                                    },
                                                    next: KontRequireBlockAsyncNext {
                                                        inner: self.inner,
                                                    },
                                                }));
                                            },
                                            search_tree_walker::Kont::ItemFound(kont_item_found) => {
                                                *source_state = SourceSearchTreeState::Step {
                                                    walker_kont: kont_item_found.next
                                                        .item_received()
                                                        .map_err(Error::SearchTreeWalker)?,
                                                };
                                                self.inner.state = State::MergerStep {
                                                    merger_kont: merger_next.proceed_with_item(
                                                        await_iter,
                                                        KeyValueRef::Item {
                                                            key: kont_item_found.item.key,
                                                            value_cell: kont_item_found.item.value_cell.into(),
                                                        },
                                                    ),
                                                };
                                                break;
                                            },
                                            search_tree_walker::Kont::Finished =>
                                                (),
                                        },
                                    SourceSearchTreeState::Done => {
                                        self.inner.state = State::MergerStep {
                                            merger_kont: merger_next.proceed_with_item(
                                                await_iter,
                                                KeyValueRef::NoMore,
                                            ),
                                        };
                                        break;
                                    },
                                },
                        }
                    }
                },

                State::Emitted =>
                    unreachable!(),

            }
        }
    }
}

impl SourceButcher {
    pub fn new(search_range: SearchRangeBounds, memcache: Arc<MemCache>) -> Self {
        Self {
            source_state: SourceButcherState::Init {
                search_range,
                memcache,
            },
        }
    }
}

impl SourceSearchTree {
    pub fn new(search_range: SearchRangeBounds, root_block: BlockRef) -> Self {
        Self {
            source_state: SourceSearchTreeState::Init {
                search_range,
                root_block,
            },
        }
    }
}

impl KontRequireBlockAsyncNext {
    pub fn block_arrived(mut self, async_token: AsyncToken, block_bytes: Bytes) -> Result<Kont, Error> {
        let walker_kont = async_token
            .walker_next
            .block_arrived(block_bytes)
            .map_err(Error::SearchTreeWalker)?;
        self.inner.await_iters.push(
            Source::SearchTree(SourceSearchTree {
                source_state: SourceSearchTreeState::Step { walker_kont, },
            }),
        );
        RangesMergeCps { inner: self.inner, }.step()
    }
}

impl KontEmitDeprecatedNext {
    pub fn proceed(mut self) -> Result<Kont, Error> {
        let merger_kont = self.merger_next.proceed();
        self.inner.state = State::MergerStep { merger_kont, };
        RangesMergeCps { inner: self.inner, }.step()
    }
}

impl KontEmitItemNext {
    pub fn proceed(mut self) -> Result<Kont, Error> {
        let merger_kont = self.merger_next.proceed();
        self.inner.state = State::MergerStep { merger_kont, };
        RangesMergeCps { inner: self.inner, }.step()
    }
}
