use std::{
    mem,
    sync::{
        Arc,
    },
    ops::{
        Deref,
        DerefMut,
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
        SearchRangeBounds,
    },
};

#[cfg(test)]
mod tests;

#[derive(Clone)]
pub struct RangesMergeCps<V, S> where V: DerefMut<Target = Vec<S>> {
    inner: Inner<V, S>,
}

pub enum Kont<V, S> where V: DerefMut<Target = Vec<S>> {
    RequireBlockAsync(KontRequireBlockAsync<V, S>),
    AwaitBlocks(KontAwaitBlocks<V, S>),
    EmitDeprecated(KontEmitDeprecated<V, S>),
    EmitItem(KontEmitItem<V, S>),
    BlockFinished(KontBlockFinished<V, S>),
    Finished,
}

pub struct KontRequireBlockAsync<V, S> where V: DerefMut<Target = Vec<S>> {
    pub block_ref: BlockRef,
    pub async_token: AsyncToken<S>,
    pub next: KontRequireBlockAsyncNext<V, S>,
}

pub struct AsyncToken<S> {
    await_iter: S,
    walker_next: search_tree_walker::KontRequireBlockNext,
}

pub struct KontRequireBlockAsyncNext<V, S> where V: DerefMut<Target = Vec<S>> {
    inner: Inner<V, S>,
}

pub struct KontAwaitBlocks<V, S> where V: DerefMut<Target = Vec<S>> {
    pub next: KontAwaitBlocksNext<V, S>,
}

pub struct KontAwaitBlocksNext<V, S> where V: DerefMut<Target = Vec<S>> {
    inner: Inner<V, S>,
}

pub struct KontEmitDeprecated<V, S> where V: DerefMut<Target = Vec<S>> {
    pub item: kv::KeyValuePair<storage::OwnedValueBlockRef>,
    pub next: KontEmitDeprecatedNext<V, S>,
}

pub struct KontEmitDeprecatedNext<V, S> where V: DerefMut<Target = Vec<S>> {
    merger_next: MergerKontEmitDeprecatedNext<V, S>,
    inner: Inner<V, S>,
}

pub struct KontEmitItem<V, S> where V: DerefMut<Target = Vec<S>> {
    pub item: kv::KeyValuePair<storage::OwnedValueBlockRef>,
    pub next: KontEmitItemNext<V, S>,
}

pub struct KontEmitItemNext<V, S> where V: DerefMut<Target = Vec<S>> {
    merger_next: MergerKontEmitItemNext<V, S>,
    inner: Inner<V, S>,
}

pub struct KontBlockFinished<V, S> where V: DerefMut<Target = Vec<S>> {
    pub block_ref: BlockRef,
    pub next: KontBlockFinishedNext<V, S>,
}

pub struct KontBlockFinishedNext<V, S> where V: DerefMut<Target = Vec<S>> {
    await_iter: S,
    walker_next: search_tree_walker::KontBlockFinishedNext,
    inner: Inner<V, S>,
}

#[derive(Debug)]
pub enum Error {
    SearchTreeWalker(search_tree_walker::Error),
}

#[derive(Clone)]
struct Inner<V, S> where V: DerefMut<Target = Vec<S>> {
    state: State<V, S>,
    await_iters: Vec<S>,
    kv_pool: pool::Pool<Vec<kv::KeyValuePair<kv::Value>>>,
    block_entry_steps_pool: pool::Pool<Vec<search_tree_walker::BlockEntryStep>>,
}

pub enum Source {
    Butcher(SourceButcher),
    SearchTree(SourceSearchTree),
}

impl Deref for Source {
    type Target = Self;

    fn deref(&self) -> &Self::Target {
        self
    }
}

impl DerefMut for Source {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self
    }
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

#[derive(Clone)]
enum State<V, S> where V: DerefMut<Target = Vec<S>> {
    MergerStep { merger_kont: MergerKont<V, S>, },
    AwaitIters { merger_next: MergerKontAwaitScheduledNext<V, S>, },
    Emitted,
}

type MergerKont<V, S> = merger::Kont<V, S>;
type MergerKontAwaitScheduledNext<V, S> = merger::KontAwaitScheduledNext<V, S>;
type MergerKontEmitDeprecatedNext<V, S> = merger::KontEmitDeprecatedNext<V, S>;
type MergerKontEmitItemNext<V, S> = merger::KontEmitItemNext<V, S>;

impl<V, S> RangesMergeCps<V, S> where V: DerefMut<Target = Vec<S>>, S: DerefMut<Target = Source> {
    pub fn new(
        sources: V,
        kv_pool: pool::Pool<Vec<kv::KeyValuePair<kv::Value>>>,
        block_entry_steps_pool: pool::Pool<Vec<search_tree_walker::BlockEntryStep>>,
    )
        -> Self
    {
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

    pub fn step(mut self) -> Result<Kont<V, S>, Error> {
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
                    let mut await_iter = if let Some(await_iter) = self.inner.await_iters.pop() {
                        await_iter
                    } else {
                        self.inner.state = State::AwaitIters { merger_next, };
                        return Ok(Kont::AwaitBlocks(KontAwaitBlocks {
                            next: KontAwaitBlocksNext {
                                inner: self.inner,
                            },
                        }));
                    };
                    loop {
                        match &mut *await_iter {
                            Source::Butcher(SourceButcher { source_state, }) =>
                                match mem::replace(source_state, SourceButcherState::Done) {
                                    SourceButcherState::Init { search_range, memcache ,} => {
                                        let source_butcher_next = SourceButcher::from_active_memcache(
                                            search_range,
                                            &memcache,
                                            &self.inner.kv_pool,
                                        );
                                        *source_state = source_butcher_next.source_state;
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
                                                    merger_kont: merger_next.item_arrived(
                                                        await_iter,
                                                        kv::KeyValuePair {
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
                                            merger_kont: merger_next.no_more(),
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
                                                        await_iter,
                                                        walker_next: kont_require_block.next,
                                                    },
                                                    next: KontRequireBlockAsyncNext {
                                                        inner: self.inner,
                                                    },
                                                }));
                                            },
                                            search_tree_walker::Kont::BlockFinished(
                                                search_tree_walker::KontBlockFinished { block_ref, next, },
                                            ) => {
                                                self.inner.state = State::AwaitIters { merger_next, };
                                                return Ok(Kont::BlockFinished(KontBlockFinished {
                                                    block_ref,
                                                    next: KontBlockFinishedNext {
                                                        await_iter,
                                                        walker_next: next,
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
                                                    merger_kont: merger_next.item_arrived(
                                                        await_iter,
                                                        kv::KeyValuePair {
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
                                            merger_kont: merger_next.no_more(),
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

    fn block_arrived(mut self, mut async_token: AsyncToken<S>, block_bytes: Bytes) -> Result<Kont<V, S>, Error> {
        let walker_kont = async_token
            .walker_next
            .block_arrived(block_bytes)
            .map_err(Error::SearchTreeWalker)?;
        let source = &mut *async_token.await_iter;
        *source = Source::SearchTree(SourceSearchTree {
            source_state: SourceSearchTreeState::Step { walker_kont, },
        });
        self.inner.await_iters.push(async_token.await_iter);
        self.step()
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

    pub fn from_active_memcache(
        search_range: SearchRangeBounds,
        memcache: &MemCache,
        kv_pool: &pool::Pool<Vec<kv::KeyValuePair<kv::Value>>>,
    )
        -> Self
    {
        let mut kv_pairs = kv_pool.lend(Vec::new);
        kv_pairs.clear();
        kv_pairs.extend(memcache.range(search_range));
        kv_pairs.shrink_to_fit();
        kv_pairs.reverse();
        Self {
            source_state: SourceButcherState::Next {
                kv_pairs_rev: kv_pairs,
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

impl<V, S> KontRequireBlockAsyncNext<V, S> where V: DerefMut<Target = Vec<S>>, S: DerefMut<Target = Source> {
    pub fn scheduled(self) -> Result<Kont<V, S>, Error> {
        RangesMergeCps { inner: self.inner, }.step()
    }
}

impl<V, S> KontAwaitBlocksNext<V, S> where V: DerefMut<Target = Vec<S>>, S: DerefMut<Target = Source> {
    pub fn block_arrived(self, async_token: AsyncToken<S>, block_bytes: Bytes) -> Result<Kont<V, S>, Error> {
        RangesMergeCps { inner: self.inner, }.block_arrived(async_token, block_bytes)
    }
}

impl<V, S> KontBlockFinishedNext<V, S> where V: DerefMut<Target = Vec<S>>, S: DerefMut<Target = Source> {
    pub fn proceed(mut self) -> Result<Kont<V, S>, Error> {
        let walker_kont = self
            .walker_next
            .proceed()
            .map_err(Error::SearchTreeWalker)?;
        let source = &mut *self.await_iter;
        *source = Source::SearchTree(SourceSearchTree {
            source_state: SourceSearchTreeState::Step { walker_kont, },
        });
        self.inner.await_iters.push(self.await_iter);
        RangesMergeCps { inner: self.inner, }.step()
    }
}

impl<V, S> KontEmitDeprecatedNext<V, S> where V: DerefMut<Target = Vec<S>>, S: DerefMut<Target = Source> {
    pub fn proceed(mut self) -> Result<Kont<V, S>, Error> {
        let merger_kont = self.merger_next.proceed();
        self.inner.state = State::MergerStep { merger_kont, };
        RangesMergeCps { inner: self.inner, }.step()
    }
}

impl<V, S> KontEmitItemNext<V, S> where V: DerefMut<Target = Vec<S>>, S: DerefMut<Target = Source> {
    pub fn proceed(mut self) -> Result<Kont<V, S>, Error> {
        let merger_kont = self.merger_next.proceed();
        self.inner.state = State::MergerStep { merger_kont, };
        RangesMergeCps { inner: self.inner, }.step()
    }
}
