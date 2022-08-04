use std::{
    mem,
    sync::{
        Arc,
    },
};

use o1::{
    set::{
        Set,
        Ref,
    },
};

use alloc_pool::{
    pool,
    Unique,
};

use crate::{
    kv,
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
}

#[derive(Debug)]
pub enum Error {
}

struct Inner {
    state: State,
    await_iters: Set<Source>,
    kv_pool: pool::Pool<Vec<kv::KeyValuePair<kv::Value>>>,
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
    Init(BlockRef),
}

enum State {
    MergerStep { merger_kont: MergerKont, },
}

type MergerKont = merger::Kont<Unique<Vec<Source>>, Source>;

impl RangesMergeCps {
    pub fn new<I>(
        sources_iter: I,
        sources_pool: pool::Pool<Vec<Source>>,
        kv_pool: pool::Pool<Vec<kv::KeyValuePair<kv::Value>>>,
    )
        -> Self
    where I: IntoIterator<Item = Source>
    {
        let mut sources = sources_pool.lend(Vec::new);
        sources.clear();
        sources.extend(sources_iter.into_iter());
        sources.shrink_to_fit();
        let merger = merger::ItersMergerCps::new(sources);

        Self {
            inner: Inner {
                state: State::MergerStep {
                    merger_kont: merger.step(),
                },
                await_iters: Set::new(),
                kv_pool,
            },
        }
    }

    pub fn step(mut self) -> Result<Kont, Error> {
        loop {
            match self.inner.state {

                State::MergerStep { merger_kont, } =>
                    match merger_kont {
                        merger::Kont::ScheduleIterAwait(merger::KontScheduleIterAwait { await_iter, next, }) => {
                            self.inner.await_iters.insert(await_iter);
                            self.inner.state = State::MergerStep {
                                merger_kont: next.proceed(),
                            };
                        },
                        merger::Kont::AwaitScheduled(merger::KontAwaitScheduled { next, }) => {
                            let await_iter_ref = self.inner.await_iters.refs().next().unwrap();
                            let mut await_iter = self.inner.await_iters.remove(await_iter_ref).unwrap();
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
                                                            merger_kont: next.proceed_with_item(
                                                                await_iter,
                                                                KeyValueRef::Item {
                                                                    key: kv_pair.key,
                                                                    value_cell: kv_pair.value_cell,
                                                                },
                                                            ),
                                                        };
                                                        break;
                                                    },
                                                },
                                            SourceButcherState::Done => {
                                                self.inner.state = State::MergerStep {
                                                    merger_kont: next.proceed_with_item(
                                                        await_iter,
                                                        KeyValueRef::NoMore,
                                                    ),
                                                };
                                                break;
                                            },
                                        },
                                    Source::SearchTree(source_search_tree) => {

                                        todo!();
                                    },
                                }
                            }
                        },
                        merger::Kont::Deprecated(merger::KontDeprecated { item, next, }) => {

                            todo!();
                        },
                        merger::Kont::Item(merger::KontItem { item, next, }) => {

                            todo!();
                        },
                        merger::Kont::Done => {

                            todo!();
                        },
                    },

            }


            todo!();
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
    pub fn new(root_block: BlockRef) -> Self {
        Self {
            source_state: SourceSearchTreeState::Init(root_block),
        }
    }
}
