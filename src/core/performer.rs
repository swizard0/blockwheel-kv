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

use crate::{
    kv,
    version,
    core::{
        bin_merger::{
            BinMerger,
        },
        OrdKey,
        MemCache,
        BlockRef,
    },
    Info,
};

#[derive(Clone, Debug)]
pub struct Params {
    pub tree_block_size: usize,
    pub remove_tasks_limit: usize,
    pub values_inline_size_limit: usize,
}

impl Default for Params {
    fn default() -> Params {
        Params {
            tree_block_size: 32,
            remove_tasks_limit: 64,
            values_inline_size_limit: 128,
        }
    }
}

pub struct Performer {
    inner: Inner,
}

pub enum Kont {
    Poll(KontPoll),
    Inserted(KontInserted),
    FlushButcher(KontFlushButcher),
}

pub struct KontPoll {
    pub next: KontPollNext,
}

pub struct KontPollNext {
    inner: Inner,
}

pub struct KontInserted {
    pub next: KontInsertedNext,
}

pub struct KontInsertedNext {
    inner: Inner,
}

pub struct KontFlushButcher {
    pub search_tree_ref: Ref,
    pub frozen_memcache: Arc<MemCache>,
    pub next: KontFlushButcherNext,
}

pub struct KontFlushButcherNext {
    inner: Inner,
}

struct Inner {
    params: Params,
    version_provider: version::Provider,
    butcher: MemCache,
    search_trees: Set<SearchTree>,
    search_trees_pile: BinMerger<SearchTreeRef>,
    info: Info,
}

impl Performer {
    pub fn new(
        params: Params,
        version_provider: version::Provider,
    )
        -> Self
    {
        Self {
            inner: Inner::new(
                params,
                version_provider,
            ),
        }
    }

    pub fn step(self) -> Kont {
        Kont::Poll(KontPoll { next: KontPollNext { inner: self.inner, }, })
    }
}

impl Inner {
    fn new(
        params: Params,
        version_provider: version::Provider,
    )
        -> Self
    {
        Self {
            params,
            version_provider,
            butcher: MemCache::new(),
            search_trees: Set::new(),
            search_trees_pile: BinMerger::new(),
            info: Info::default(),
        }
    }

    fn insert_butcher(&mut self, key: kv::Key, cell: kv::Cell<kv::Value>) -> Option<kv::ValueCell<kv::Value>> {
        let ord_key = OrdKey::new(key);
        let version = self.version_provider.obtain();
        let value_cell = kv::ValueCell { version, cell, };
        self.butcher.insert(ord_key.clone(), value_cell)
    }

    fn insert(&mut self, key: kv::Key, value: kv::Value) {
        let maybe_prev = self.insert_butcher(key, kv::Cell::Value(value));
        match maybe_prev {
            None =>
                self.info.alive_cells_count += 1,
            Some(kv::ValueCell { cell: kv::Cell::Value(..), .. }) =>
                (),
            Some(kv::ValueCell { cell: kv::Cell::Tombstone, .. }) => {
                assert!(self.info.tombstones_count > 0);
                self.info.tombstones_count -= 1;
                self.info.alive_cells_count += 1;
            },
        }
    }

    fn maybe_flush(&mut self) -> FlushOutcome {
        let items_count = self.butcher.len();
        if items_count >= self.params.tree_block_size {
            let frozen_memcache =
                Arc::new(mem::replace(&mut self.butcher, MemCache::new()));
            let search_tree_ref =
                self.search_trees.insert(SearchTree::Bootstrap(SearchTreeBootstrap {
                    frozen_memcache: frozen_memcache.clone(),
                }));
            // we do not want to merge the tree until it is written to blockwheel
            // self.search_trees_pile
            //     .push(SearchTreeRef { search_tree_ref, items_count, }, items_count);
            self.info.reset();
            FlushOutcome::TimeToFlush {
                search_tree_ref,
                frozen_memcache,
            }
        } else {
            FlushOutcome::NotFlushed
        }
    }

    fn butcher_flushed(&mut self, search_tree_ref: Ref, root_block: BlockRef) {
        let search_tree = self.search_trees.get_mut(search_tree_ref).unwrap();
        let prev_search_tree = mem::replace(
            search_tree,
            SearchTree::Constructed(
                SearchTreeConstructed {
                    root_block,
                },
            ),
        );
        assert!(matches!(prev_search_tree, SearchTree::Bootstrap(..)));
    }
}

enum FlushOutcome {
    NotFlushed,
    TimeToFlush {
        search_tree_ref: Ref,
        frozen_memcache: Arc<MemCache>,
    },
}

impl KontPollNext {
    pub fn incoming_insert(mut self, key: kv::Key, value: kv::Value) -> Kont {
        self.inner.insert(key, value);
        Kont::Inserted(KontInserted {
            next: KontInsertedNext {
                inner: self.inner,
            },
        })
    }

    pub fn butcher_flushed(mut self, search_tree_ref: Ref, root_block: BlockRef) -> Kont {
        self.inner.butcher_flushed(search_tree_ref, root_block);
        Kont::Poll(KontPoll { next: KontPollNext { inner: self.inner, }, })
    }
}

impl KontInsertedNext {
    pub fn step(mut self) -> Kont {
	match self.inner.maybe_flush() {
            FlushOutcome::NotFlushed =>
                Kont::Poll(KontPoll { next: KontPollNext { inner: self.inner, }, }),
            FlushOutcome::TimeToFlush { search_tree_ref, frozen_memcache, } =>
                Kont::FlushButcher(KontFlushButcher {
                    search_tree_ref,
                    frozen_memcache,
                    next: KontFlushButcherNext {
                        inner: self.inner,
                    },
                }),
        }
    }
}

enum KontInsertedNextAction {
    Poll,
    FlushButcher {
        search_tree_ref: Ref,
        frozen_memcache: Arc<MemCache>,
    },
}

enum SearchTree {
    Bootstrap(SearchTreeBootstrap),
    Constructed(SearchTreeConstructed),
}

struct SearchTreeBootstrap {
    frozen_memcache: Arc<MemCache>,
}

struct SearchTreeConstructed {
    root_block: BlockRef,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct SearchTreeRef {
    items_count: usize,
    search_tree_ref: Ref,
}
