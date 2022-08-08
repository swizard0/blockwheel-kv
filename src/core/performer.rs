use std::{
    mem,
    ops::{
        Deref,
        DerefMut,
    },
    sync::{
        Arc,
    },
    marker::{
        PhantomData,
    },
    collections::{
        hash_map,
        HashMap,
    },
};

use alloc_pool::{
    pool,
    Unique,
};

use crate::{
    kv,
    version,
    core::{
        context::{
            Context,
        },
        bin_merger::{
            BinMerger,
        },
        search_tree_walker,
        search_ranges_merge,
        OrdKey,
        MemCache,
        BlockRef,
        SearchRangeBounds,
    },
    Info,
};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug)]
pub struct Params {
    pub butcher_block_size: usize,
    pub tree_block_size: usize,
    pub remove_tasks_limit: usize,
    pub values_inline_size_limit: usize,
}

impl Default for Params {
    fn default() -> Params {
        Params {
            butcher_block_size: 128,
            tree_block_size: 32,
            remove_tasks_limit: 64,
            values_inline_size_limit: 128,
        }
    }
}

pub struct Performer<C> where C: Context {
    inner: Inner<C>,
}

pub enum Kont<C> where C: Context {
    Poll(KontPoll<C>),
    Inserted(KontInserted<C>),
    FlushButcher(KontFlushButcher<C>),
    LookupRangeMergerReady(KontLookupRangeMergerReady<C>),
}

pub struct KontPoll<C> where C: Context {
    pub next: KontPollNext<C>,
}

pub struct KontPollNext<C> where C: Context {
    inner: Inner<C>,
}

pub struct KontInserted<C> where C: Context {
    pub version: u64,
    pub insert_context: C::Insert,
    pub next: KontInsertedNext<C>,
}

pub struct KontInsertedNext<C> where C: Context {
    inner: Inner<C>,
}

pub struct KontFlushButcher<C> where C: Context {
    pub search_tree_id: u64,
    pub frozen_memcache: Arc<MemCache>,
    pub next: KontFlushButcherNext<C>,
}

pub struct KontFlushButcherNext<C> where C: Context {
    inner: Inner<C>,
}

pub struct KontLookupRangeMergerReady<C> where C: Context {
    pub ranges_merger: LookupRangesMerger,
    pub lookup_context: C::Lookup,
    pub next: KontLookupRangeMergerReadyNext<C>,
}

pub struct KontLookupRangeMergerReadyNext<C> where C: Context {
    inner: Inner<C>,
}

pub struct LookupRangesMerger {
    pub source: search_ranges_merge::RangesMergeCps<Unique<Vec<LookupRangeSource>>, LookupRangeSource>,
    pub token: LookupRangeToken,
}

#[derive(Default)]
pub struct LookupRangeToken {
    search_trees_ids: Vec<u64>,
}

pub struct LookupRangeSource {
    pub source: search_ranges_merge::Source,
}

struct Inner<C> where C: Context {
    params: Params,
    version_provider: version::Provider,
    kv_pool: pool::Pool<Vec<kv::KeyValuePair<kv::Value>>>,
    sources_pool: pool::Pool<Vec<LookupRangeSource>>,
    block_entry_steps_pool: pool::Pool<Vec<search_tree_walker::BlockEntryStep>>,
    butcher: MemCache,
    forest: SearchForest,
    info: Info,
    _marker: PhantomData<C>,
}

pub struct SearchForest {
    next_id: u64,
    search_trees: HashMap<u64, SearchTree>,
    search_trees_pile: BinMerger<SearchTreeRef>,
    search_trees_decay: HashMap<u64, SearchTreeConstructed>,
    accesses_count: usize,
}

impl<C> Performer<C> where C: Context {
    pub fn new(
        params: Params,
        version_provider: version::Provider,
        kv_pool: pool::Pool<Vec<kv::KeyValuePair<kv::Value>>>,
        sources_pool: pool::Pool<Vec<LookupRangeSource>>,
        block_entry_steps_pool: pool::Pool<Vec<search_tree_walker::BlockEntryStep>>,
        forest: SearchForest,
    )
        -> Self
    {
        Self {
            inner: Inner {
                params,
                butcher: MemCache::new(),
                version_provider,
                kv_pool,
                sources_pool,
                block_entry_steps_pool,
                forest,
                info: Info::default(),
                _marker: PhantomData,
            },
        }
    }

    pub fn step(self) -> Kont<C> {
        Kont::Poll(KontPoll { next: KontPollNext { inner: self.inner, }, })
    }
}

impl<C> Inner<C> where C: Context {
    fn insert_butcher(&mut self, key: kv::Key, cell: kv::Cell<kv::Value>) -> (u64, Option<kv::ValueCell<kv::Value>>) {
        let ord_key = OrdKey::new(key);
        let version = self.version_provider.obtain();
        let value_cell = kv::ValueCell { version, cell, };
        let maybe_prev = self.butcher.insert(ord_key.clone(), value_cell);
        (version, maybe_prev)
    }

    fn insert(&mut self, key: kv::Key, value: kv::Value) -> u64 {
        let (version, maybe_prev) = self.insert_butcher(key, kv::Cell::Value(value));
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
        version
    }

    fn maybe_flush(&mut self) -> FlushOutcome {
        let items_count = self.butcher.len();
        if items_count >= self.params.butcher_block_size {
            let frozen_memcache =
                Arc::new(mem::replace(&mut self.butcher, MemCache::new()));
            let search_tree_id = self.forest.add_bootstrap(frozen_memcache.clone());
            // we do not want to merge the tree until it is written to blockwheel
            // self.search_trees_pile
            //     .push(SearchTreeRef { search_tree_id, items_count, }, items_count);
            self.info.reset();
            FlushOutcome::TimeToFlush {
                search_tree_id,
                frozen_memcache,
            }
        } else {
            FlushOutcome::NotFlushed
        }
    }

    fn butcher_flushed(&mut self, search_tree_id: u64, root_block: BlockRef) {
        let items_count = match self.forest.remove(search_tree_id) {
            Some(SearchTree::Bootstrap(SearchTreeBootstrap { frozen_memcache, })) =>
                frozen_memcache.len(),
            _ =>
                unreachable!("expected SearchTreeBootstrap after butcher_flushed"),
        };
        self.forest.add_constructed(root_block, items_count);
    }

    fn make_lookup_ranges_merger(&mut self, search_range: SearchRangeBounds) -> LookupRangesMerger {
        let mut sources = self.sources_pool.lend(Vec::new);
        sources.clear();

        let mut search_trees_ids = Vec::new();

        let butcher_source = LookupRangeSource {
            source: search_ranges_merge::Source::Butcher(
                search_ranges_merge::SourceButcher::from_active_memcache(
                    search_range.clone(),
                    &self.butcher,
                    &self.kv_pool,
                ),
            ),
        };
        sources.push(butcher_source);

        let mut total_accesses_count = 0;
        for (search_tree_id, search_tree) in &mut self.forest.search_trees {
            let source = match search_tree {
                SearchTree::Bootstrap(SearchTreeBootstrap { frozen_memcache, }) =>
                    LookupRangeSource {
                        source: search_ranges_merge::Source::Butcher(
                            search_ranges_merge::SourceButcher::new(
                                search_range.clone(),
                                frozen_memcache.clone(),
                            ),
                        ),
                    },
                SearchTree::Constructed(SearchTreeConstructed { root_block, accesses_count, }) => {
                    *accesses_count += 1;
                    total_accesses_count += 1;
                    search_trees_ids.push(*search_tree_id);
                    LookupRangeSource {
                        source: search_ranges_merge::Source::SearchTree(
                            search_ranges_merge::SourceSearchTree::new(
                                search_range.clone(),
                                root_block.clone(),
                            ),
                        ),
                    }
                },
            };
            sources.push(source);
        }
        self.forest.accesses_count += total_accesses_count;
        sources.shrink_to_fit();

        let source = search_ranges_merge::RangesMergeCps::new(
            sources,
            self.kv_pool.clone(),
            self.block_entry_steps_pool.clone(),
        );

        LookupRangesMerger {
            source,
            token: LookupRangeToken {
                search_trees_ids,
            },
        }
    }

    fn commit_lookup_range(&mut self, lookup_range_token: LookupRangeToken) {
        for search_tree_id in lookup_range_token.search_trees_ids {
            match self.forest.search_trees.get_mut(&search_tree_id) {
                Some(SearchTree::Bootstrap(..)) =>
                    unreachable!(),
                Some(SearchTree::Constructed(search_tree)) => {
                    assert!(search_tree.accesses_count > 0);
                    search_tree.accesses_count -= 1;
                    assert!(self.forest.accesses_count > 0);
                    self.forest.accesses_count -= 1;
                },
                None => {
                    // search tree has been scheduled to decay
                    match self.forest.search_trees_decay.entry(search_tree_id) {
                        hash_map::Entry::Vacant(..) =>
                            unreachable!(),
                        hash_map::Entry::Occupied(mut oe) => {
                            let search_tree = oe.get_mut();
                            assert!(search_tree.accesses_count > 0);
                            search_tree.accesses_count -= 1;
                            assert!(self.forest.accesses_count > 0);
                            self.forest.accesses_count -= 1;

                            todo!();
                        },
                    }
                },
            }
        }
    }
}

enum FlushOutcome {
    NotFlushed,
    TimeToFlush {
        search_tree_id: u64,
        frozen_memcache: Arc<MemCache>,
    },
}

impl<C> KontPollNext<C> where C: Context {
    pub fn incoming_insert(mut self, key: kv::Key, value: kv::Value, insert_context: C::Insert) -> Kont<C> {
        let version = self.inner.insert(key, value);
        Kont::Inserted(KontInserted {
            version,
            insert_context,
            next: KontInsertedNext {
                inner: self.inner,
            },
        })
    }

    pub fn begin_lookup_range(mut self, search_range: SearchRangeBounds, lookup_context: C::Lookup) -> Kont<C> {
	let ranges_merger = self.inner.make_lookup_ranges_merger(search_range);
        Kont::LookupRangeMergerReady(KontLookupRangeMergerReady {
            ranges_merger,
            lookup_context,
            next: KontLookupRangeMergerReadyNext {
                inner: self.inner,
            },
        })
    }

    pub fn commit_lookup_range(mut self, lookup_range_token: LookupRangeToken) -> Kont<C> {
        self.inner.commit_lookup_range(lookup_range_token);

        todo!();
    }

    pub fn butcher_flushed(mut self, search_tree_id: u64, root_block: BlockRef) -> Kont<C> {
        self.inner.butcher_flushed(search_tree_id, root_block);
        Kont::Poll(KontPoll { next: KontPollNext { inner: self.inner, }, })
    }
}

impl<C> KontInsertedNext<C> where C: Context {
    pub fn got_it(mut self) -> Kont<C> {
	match self.inner.maybe_flush() {
            FlushOutcome::NotFlushed =>
                Kont::Poll(KontPoll { next: KontPollNext { inner: self.inner, }, }),
            FlushOutcome::TimeToFlush { search_tree_id, frozen_memcache, } =>
                Kont::FlushButcher(KontFlushButcher {
                    search_tree_id,
                    frozen_memcache,
                    next: KontFlushButcherNext {
                        inner: self.inner,
                    },
                }),
        }
    }
}

impl<C> KontFlushButcherNext<C> where C: Context {
    pub fn scheduled(self) -> Kont<C> {
        Kont::Poll(KontPoll { next: KontPollNext { inner: self.inner, }, })
    }
}

impl<C> KontLookupRangeMergerReadyNext<C> where C: Context {
    pub fn got_it(self) -> Kont<C> {
        Kont::Poll(KontPoll { next: KontPollNext { inner: self.inner, }, })
    }
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
    accesses_count: usize,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct SearchTreeRef {
    items_count: usize,
    search_tree_id: u64,
}

impl SearchForest {
    pub fn new() -> Self {
        Self {
            next_id: 0,
            search_trees: HashMap::new(),
            search_trees_pile: BinMerger::new(),
            search_trees_decay: HashMap::new(),
            accesses_count: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.search_trees.len()
    }

    pub fn add_constructed(&mut self, root_block: BlockRef, items_count: usize) -> u64 {
        let search_tree_id = self.next_id;
        self.next_id += 1;

        self.search_trees.insert(
            search_tree_id,
            SearchTree::Constructed(SearchTreeConstructed {
                root_block,
                accesses_count: 0,
            }),
        );
        self.search_trees_pile.push(
            SearchTreeRef {
                search_tree_id,
                items_count,
            },
            items_count,
        );
        search_tree_id
    }

    fn add_bootstrap(&mut self, frozen_memcache: Arc<MemCache>) -> u64 {
        let search_tree_id = self.next_id;
        self.next_id += 1;

        self.search_trees.insert(
            search_tree_id,
            SearchTree::Bootstrap(SearchTreeBootstrap {
                frozen_memcache,
            }),
        );
        search_tree_id
    }

    fn remove(&mut self, search_tree_id: u64) -> Option<SearchTree> {
        self.search_trees.remove(&search_tree_id)
    }
}

impl Deref for LookupRangeSource {
    type Target = search_ranges_merge::Source;

    fn deref(&self) -> &Self::Target {
        &self.source
    }
}

impl DerefMut for LookupRangeSource {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.source
    }
}
