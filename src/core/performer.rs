use std::{
    mem,
    ops::{
        Deref,
        DerefMut,
    },
    sync::{
        Arc,
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
    Removed(KontRemoved<C>),
    Flushed(KontFlushed<C>),
    FlushButcher(KontFlushButcher<C>),
    LookupRangeMergerReady(KontLookupRangeMergerReady<C>),
    MergeSearchTrees(KontMergeSearchTrees<C>),
    DemolishSearchTree(KontDemolishSearchTree<C>),
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

pub struct KontRemoved<C> where C: Context {
    pub version: u64,
    pub remove_context: C::Remove,
    pub next: KontRemovedNext<C>,
}

pub struct KontRemovedNext<C> where C: Context {
    inner: Inner<C>,
}

pub struct KontFlushed<C> where C: Context {
    pub flush_context: C::Flush,
    pub next: KontFlushedNext<C>,
}

pub struct KontFlushedNext<C> where C: Context {
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

pub struct KontMergeSearchTrees<C> where C: Context {
    pub ranges_merger: SearchTreesMerger,
    pub next: KontMergeSearchTreesNext<C>,
}

pub struct KontMergeSearchTreesNext<C> where C: Context {
    inner: Inner<C>,
}

pub struct KontDemolishSearchTree<C> where C: Context {
    pub order: DemolishOrder,
    pub next: KontDemolishSearchTreeNext<C>,
}

pub struct KontDemolishSearchTreeNext<C> where C: Context {
    inner: Inner<C>,
}

pub struct LookupRangesMerger {
    pub source: search_ranges_merge::RangesMergeCps<Unique<Vec<LookupRangeSource>>, LookupRangeSource>,
    pub token: AccessToken,
}

pub struct SearchTreesMerger {
    pub source_count_items: search_ranges_merge::RangesMergeCps<Unique<Vec<LookupRangeSource>>, LookupRangeSource>,
    pub source_build: search_ranges_merge::RangesMergeCps<Unique<Vec<LookupRangeSource>>, LookupRangeSource>,
    pub token: AccessToken,
}

#[derive(Default)]
pub struct AccessToken {
    search_trees_ids: Vec<u64>,
}

pub struct LookupRangeSource {
    pub source: search_ranges_merge::Source,
}

pub struct DemolishOrder {
    pub walker: search_tree_walker::WalkerCps,
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
    pending_events: Vec<PendingEvent<C>>,
    pending_flushes: Vec<C::Flush>,
}

pub struct SearchForest {
    next_id: u64,
    search_trees: HashMap<u64, SearchTree>,
    search_trees_pile: BinMerger<SearchTreeRef>,
    search_trees_decay: HashMap<u64, SearchTreeConstructed>,
    accesses_count: usize,
    butcher_flushes_count: usize,
}

enum PendingEvent<C> where C: Context {
    FlushButcher(PendingEventFlushButcher),
    Inserted {
        version: u64,
        insert_context: C::Insert,
    },
    Removed {
        version: u64,
        remove_context: C::Remove,
    },
    LookupRangeMergerReady {
        ranges_merger: LookupRangesMerger,
        lookup_context: C::Lookup,
    },
    Flushed {
        flush_context: C::Flush,
    },
    MergeSearchTrees(PendingEventMergeSearchTrees),
    Demolish {
        order: DemolishOrder,
    },
}

struct PendingEventFlushButcher {
    search_tree_id: u64,
    frozen_memcache: Arc<MemCache>,
}

struct PendingEventMergeSearchTrees {
    ranges_merger: SearchTreesMerger,
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
                pending_events: Vec::new(),
                pending_flushes: Vec::new(),
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

    fn remove(&mut self, key: kv::Key) -> u64 {
        let (version, maybe_prev) = self.insert_butcher(key, kv::Cell::Tombstone);
        match maybe_prev {
            None =>
                self.info.tombstones_count += 1,
            Some(kv::ValueCell { cell: kv::Cell::Tombstone, .. }) =>
                (),
            Some(kv::ValueCell { cell: kv::Cell::Value(..), .. }) => {
                assert!(self.info.alive_cells_count > 0);
                self.info.alive_cells_count -= 1;
                self.info.tombstones_count += 1;
            },
        }
        version
    }

    fn maybe_flush(&mut self, force: bool) -> Option<PendingEventFlushButcher> {
        let items_count = self.butcher.len();
        if (!force && items_count >= self.params.butcher_block_size) || (force && items_count > 0) {
            let frozen_memcache =
                Arc::new(mem::replace(&mut self.butcher, MemCache::new()));
            let search_tree_id = self.forest.add_bootstrap(frozen_memcache.clone());
            self.forest.butcher_flushes_count += 1;
            // we do not want to merge the tree until it is written to blockwheel
            // self.forest.search_trees_pile
            //     .push(SearchTreeRef { search_tree_id, items_count, }, items_count);
            self.info.reset();
            Some(PendingEventFlushButcher {
                search_tree_id,
                frozen_memcache,
            })
        } else {
            None
        }
    }

    fn butcher_flushed(&mut self, search_tree_id: u64, root_block: BlockRef) {
        match self.forest.search_trees.entry(search_tree_id) {
            hash_map::Entry::Vacant(..) =>
                unreachable!(),
            hash_map::Entry::Occupied(mut oe) =>
                match oe.get_mut() {
                    SearchTree::Bootstrap(SearchTreeBootstrap { frozen_memcache, }) => {
                        let items_count = frozen_memcache.len();
                        *oe.get_mut() = SearchTree::Constructed(SearchTreeConstructed {
                            root_block,
                            accesses_count: 0,
                        });
                        self.forest.search_trees_pile
                            .push(SearchTreeRef { search_tree_id, items_count, }, items_count);
                    },
                    SearchTree::Constructed(..) =>
                        unreachable!("expected SearchTreeBootstrap after butcher_flushed"),
                },
        }
        assert!(self.forest.butcher_flushes_count > 0);
        self.forest.butcher_flushes_count -= 1;
    }

    fn make_lookup_ranges_merger(&mut self, search_range: SearchRangeBounds) -> LookupRangesMerger {
        let mut sources = self.sources_pool.lend(Vec::new);
        sources.clear();

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

        let mut search_trees_ids = Vec::new();
        for (&search_tree_id, search_tree) in &mut self.forest.search_trees {
            sources.push(search_tree.build_source(&search_range));
            register_access_source(search_tree_id, search_tree, &mut search_trees_ids, &mut self.forest.accesses_count);
        }
        sources.shrink_to_fit();

        let source = search_ranges_merge::RangesMergeCps::new(
            sources,
            self.kv_pool.clone(),
            self.block_entry_steps_pool.clone(),
        );

        LookupRangesMerger {
            source,
            token: AccessToken {
                search_trees_ids,
            },
        }
    }

    fn commit_lookup_range(&mut self, access_token: AccessToken) {
        for search_tree_id in access_token.search_trees_ids {
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

                            if search_tree.accesses_count == 0 {
                                let search_tree = oe.remove();
                                self.demand_search_tree_removal(search_tree);
                            }
                        },
                    }
                },
            }
        }
    }

    fn maybe_merge_search_trees(&mut self) -> Option<PendingEventMergeSearchTrees> {
        let (SearchTreeRef { search_tree_id: search_tree_id_a, .. }, SearchTreeRef { search_tree_id: search_tree_id_b, .. }) =
             self.forest.search_trees_pile.pop()?;

        let search_range = SearchRangeBounds::unbounded();
        let mut search_trees_ids = Vec::new();

        let mut sources_count_items = self.sources_pool.lend(Vec::new);
        sources_count_items.clear();
        let mut sources_build = self.sources_pool.lend(Vec::new);
        sources_build.clear();

        let search_tree_a = self.forest.search_trees.get(&search_tree_id_a).unwrap();
        sources_count_items.push(search_tree_a.build_source(&search_range));
        sources_build.push(search_tree_a.build_source(&search_range));
        self.register_access(search_tree_id_a, &mut search_trees_ids);

        let search_tree_b = self.forest.search_trees.get(&search_tree_id_b).unwrap();
        sources_count_items.push(search_tree_b.build_source(&search_range));
        sources_build.push(search_tree_b.build_source(&search_range));
        self.register_access(search_tree_id_b, &mut search_trees_ids);

        sources_count_items.shrink_to_fit();
        sources_build.shrink_to_fit();

        Some(PendingEventMergeSearchTrees {
            ranges_merger: SearchTreesMerger {
                source_count_items: search_ranges_merge::RangesMergeCps::new(
                    sources_count_items,
                    self.kv_pool.clone(),
                    self.block_entry_steps_pool.clone(),
                ),
                source_build: search_ranges_merge::RangesMergeCps::new(
                    sources_build,
                    self.kv_pool.clone(),
                    self.block_entry_steps_pool.clone(),
                ),
                token: AccessToken {
                    search_trees_ids,
                },
            },
        })
    }

    fn register_access(&mut self, search_tree_id: u64, search_tree_ids: &mut Vec<u64>) {
        register_access_source(
            search_tree_id,
            self.forest.search_trees.get_mut(&search_tree_id).unwrap(),
            search_tree_ids,
            &mut self.forest.accesses_count,
        );
    }

    fn search_trees_merged(&mut self, root_block: BlockRef, items_count: usize, access_token: AccessToken) {
        for search_tree_id in access_token.search_trees_ids {
            match self.forest.search_trees.remove(&search_tree_id) {
                None | Some(SearchTree::Bootstrap(..)) =>
                    unreachable!(),
                Some(SearchTree::Constructed(mut search_tree)) => {
                    assert!(search_tree.accesses_count > 0);
                    search_tree.accesses_count -= 1;
                    assert!(self.forest.accesses_count > 0);
                    self.forest.accesses_count -= 1;
                    if search_tree.accesses_count == 0 {
                        self.demand_search_tree_removal(search_tree);
                    } else {
                        // schedule for decay
                        self.forest.search_trees_decay
                            .insert(search_tree_id, search_tree);
                    }
                },
            }
        }
        let search_tree_id =
            self.forest.add_constructed(root_block, items_count);
        self.forest.search_trees_pile
            .push(SearchTreeRef { search_tree_id, items_count, }, items_count);
    }

    fn demand_search_tree_removal(&mut self, search_tree: SearchTreeConstructed) {
        let walker = search_tree_walker::WalkerCps::new(
            search_tree.root_block,
            SearchRangeBounds::unbounded(),
            self.block_entry_steps_pool.clone(),
        );
        self.pending_events.push(PendingEvent::Demolish { order: DemolishOrder { walker, }, });
    }

    fn poll(mut self) -> Kont<C> {
        // time to flush butcher
        if let Some(event) = self.maybe_flush(false) {
            self.pending_events.push(PendingEvent::FlushButcher(event));
        }
        // flush done
        if !self.pending_flushes.is_empty() && self.forest.flush_friendly() {
            if let Some(event) = self.maybe_flush(true) {
                self.pending_events.push(PendingEvent::FlushButcher(event));
            } else {
                let events = self.pending_flushes
                    .drain(..)
                    .map(|flush_context| PendingEvent::Flushed { flush_context, });
                self.pending_events.extend(events);
            }
        }
        // search trees merge
        while let Some(event) = self.maybe_merge_search_trees() {
            self.pending_events.push(PendingEvent::MergeSearchTrees(event));
        }

        match self.pending_events.pop() {
            None =>
                Kont::Poll(KontPoll { next: KontPollNext { inner: self, }, }),
            Some(PendingEvent::FlushButcher(PendingEventFlushButcher { search_tree_id, frozen_memcache, })) =>
                Kont::FlushButcher(KontFlushButcher {
                    search_tree_id,
                    frozen_memcache,
                    next: KontFlushButcherNext { inner: self, },
                }),
            Some(PendingEvent::Inserted { version, insert_context, }) =>
                Kont::Inserted(KontInserted {
                    version,
                    insert_context,
                    next: KontInsertedNext { inner: self, },
                }),
            Some(PendingEvent::LookupRangeMergerReady { ranges_merger, lookup_context, }) =>
                Kont::LookupRangeMergerReady(KontLookupRangeMergerReady {
                    ranges_merger,
                    lookup_context,
                    next: KontLookupRangeMergerReadyNext { inner: self },
                }),
            Some(PendingEvent::Removed { version, remove_context, }) =>
                Kont::Removed(KontRemoved {
                    version,
                    remove_context,
                    next: KontRemovedNext { inner: self },
                }),
            Some(PendingEvent::Flushed { flush_context, }) =>
                Kont::Flushed(KontFlushed {
                    flush_context,
                    next: KontFlushedNext { inner: self, },
                }),
            Some(PendingEvent::MergeSearchTrees(PendingEventMergeSearchTrees { ranges_merger, })) =>
                Kont::MergeSearchTrees(KontMergeSearchTrees {
                    ranges_merger,
                    next: KontMergeSearchTreesNext { inner: self, },
                }),
            Some(PendingEvent::Demolish { order, }) =>
                Kont::DemolishSearchTree(KontDemolishSearchTree {
                    order,
                    next: KontDemolishSearchTreeNext { inner: self, },
                }),
        }
    }
}

impl<C> KontPollNext<C> where C: Context {
    pub fn incoming_insert(mut self, key: kv::Key, value: kv::Value, insert_context: C::Insert) -> Kont<C> {
        let version = self.inner.insert(key, value);
        self.inner.pending_events.push(PendingEvent::Inserted {
            version,
            insert_context,
        });
        self.inner.poll()
    }

    pub fn begin_lookup_range(mut self, search_range: SearchRangeBounds, lookup_context: C::Lookup) -> Kont<C> {
	let ranges_merger = self.inner.make_lookup_ranges_merger(search_range);
        self.inner.pending_events.push(PendingEvent::LookupRangeMergerReady {
            ranges_merger,
            lookup_context,
        });
        self.inner.poll()
    }

    pub fn commit_lookup_range(mut self, access_token: AccessToken) -> Kont<C> {
        self.inner.commit_lookup_range(access_token);
        self.inner.poll()
    }

    pub fn incoming_remove(mut self, key: kv::Key, remove_context: C::Remove) -> Kont<C> {
        let version = self.inner.remove(key);
        self.inner.pending_events.push(PendingEvent::Removed {
            version,
            remove_context,
        });
        self.inner.poll()
    }

    pub fn incoming_flush(mut self, flush_context: C::Flush) -> Kont<C> {
        self.inner.pending_flushes.push(flush_context);
        self.inner.poll()
    }

    pub fn butcher_flushed(mut self, search_tree_id: u64, root_block: BlockRef) -> Kont<C> {
        self.inner.butcher_flushed(search_tree_id, root_block);
        self.inner.poll()
    }

    pub fn search_trees_merged(
        mut self,
        merged_search_tree_ref: BlockRef,
        merged_search_tree_items_count: usize,
        access_token: AccessToken,
    )
        -> Kont<C>
    {
        self.inner.search_trees_merged(merged_search_tree_ref, merged_search_tree_items_count, access_token);
        self.inner.poll()
    }
}

impl<C> KontInsertedNext<C> where C: Context {
    pub fn got_it(self) -> Kont<C> {
        self.inner.poll()
    }
}

impl<C> KontRemovedNext<C> where C: Context {
    pub fn got_it(self) -> Kont<C> {
        self.inner.poll()
    }
}

impl<C> KontFlushedNext<C> where C: Context {
    pub fn commit_flush(self) -> Kont<C> {
        self.inner.poll()
    }
}

impl<C> KontFlushButcherNext<C> where C: Context {
    pub fn scheduled(self) -> Kont<C> {
        self.inner.poll()
    }
}

impl<C> KontLookupRangeMergerReadyNext<C> where C: Context {
    pub fn got_it(self) -> Kont<C> {
        self.inner.poll()
    }
}

impl<C> KontMergeSearchTreesNext<C> where C: Context {
    pub fn scheduled(self) -> Kont<C> {
        self.inner.poll()
    }
}

impl<C> KontDemolishSearchTreeNext<C> where C: Context {
    pub fn roger_that(self) -> Kont<C> {
        self.inner.poll()
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
            butcher_flushes_count: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.search_trees.len()
    }

    pub fn flush_friendly(&self) -> bool {
        self.butcher_flushes_count == 0 &&
            self.accesses_count == 0
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

impl SearchTree {
    fn build_source(&self, search_range: &SearchRangeBounds) -> LookupRangeSource {
        match self {
            SearchTree::Bootstrap(SearchTreeBootstrap { frozen_memcache, }) =>
                LookupRangeSource {
                    source: search_ranges_merge::Source::Butcher(
                        search_ranges_merge::SourceButcher::new(
                            search_range.clone(),
                            frozen_memcache.clone(),
                        ),
                    ),
                },
            SearchTree::Constructed(SearchTreeConstructed { root_block, .. }) => {
                LookupRangeSource {
                    source: search_ranges_merge::Source::SearchTree(
                        search_ranges_merge::SourceSearchTree::new(
                            search_range.clone(),
                            root_block.clone(),
                        ),
                    ),
                }
            },
        }
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

fn register_access_source(
    search_tree_id: u64,
    search_tree: &mut SearchTree,
    search_tree_ids: &mut Vec<u64>,
    total_accesses_count: &mut usize,
) {
    match search_tree {
        SearchTree::Bootstrap(..) =>
            (),
        SearchTree::Constructed(SearchTreeConstructed { accesses_count, .. }) => {
            *accesses_count += 1;
            *total_accesses_count += 1;
            search_tree_ids.push(search_tree_id);
        },
    }
}
