use std::{
    cmp,
    ops::{
        Deref,
        DerefMut,
        Bound,
        RangeBounds,
    },
    borrow::Borrow,
    collections::BTreeMap,
};

use alloc_pool::{
    bytes::{
        Bytes,
    },
    Unique,
};

use arbeitssklave::{
    komm::{
        self,
        Echo,
    },
};

use crate::{
    kv,
    storage,
    wheels::{
        BlockRef,
    },
    EchoPolicy,
};

pub mod performer_sklave;

use performer_sklave::{
    running::{
        flush_butcher,
        lookup_range_merge,
        merge_search_trees,
        demolish_search_tree,
    },
};

mod merger;
mod context;
mod performer;
mod bin_merger;
mod search_tree_walker;
mod search_tree_builder;
mod search_ranges_merge;

#[cfg(test)]
mod tests;

// blockwheel_fs rueckkopplung

pub type FsInfo<E> = komm::Rueckkopplung<performer_sklave::Order<E>, performer_sklave::WheelRouteInfo>;
pub type FsFlush<E> = komm::Rueckkopplung<performer_sklave::Order<E>, performer_sklave::WheelRouteFlush>;

pub enum FsWriteBlock {
    FlushButcher {
        rueckkopplung: komm::Rueckkopplung<flush_butcher::Order, flush_butcher::WriteBlockTarget>,
    },
    MergeSearchTrees {
        rueckkopplung: komm::Rueckkopplung<merge_search_trees::Order, merge_search_trees::WriteBlockTarget>,
    },
}

impl Echo<Result<blockwheel_fs::block::Id, blockwheel_fs::RequestWriteBlockError>> for FsWriteBlock {
    fn commit_echo(self, inhalt: Result<blockwheel_fs::block::Id, blockwheel_fs::RequestWriteBlockError>) -> Result<(), komm::EchoError> {
        match self {
            FsWriteBlock::FlushButcher { rueckkopplung, } =>
                rueckkopplung.commit_echo(inhalt),
            FsWriteBlock::MergeSearchTrees { rueckkopplung, } =>
                rueckkopplung.commit_echo(inhalt),
        }
    }
}

pub enum FsReadBlock<E> where E: EchoPolicy {
    LookupRangeMerge {
        rueckkopplung: komm::Rueckkopplung<lookup_range_merge::Order<E>, lookup_range_merge::ReadBlockTarget>,
    },
    MergeSearchTrees {
        rueckkopplung: komm::Rueckkopplung<merge_search_trees::Order, merge_search_trees::ReadBlockTarget>,
    },
    DemolishSearchTree {
        rueckkopplung: komm::Rueckkopplung<demolish_search_tree::Order, demolish_search_tree::ReadBlockTarget>,
    },
}

impl<E> Echo<Result<Bytes, blockwheel_fs::RequestReadBlockError>> for FsReadBlock<E> where E: EchoPolicy {
    fn commit_echo(self, inhalt: Result<Bytes, blockwheel_fs::RequestReadBlockError>) -> Result<(), komm::EchoError> {
        match self {
            FsReadBlock::LookupRangeMerge { rueckkopplung, } =>
                rueckkopplung.commit_echo(inhalt),
            FsReadBlock::MergeSearchTrees { rueckkopplung, } =>
                rueckkopplung.commit_echo(inhalt),
            FsReadBlock::DemolishSearchTree { rueckkopplung, } =>
                rueckkopplung.commit_echo(inhalt),
        }
    }
}

// types

pub type SearchTreeBuilderCps =
    search_tree_builder::BuilderCps<kv::KeyValuePair<storage::ValueRef>, BlockRef>;
pub type SearchTreeBuilderKont =
    search_tree_builder::Kont<kv::KeyValuePair<storage::ValueRef>, BlockRef>;
pub type SearchTreeBuilderBlockEntry =
    search_tree_builder::BlockEntry<kv::KeyValuePair<storage::ValueRef>, BlockRef>;
pub type SearchTreeBuilderBlockNext =
    search_tree_builder::KontPollProcessedBlockNext<kv::KeyValuePair<storage::ValueRef>, BlockRef>;
pub type SearchTreeBuilderItemOrBlockNext =
    search_tree_builder::KontPollNextItemOrProcessedBlockNext<kv::KeyValuePair<storage::ValueRef>, BlockRef>;

pub type SearchRangesMergeCps =
    search_ranges_merge::RangesMergeCps<Unique<Vec<performer::LookupRangeSource>>, performer::LookupRangeSource>;
pub type SearchRangesMergeKont =
    search_ranges_merge::Kont<Unique<Vec<performer::LookupRangeSource>>, performer::LookupRangeSource>;
pub type SearchRangesMergeBlockNext =
    search_ranges_merge::KontAwaitBlocksNext<Unique<Vec<performer::LookupRangeSource>>, performer::LookupRangeSource>;
pub type SearchRangesMergeItemNext =
    search_ranges_merge::KontEmitItemNext<Unique<Vec<performer::LookupRangeSource>>, performer::LookupRangeSource>;

// MemCache

pub struct MemCache {
    cache: BTreeMap<OrdKey, kv::ValueCell<kv::Value>>,
}

impl MemCache {
    fn new() -> MemCache {
        MemCache {
            cache: BTreeMap::new(),
        }
    }

    fn range(&self, range: SearchRangeBounds) -> impl Iterator<Item = kv::KeyValuePair<kv::Value>> + '_ {
        fn ord_key_map(bound: Bound<kv::Key>) -> Bound<OrdKey> {
            match bound {
                Bound::Unbounded =>
                    Bound::Unbounded,
                Bound::Included(key) =>
                    Bound::Included(OrdKey::new(key)),
                Bound::Excluded(key) =>
                    Bound::Excluded(OrdKey::new(key)),
            }
        }

        let ord_key_range = (ord_key_map(range.range_from), ord_key_map(range.range_to));
        self.cache.range(ord_key_range)
            .map(|(ord_key, value_cell)| kv::KeyValuePair {
                key: ord_key.as_ref().clone(),
                value_cell: value_cell.clone(),
            })
    }
}

impl Deref for MemCache {
    type Target = BTreeMap<OrdKey, kv::ValueCell<kv::Value>>;

    fn deref(&self) -> &Self::Target {
        &self.cache
    }
}

impl DerefMut for MemCache {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cache
    }
}

// SearchRangeBounds

#[derive(Clone, Debug)]
pub struct SearchRangeBounds {
    range_from: Bound<kv::Key>,
    range_to: Bound<kv::Key>,
}

impl SearchRangeBounds {
    fn unbounded() -> SearchRangeBounds {
        SearchRangeBounds {
            range_from: Bound::Unbounded,
            range_to: Bound::Unbounded,
        }
    }
}

impl<R> From<R> for SearchRangeBounds where R: RangeBounds<kv::Key> {
    fn from(range: R) -> SearchRangeBounds {
        SearchRangeBounds {
            range_from: match range.start_bound() {
                Bound::Unbounded =>
                    Bound::Unbounded,
                Bound::Included(key) =>
                    Bound::Included(key.clone()),
                Bound::Excluded(key) =>
                    Bound::Excluded(key.clone()),
            },
            range_to: match range.end_bound() {
                Bound::Unbounded =>
                    Bound::Unbounded,
                Bound::Included(key) =>
                    Bound::Included(key.clone()),
                Bound::Excluded(key) =>
                    Bound::Excluded(key.clone()),
            },
        }
    }
}

// OrdKey

#[derive(Clone, Debug)]
pub struct OrdKey {
    inner: kv::Key,
}

impl OrdKey {
    fn new(inner: kv::Key) -> OrdKey {
        OrdKey { inner, }
    }
}

impl AsRef<kv::Key> for OrdKey {
    #[inline]
    fn as_ref(&self) -> &kv::Key {
        &self.inner
    }
}

impl Deref for OrdKey {
    type Target = kv::Key;

    #[inline]
    fn deref(&self) -> &kv::Key {
        self.as_ref()
    }
}

impl PartialEq for OrdKey {
    fn eq(&self, other: &OrdKey) -> bool {
        self.inner.key_bytes == other.inner.key_bytes
    }
}

impl Eq for OrdKey { }

impl PartialOrd for OrdKey {
    fn partial_cmp(&self, other: &OrdKey) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdKey {
    fn cmp(&self, other: &OrdKey) -> cmp::Ordering {
        self.inner.key_bytes.cmp(&other.inner.key_bytes)
    }
}

impl Borrow<[u8]> for OrdKey {
    fn borrow(&self) -> &[u8] {
        &self.inner.key_bytes
    }
}
