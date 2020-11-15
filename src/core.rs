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

use futures::{
    channel::{
        oneshot,
    },
};

use super::{
    kv,
    wheels::{
        BlockRef,
    },
    Info,
    Inserted,
    Removed,
    Flushed,
    LookupRange,
};

pub mod manager;
pub mod butcher;
pub mod search_tree;
pub mod merger;

#[derive(Debug)]
pub struct RequestInfo {
    reply_tx: oneshot::Sender<Info>,
}

#[derive(Debug)]
pub struct RequestInsert {
    key: kv::Key,
    value: kv::Value,
    reply_tx: oneshot::Sender<Inserted>,
}

#[derive(Debug)]
pub struct RequestLookup {
    key: kv::Key,
    reply_tx: oneshot::Sender<Option<kv::ValueCell>>,
}

pub struct RequestLookupRange {
    range: SearchRangeBounds,
    reply_tx: oneshot::Sender<LookupRange>,
}

#[derive(Debug)]
pub struct RequestRemove {
    key: kv::Key,
    reply_tx: oneshot::Sender<Removed>,
}

#[derive(Debug)]
pub struct RequestFlush {
    reply_tx: oneshot::Sender<Flushed>,
}

pub struct MemCache {
    cache: BTreeMap<OrdKey, kv::ValueCell>,
}

impl MemCache {
    fn new() -> MemCache {
        MemCache {
            cache: BTreeMap::new(),
        }
    }

    fn range(&self, range: SearchRangeBounds) -> impl Iterator<Item = kv::KeyValuePair> + '_ {
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
    type Target = BTreeMap<OrdKey, kv::ValueCell>;

    fn deref(&self) -> &Self::Target {
        &self.cache
    }
}

impl DerefMut for MemCache {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cache
    }
}

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
