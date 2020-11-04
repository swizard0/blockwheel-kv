use std::{
    cmp,
    ops::Deref,
    borrow::Borrow,
    collections::BTreeMap,
};

use super::{
    kv::{
        KeyValue,
        ContainsKey,
    },
    wheels::{
        WheelFilename,
    },
    blockwheel::{
        block,
    },
};

pub mod manager;
pub mod butcher;
pub mod search_tree;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct BlockRef {
    blockwheel_filename: WheelFilename,
    block_id: block::Id,
}

pub type MemCache = BTreeMap<OrdKey<KeyValue>, ()>;

#[derive(Clone, Debug)]
enum ValueFound {
    Value { kv: kv::KeyValue, },
    Tombstone,
    Blackmark,
}

#[derive(Clone, Debug)]
pub struct OrdKey<T> {
    inner: T,
}

impl<T> OrdKey<T> {
    fn new(inner: T) -> OrdKey<T> {
        OrdKey { inner, }
    }

    fn into_inner(self) -> T {
        self.inner
    }
}

impl<T> AsRef<T> for OrdKey<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        &self.inner
    }
}

impl<T> Deref for OrdKey<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        self.as_ref()
    }
}

impl<T> PartialEq for OrdKey<T> where T: ContainsKey {
    fn eq(&self, other: &OrdKey<T>) -> bool {
        self.inner.key_data() == other.inner.key_data()
    }
}

impl<T> Eq for OrdKey<T> where T: ContainsKey { }

impl<T> PartialOrd for OrdKey<T> where T: ContainsKey {
    fn partial_cmp(&self, other: &OrdKey<T>) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for OrdKey<T> where T: ContainsKey {
    fn cmp(&self, other: &OrdKey<T>) -> cmp::Ordering {
        self.inner.key_data().cmp(other.inner.key_data())
    }
}

impl<T> Borrow<[u8]> for OrdKey<T> where T: ContainsKey {
    fn borrow(&self) -> &[u8] {
        self.inner.key_data()
    }
}
