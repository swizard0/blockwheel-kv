use std::{
    cmp,
    ops::Deref,
    borrow::Borrow,
    collections::BTreeMap,
};

use super::{
    kv,
    wheels::{
        BlockRef,
    },
};

pub mod manager;
pub mod butcher;
pub mod search_tree;

pub type MemCache = BTreeMap<OrdKey, kv::ValueCell>;

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
