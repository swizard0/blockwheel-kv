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
        OrdKey,
        MemCache,
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
    PollRequest(KontPollRequest),
    Inserted(KontInserted),
}

pub struct KontPollRequest {
    inner: Inner,
}

pub struct KontInserted {
    pub next: KontInsertedNext,
}

pub struct KontInsertedNext {
    inner: Inner,
    next_action: KontInsertedNextAction,
}

enum KontInsertedNextAction {
    PollRequest,
}

struct Inner {
    params: Params,
    version_provider: version::Provider,
    butcher: MemCache,
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
        Kont::PollRequest(KontPollRequest { inner: self.inner, })
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

    fn maybe_flush(&mut self) -> Option<Arc<MemCache>> {
        if self.butcher.len() >= self.params.tree_block_size {
            let flushed_memcache =
                Arc::new(mem::replace(&mut self.butcher, MemCache::new()));
            self.info.reset();
            Some(flushed_memcache)
        } else {
            None
        }
    }
}

impl KontPollRequest {
    pub fn incoming_insert(mut self, key: kv::Key, value: kv::Value) -> Kont {
        self.inner.insert(key, value);
	let next_action = match self.inner.maybe_flush() {
            None =>
                KontInsertedNextAction::PollRequest,
            Some(flushed_memcache) =>
                todo!(),
        };
        Kont::Inserted(KontInserted {
            next: KontInsertedNext {
                inner: self.inner,
                next_action,
            },
        })
    }
}
