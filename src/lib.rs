#[forbid(unsafe_code)]

use std::{
    ops::{
        AddAssign,
        RangeBounds,
    },
};

use alloc_pool::{
    bytes::{
        BytesPool,
    },
};

use arbeitssklave::{
    komm,
};

pub mod kv;
pub mod job;
pub mod wheels;
pub mod version;

mod core;
mod storage;

#[cfg(test)]
mod tests;

#[derive(Clone, Debug)]
pub struct Params {
    pub butcher_block_size: usize,
    pub tree_block_size: usize,
    pub search_tree_values_inline_size_limit: usize,
    pub search_tree_bootstrap_search_trees_limit: usize,
}

impl Default for Params {
    fn default() -> Params {
        Params {
            butcher_block_size: 128,
            tree_block_size: 32,
            search_tree_values_inline_size_limit: 128,
            search_tree_bootstrap_search_trees_limit: 16,
        }
    }
}

pub trait AccessPolicy: Sized + Send + 'static
where Self::Order: From<komm::UmschlagAbbrechen<Self::Info>>,
      Self::Order: From<komm::Umschlag<Info, Self::Info>>,
      Self::Order: From<komm::UmschlagAbbrechen<Self::Insert>>,
      Self::Order: From<komm::Umschlag<Inserted, Self::Insert>>,
      Self::Order: From<komm::UmschlagAbbrechen<Self::LookupRange>>,
      Self::Order: From<komm::Umschlag<KeyValueStreamItem<Self>, Self::LookupRange>>,
      Self::Order: From<komm::UmschlagAbbrechen<Self::Remove>>,
      Self::Order: From<komm::Umschlag<Removed, Self::Remove>>,
      Self::Order: From<komm::UmschlagAbbrechen<Self::Flush>>,
      Self::Order: From<komm::Umschlag<Flushed, Self::Flush>>,
      Self::Order: Send + 'static,
      Self::Info: Send + 'static,
      Self::Insert: Send + 'static,
      Self::LookupRange: Send + 'static,
      Self::Remove: Send + 'static,
      Self::Flush: Send + 'static,
{
    type Order;
    type Info;
    type Insert;
    type LookupRange;
    type Remove;
    type Flush;
}

pub enum KeyValueStreamItem<A> where A: AccessPolicy {
    KeyValue {
        key_value_pair: kv::KeyValuePair<kv::Value>,
        next: LookupRangeStream<A>,
    },
    NoMore,
}

pub struct LookupRangeStream<A> where A: AccessPolicy {
    next: komm::Rueckkopplung<core::performer_sklave::Order<A>, core::performer_sklave::LookupRangeRoute>,
}

impl<A> LookupRangeStream<A> where A: AccessPolicy {
    pub fn next(self, rueckkopplung: komm::Rueckkopplung<A::Order, A::LookupRange>) -> Result<(), komm::Error> {
        self.next.commit(core::performer_sklave::LookupRangeStreamNext { rueckkopplung, })
    }
}

#[derive(Debug)]
pub enum Error {
    PerformerVersklaven(arbeitssklave::Error),
    PerformerSendegeraet(komm::Error),
}

pub struct Freie<A> where A: AccessPolicy {
    performer_sklave_freie: arbeitssklave::Freie<core::performer_sklave::Welt<A>, core::performer_sklave::Order<A>>,
}

impl<A> Freie<A> where A: AccessPolicy {
    pub fn new() -> Self {
        Self {
            performer_sklave_freie: arbeitssklave::Freie::new(),
        }
    }

    pub fn versklaven<P>(
        self,
        params: Params,
        blocks_pool: BytesPool,
        version_provider: version::Provider,
        wheels: wheels::Wheels<A>,
        thread_pool: &P,
    )
        -> Result<Meister<A>, Error>
    where P: edeltraud::ThreadPool<job::Job<A>> + Clone + Send + 'static,
    {
        let performer_sklave_sendegeraet =
            komm::Sendegeraet::starten(
                &self.performer_sklave_freie,
                thread_pool.clone(),
            )
            .map_err(Error::PerformerSendegeraet)?;

        let welt = core::performer_sklave::Welt::new(
            core::performer_sklave::Env {
                params,
                blocks_pool,
                version_provider,
                wheels,
                sendegeraet: performer_sklave_sendegeraet,
                incoming_orders: Vec::new(),
                delayed_orders: Vec::new(),
            }
        );

        let performer_sklave_meister = self
            .performer_sklave_freie
            .versklaven(welt, thread_pool)
            .map_err(Error::PerformerVersklaven)?;

        Ok(Meister {
            performer_sklave_meister,
        })
    }
}

pub struct Meister<A> where A: AccessPolicy {
    performer_sklave_meister: arbeitssklave::Meister<core::performer_sklave::Welt<A>, core::performer_sklave::Order<A>>,
}

impl<A> Clone for Meister<A> where A: AccessPolicy {
    fn clone(&self) -> Self {
        Meister {
            performer_sklave_meister: self.performer_sklave_meister.clone(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Inserted {
    pub version: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Removed {
    pub version: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Flushed;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, Debug)]
pub struct Info {
    pub alive_cells_count: usize,
    pub tombstones_count: usize,
}

impl<A> Meister<A> where A: AccessPolicy {
    pub fn info<P>(
        &self,
        rueckkopplung: komm::Rueckkopplung<A::Order, A::Info>,
        thread_pool: &P,
    )
        -> Result<(), arbeitssklave::Error>
    where P: edeltraud::ThreadPool<job::Job<A>>
    {
        self.performer_sklave_meister
            .befehl(
                core::performer_sklave::Order::Request(
                    core::performer_sklave::OrderRequest::Info(
                        core::performer_sklave::OrderRequestInfo {
                            rueckkopplung,
                        },
                    ),
                ),
                thread_pool,
            )
    }

    pub fn insert<P>(
        &self,
        key: kv::Key,
        value: kv::Value,
        rueckkopplung: komm::Rueckkopplung<A::Order, A::Insert>,
        thread_pool: &P,
    )
        -> Result<(), arbeitssklave::Error>
    where P: edeltraud::ThreadPool<job::Job<A>>
    {
        self.performer_sklave_meister
            .befehl(
                core::performer_sklave::Order::Request(
                    core::performer_sklave::OrderRequest::Insert(
                        core::performer_sklave::OrderRequestInsert{
                            key, value, rueckkopplung,
                        },
                    ),
                ),
                thread_pool,
            )
    }

    pub fn lookup_range<R, P>(
        &self,
        range: R,
        rueckkopplung: komm::Rueckkopplung<A::Order, A::LookupRange>,
        thread_pool: &P,
    )
        -> Result<(), arbeitssklave::Error>
    where R: RangeBounds<kv::Key>,
          P: edeltraud::ThreadPool<job::Job<A>>,
    {
        self.performer_sklave_meister
            .befehl(
                core::performer_sklave::Order::Request(
                    core::performer_sklave::OrderRequest::LookupRange(
                        core::performer_sklave::OrderRequestLookupRange{
                            search_range: range.into(),
                            rueckkopplung,
                        },
                    ),
                ),
                thread_pool,
            )
    }

    pub fn remove<P>(
        &self,
        key: kv::Key,
        rueckkopplung: komm::Rueckkopplung<A::Order, A::Remove>,
        thread_pool: &P,
    )
        -> Result<(), arbeitssklave::Error>
    where P: edeltraud::ThreadPool<job::Job<A>>
    {
        self.performer_sklave_meister
            .befehl(
                core::performer_sklave::Order::Request(
                    core::performer_sklave::OrderRequest::Remove(
                        core::performer_sklave::OrderRequestRemove{
                            key, rueckkopplung,
                        },
                    ),
                ),
                thread_pool,
            )
    }

    pub fn flush<P>(
        &self,
        rueckkopplung: komm::Rueckkopplung<A::Order, A::Flush>,
        thread_pool: &P,
    )
        -> Result<(), arbeitssklave::Error>
    where P: edeltraud::ThreadPool<job::Job<A>>
    {
        self.performer_sklave_meister
            .befehl(
                core::performer_sklave::Order::Request(
                    core::performer_sklave::OrderRequest::Flush(
                        core::performer_sklave::OrderRequestFlush {
                            rueckkopplung,
                        },
                    ),
                ),
                thread_pool,
            )
    }
}

impl AddAssign for Info {
    fn add_assign(&mut self, rhs: Info) {
        self.alive_cells_count += rhs.alive_cells_count;
        self.tombstones_count += rhs.tombstones_count;
    }
}

impl Info {
    pub fn reset(&mut self) {
        self.alive_cells_count = 0;
        self.tombstones_count = 0;
    }
}
