#![forbid(unsafe_code)]

use std::{
    fmt,
    ops::{
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

pub trait EchoPolicy
where Self: Sized + Send + 'static,
      Self::Info: komm::Echo<Info> + Send + 'static,
      Self::Insert: komm::Echo<Inserted> + Send + 'static,
      Self::LookupRange: komm::Stream<kv::KeyValuePair<kv::Value>, LookupRangeStream<Self>> + Send + 'static,
      Self::Remove: komm::Echo<Removed> + Send + 'static,
      Self::Flush: komm::Echo<Flushed> + Send + 'static,
{
    type Info;
    type Insert;
    type LookupRange;
    type Remove;
    type Flush;
}

#[derive(Debug)]
pub struct LookupRangeStream<E> where E: EchoPolicy {
    next: HideDebug<komm::Rueckkopplung<core::performer_sklave::Order<E>, core::performer_sklave::LookupRangeRoute>>,
}

impl<E> komm::Echo<E::LookupRange> for LookupRangeStream<E> where E: EchoPolicy {
    fn commit_echo<T>(self, inhalt: T) -> Result<(), komm::EchoError> where komm::EchoInhalt<T>: komm::TunInhalt<E::LookupRange> {
        self.next.0.commit_echo(inhalt)
    }
}

#[derive(Debug)]
pub enum Error {
    PerformerVersklaven(arbeitssklave::Error),
    PerformerSendegeraet(komm::Error),
}

pub struct Freie<E> where E: EchoPolicy {
    performer_sklave_freie: arbeitssklave::Freie<core::performer_sklave::Welt<E>, core::performer_sklave::Order<E>>,
}

impl<E> Default for Freie<E> where E: EchoPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> Freie<E> where E: EchoPolicy {
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
        wheels: wheels::Wheels<E>,
        thread_pool: &P,
    )
        -> Result<Meister<E>, Error>
    where P: edeltraud::ThreadPool<job::Job<E>> + Clone + Send + 'static,
    {
        let performer_sklave_sendegeraet =
            komm::Sendegeraet::starten(
                &self.performer_sklave_freie,
                thread_pool.clone(),
            )
            .map_err(Error::PerformerSendegeraet)?;

        let welt = core::performer_sklave::Welt::new(
            core::performer_sklave::Env::new(
                params,
                blocks_pool,
                version_provider,
                wheels,
                performer_sklave_sendegeraet,
            ),
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

pub struct Meister<E> where E: EchoPolicy {
    performer_sklave_meister: arbeitssklave::Meister<core::performer_sklave::Welt<E>, core::performer_sklave::Order<E>>,
}

impl<E> Clone for Meister<E> where E: EchoPolicy {
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

#[derive(Clone, PartialEq, Eq, Hash, Default, Debug)]
pub struct Info {
    pub alive_cells_count: usize,
    pub tombstones_count: usize,
    pub wheels: Vec<WheelInfo>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct WheelInfo {
    pub blockwheel_filename: wheels::WheelFilename,
    pub info: blockwheel_fs::Info,
}

impl<E> Meister<E> where E: EchoPolicy {
    pub fn info<P>(
        &self,
        echo: E::Info,
        thread_pool: &P,
    )
        -> Result<(), arbeitssklave::Error>
    where P: edeltraud::ThreadPool<job::Job<E>>
    {
        self.performer_sklave_meister
            .befehl(
                core::performer_sklave::Order::Request(
                    core::performer_sklave::OrderRequest::Info(
                        core::performer_sklave::OrderRequestInfo {
                            echo,
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
        echo: E::Insert,
        thread_pool: &P,
    )
        -> Result<(), arbeitssklave::Error>
    where P: edeltraud::ThreadPool<job::Job<E>>
    {
        self.performer_sklave_meister
            .befehl(
                core::performer_sklave::Order::Request(
                    core::performer_sklave::OrderRequest::Insert(
                        core::performer_sklave::OrderRequestInsert{
                            key, value, echo,
                        },
                    ),
                ),
                thread_pool,
            )
    }

    pub fn lookup_range<R, P>(
        &self,
        range: R,
        stream: E::LookupRange,
        thread_pool: &P,
    )
        -> Result<(), arbeitssklave::Error>
    where R: RangeBounds<kv::Key>,
          P: edeltraud::ThreadPool<job::Job<E>>,
    {
        self.performer_sklave_meister
            .befehl(
                core::performer_sklave::Order::Request(
                    core::performer_sklave::OrderRequest::LookupRange(
                        core::performer_sklave::OrderRequestLookupRange{
                            search_range: range.into(),
                            stream,
                        },
                    ),
                ),
                thread_pool,
            )
    }

    pub fn remove<P>(
        &self,
        key: kv::Key,
        echo: E::Remove,
        thread_pool: &P,
    )
        -> Result<(), arbeitssklave::Error>
    where P: edeltraud::ThreadPool<job::Job<E>>
    {
        self.performer_sklave_meister
            .befehl(
                core::performer_sklave::Order::Request(
                    core::performer_sklave::OrderRequest::Remove(
                        core::performer_sklave::OrderRequestRemove{
                            key, echo,
                        },
                    ),
                ),
                thread_pool,
            )
    }

    pub fn flush<P>(
        &self,
        echo: E::Flush,
        thread_pool: &P,
    )
        -> Result<(), arbeitssklave::Error>
    where P: edeltraud::ThreadPool<job::Job<E>>
    {
        self.performer_sklave_meister
            .befehl(
                core::performer_sklave::Order::Request(
                    core::performer_sklave::OrderRequest::Flush(
                        core::performer_sklave::OrderRequestFlush {
                            echo,
                        },
                    ),
                ),
                thread_pool,
            )
    }
}

impl Info {
    pub fn reset(&mut self) {
        self.alive_cells_count = 0;
        self.tombstones_count = 0;
        self.wheels.clear();
    }
}

struct HideDebug<T>(T);

impl<T> fmt::Debug for HideDebug<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_tuple("Hidden")
            .field(&"<contents>")
            .finish()
    }
}
