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
where Self: Sized + Send + Sync + 'static,
      Self::Info: komm::Echo<Info> + Send + Sync + 'static,
      Self::Insert: komm::Echo<Inserted> + Send + Sync + 'static,
      Self::LookupRange: komm::Echo<komm::Streamzeug<kv::KeyValuePair<kv::Value>>> + Send + Sync + 'static,
      Self::Remove: komm::Echo<Removed> + Send + Sync + 'static,
      Self::Flush: komm::Echo<Flushed> + Send + Sync + 'static,
{
    type Info;
    type Insert;
    type LookupRange;
    type Remove;
    type Flush;
}

#[derive(Debug)]
pub enum Error {
    PerformerVersklaven(arbeitssklave::Error),
    RequestInfo(arbeitssklave::Error),
    RequestInsert(arbeitssklave::Error),
    RequestLookupRange(arbeitssklave::Error),
    RequestLookupRangeNext(arbeitssklave::Error),
    RequestRemove(arbeitssklave::Error),
    RequestFlush(arbeitssklave::Error),
}

pub struct Meister<E> where E: EchoPolicy {
    performer_sklave_meister: arbeitssklave::Meister<core::performer_sklave::Welt<E>, core::performer_sklave::Order<E>>,
    performer_sklave_sendegeraet: komm::Sendegeraet<core::performer_sklave::Order<E>>,
}

impl<E> Clone for Meister<E> where E: EchoPolicy {
    fn clone(&self) -> Self {
        Meister {
            performer_sklave_meister: self.performer_sklave_meister.clone(),
            performer_sklave_sendegeraet: self.performer_sklave_sendegeraet.clone(),
        }
    }
}

impl<E> Meister<E> where E: EchoPolicy {
    pub fn versklaven<J>(
        params: Params,
        blocks_pool: BytesPool,
        version_provider: version::Provider,
        wheels: wheels::Wheels<E>,
        thread_pool: &edeltraud::Handle<J>,
    )
        -> Result<Self, Error>
    where J: Send + 'static,
    {
        let performer_sklave_meister = arbeitssklave::Freie::new()
            .versklaven(
                core::performer_sklave::Welt::new(
                    core::performer_sklave::Env::new(
                        params,
                        blocks_pool,
                        version_provider,
                        wheels,
                    ),
                ),
                thread_pool,
            )
            .map_err(Error::PerformerVersklaven)?;
        let performer_sklave_sendegeraet = performer_sklave_meister
            .sendegeraet()
            .clone();

        Ok(Meister {
            performer_sklave_meister,
            performer_sklave_sendegeraet,
        })
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

#[must_use = "stream is automatically cancelled after drop"]
pub struct LookupRangeStream<E> where E: EchoPolicy {
    stream: komm::Stream<core::performer_sklave::Order<E>>,
}

impl<E> LookupRangeStream<E> where E: EchoPolicy {
    pub fn stream_id(&self) -> &komm::StreamId {
        self.stream.stream_id()
    }

    pub fn next(&self, stream_echo: E::LookupRange, stream_token: komm::StreamToken) -> Result<(), Error> {
        self.stream
            .mehr(
                core::performer_sklave::OrderRequestLookupRangeNext { stream_echo, },
                stream_token,
            )
            .map_err(Error::RequestLookupRangeNext)
    }
}

impl<E> Meister<E> where E: EchoPolicy {
    pub fn info<J>(
        &self,
        echo: E::Info,
        thread_pool: &edeltraud::Handle<J>,
    )
        -> Result<(), Error>
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
            .map_err(Error::RequestInfo)
    }

    pub fn insert<J>(
        &self,
        key: kv::Key,
        value: kv::Value,
        echo: E::Insert,
        thread_pool: &edeltraud::Handle<J>,
    )
        -> Result<(), Error>
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
            .map_err(Error::RequestInsert)
    }

    pub fn lookup_range<R, J>(
        &self,
        range: R,
        stream_echo: E::LookupRange,
        _thread_pool: &edeltraud::Handle<J>,
    )
        -> Result<LookupRangeStream<E>, Error>
    where R: RangeBounds<kv::Key>,
    {
        let stream = self.performer_sklave_sendegeraet
            .stream_starten(
                core::performer_sklave::OrderRequestLookupRange{
                    search_range: range.into(),
                    stream_echo,
                },
            )
            .map_err(Error::RequestLookupRange)?;
        Ok(LookupRangeStream { stream, })
    }

    pub fn remove<J>(
        &self,
        key: kv::Key,
        echo: E::Remove,
        thread_pool: &edeltraud::Handle<J>,
    )
        -> Result<(), Error>
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
            .map_err(Error::RequestRemove)
    }

    pub fn flush<J>(
        &self,
        echo: E::Flush,
        thread_pool: &edeltraud::Handle<J>,
    )
        -> Result<(), Error>
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
            .map_err(Error::RequestFlush)
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
