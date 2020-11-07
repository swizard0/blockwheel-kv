use std::{
    mem,
    sync::Arc,
    time::Duration,
};

use futures::{
    stream,
    channel::{
        mpsc,
        oneshot,
    },
    StreamExt,
    SinkExt,
};

use ero::{
    restart,
    ErrorSeverity,
    RestartStrategy,
};

use crate::{
    kv,
    version,
    core::{
        manager,
        OrdKey,
        MemCache,
    },
    Info,
    Request,
    Inserted,
    Removed,
    RequestInfo,
    RequestInsert,
    RequestLookup,
    RequestRemove,
};

#[derive(Clone, Debug)]
pub struct Params {
    pub task_restart_sec: usize,
    pub tree_block_size: usize,
}

impl Default for Params {
    fn default() -> Params {
        Params {
            task_restart_sec: 4,
            tree_block_size: 32,
        }
    }
}

pub struct GenServer {
    request_tx: mpsc::Sender<Request>,
    fused_request_rx: stream::Fuse<mpsc::Receiver<Request>>,
}

#[derive(Clone)]
pub struct Pid {
    request_tx: mpsc::Sender<Request>,
}

impl GenServer {
    pub fn new() -> GenServer {
        let (request_tx, request_rx) = mpsc::channel(0);
        GenServer {
            request_tx,
            fused_request_rx: request_rx.fuse(),
        }
    }

    pub fn pid(&self) -> Pid {
        Pid {
            request_tx: self.request_tx.clone(),
        }
    }

    pub async fn run(
        self,
        version_provider: version::Provider,
        manager_pid: manager::Pid,
        params: Params,
    )
    {
        let terminate_result = restart::restartable(
            ero::Params {
                name: "ero-blockwheel-kv memcache task",
                restart_strategy: RestartStrategy::Delay {
                    restart_after: Duration::from_secs(params.task_restart_sec as u64),
                },
            },
            State {
                fused_request_rx: self.fused_request_rx,
                version_provider,
                manager_pid,
                params,
            },
            |state| busyloop(state),
        ).await;
        if let Err(error) = terminate_result {
            log::error!("fatal error: {:?}", error);
        }
    }
}

impl Pid {
    pub async fn info(&mut self) -> Result<Info, ero::NoProcError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx.send(Request::Info(RequestInfo { reply_tx, })).await
                .map_err(|_send_error| ero::NoProcError)?;
            match reply_rx.await {
                Ok(info) =>
                    return Ok(info),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn insert(&mut self, key: kv::Key, value: kv::Value) -> Result<Inserted, ero::NoProcError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx.send(Request::Insert(RequestInsert { key: key.clone(), value: value.clone(), reply_tx, })).await
                .map_err(|_send_error| ero::NoProcError)?;

            match reply_rx.await {
                Ok(inserted) =>
                    return Ok(inserted),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn lookup(&mut self, key: kv::Key) -> Result<Option<kv::ValueCell>, ero::NoProcError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx.send(Request::Lookup(RequestLookup { key: key.clone(), reply_tx, })).await
                .map_err(|_send_error| ero::NoProcError)?;

            match reply_rx.await {
                Ok(result) =>
                    return Ok(result),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn remove(&mut self, key: kv::Key) -> Result<Removed, ero::NoProcError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx.send(Request::Remove(RequestRemove { key: key.clone(), reply_tx, })).await
                .map_err(|_send_error| ero::NoProcError)?;

            match reply_rx.await {
                Ok(Removed) =>
                    return Ok(Removed),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }
}

struct State {
    fused_request_rx: stream::Fuse<mpsc::Receiver<Request>>,
    version_provider: version::Provider,
    manager_pid: manager::Pid,
    params: Params,
}

#[derive(Debug)]
enum Error {
}

async fn busyloop(mut state: State) -> Result<(), ErrorSeverity<State, Error>> {
    let mut memcache = MemCache::new();

    while let Some(request) = state.fused_request_rx.next().await {
        match request {
            Request::Info(RequestInfo { reply_tx, }) => {
                let info = Info { };
                if let Err(_send_error) = reply_tx.send(info) {
                    log::warn!("client canceled info request");
                }
            },

            Request::Insert(RequestInsert { key, value, reply_tx, }) => {
                let ord_key = OrdKey::new(key);
                let version = state.version_provider.obtain();
                let value_cell = kv::ValueCell {
                    version,
                    cell: kv::Cell::Value(value),
                };
                let maybe_prev = memcache.insert(ord_key.clone(), value_cell);
                if let Err(_send_error) = reply_tx.send(Inserted { version, }) {
                    log::warn!("client canceled insert request");
                    match maybe_prev {
                        None => {
                            memcache.remove(&ord_key);
                        },
                        Some(prev_value_cell) => {
                            memcache.insert(ord_key, prev_value_cell);
                        },
                    }
                } else if memcache.len() >= state.params.tree_block_size {
                    // flush tree block
                    let cache = Arc::new(mem::replace(&mut memcache, MemCache::new()));
                    if let Err(ero::NoProcError) = state.manager_pid.flush_cache(cache).await {
                        log::warn!("manager has gone during flush, terminating");
                        break;
                    }
                }
            },

            Request::Lookup(RequestLookup { key, reply_tx, }) => {
                let lookup_result = memcache.get(&**key.key_bytes)
                    .cloned();
                if let Err(_send_error) = reply_tx.send(lookup_result) {
                    log::warn!("client canceled lookup request");
                }
            },

            Request::Remove(RequestRemove { key, reply_tx, }) => {
                let ord_key = OrdKey::new(key);
                let value_cell = kv::ValueCell {
                    version: state.version_provider.obtain(),
                    cell: kv::Cell::Tombstone,
                };
                let maybe_prev = memcache.insert(ord_key.clone(), value_cell);
                if let Err(_send_error) = reply_tx.send(Removed) {
                    log::warn!("client canceled remove request");
                    match maybe_prev {
                        None => {
                            memcache.remove(&ord_key);
                        },
                        Some(prev_value_cell) => {
                            memcache.insert(ord_key, prev_value_cell);
                        },
                    }
                } else if memcache.len() >= state.params.tree_block_size {
                    // flush tree block
                    let cache = Arc::new(mem::replace(&mut memcache, MemCache::new()));
                    if let Err(ero::NoProcError) = state.manager_pid.flush_cache(cache).await {
                        log::warn!("manager has gone during flush, terminating");
                        break;
                    }
                }
            },
        }
    }
    Ok(())
}
