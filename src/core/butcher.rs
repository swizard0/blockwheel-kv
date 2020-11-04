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
    kv::{
        self,
        ContainsKey,
    },
    proto,
    kv_context,
    Info,
    Inserted,
    core::{
        manager,
        OrdKey,
        MemCache,
    },
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

type Request = proto::Request<kv_context::Context>;

#[derive(Debug)]
pub enum InsertError {
    GenServer(ero::NoProcError),
    NoSpaceLeft,
}

#[derive(Debug)]
pub enum LookupError {
    GenServer(ero::NoProcError),
}

impl Pid {
    pub async fn info(&mut self) -> Result<Info, ero::NoProcError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx.send(Request::Info(proto::RequestInfo { context: reply_tx, })).await
                .map_err(|_send_error| ero::NoProcError)?;
            match reply_rx.await {
                Ok(info) =>
                    return Ok(info),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn insert(&mut self, key_value: kv::KeyValue) -> Result<Inserted, InsertError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(Request::Insert(proto::RequestInsert {
                    key_value: key_value.clone(),
                    context: reply_tx,
                }))
                .await
                .map_err(|_send_error| InsertError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(Ok(Inserted)) =>
                    return Ok(Inserted),
                Ok(Err(kv_context::RequestInsertError::NoSpaceLeft)) =>
                     return Err(InsertError::NoSpaceLeft),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn lookup(&mut self, key: kv::Key) -> Result<Option<kv::KeyValue>, LookupError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(proto::Request::Lookup(proto::RequestLookup {
                    key: key.clone(),
                    context: reply_tx,
                }))
                .await
                .map_err(|_send_error| LookupError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(Ok(result)) =>
                    return Ok(result),
                Ok(Err(..)) =>
                    unreachable!(),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }
}

struct State {
    fused_request_rx: stream::Fuse<mpsc::Receiver<Request>>,
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
            proto::Request::Info(..) =>
                unimplemented!(),

            proto::Request::Insert(proto::RequestInsert { key_value, context: reply_tx, }) => {
                memcache.insert(OrdKey::new(key_value.clone()), ());
                if let Err(_send_error) = reply_tx.send(Ok(Inserted)) {
                    log::warn!("client canceled insert request");
                    memcache.remove(key_value.key_data());
                } else if memcache.len() >= state.params.tree_block_size {
                    // flush tree block
                    let cache = Arc::new(mem::replace(&mut memcache, MemCache::new()));
                    if let Err(ero::NoProcError) = state.manager_pid.flush_cache(cache).await {
                        log::warn!("manager has gone during flush, terminating");
                        break;
                    }
                }
            },

            proto::Request::Lookup(proto::RequestLookup { key, context: reply_tx, }) => {
                let result = memcache.get_key_value(key.key_data())
                    .map(|(key, ())| key.as_ref().clone());
                if let Err(_send_error) = reply_tx.send(Ok(result)) {
                    log::warn!("client canceled lookup request");
                }
            }
        }
    }
    Ok(())
}
