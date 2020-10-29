use std::{
    sync::Arc,
    time::Duration,
};

use futures::{
    select,
    stream::{
        self,
        FuturesUnordered,
    },
    channel::{
        mpsc,
        oneshot,
    },
    StreamExt,
    SinkExt,
};

use o1::{
    set::{
        Set,
        Ref,
    },
};

use ero::{
    restart,
    ErrorSeverity,
    RestartStrategy,
};

use alloc_pool::bytes::BytesPool;

use ero_blockwheel_fs as blockwheel;

use super::{
    kv,
    proto,
    butcher,
    context,
    kv_context,
};

#[derive(Clone, Debug)]
pub struct Params {
    pub task_restart_sec: usize,
}

impl Default for Params {
    fn default() -> Params {
        Params {
            task_restart_sec: 4,
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
        blocks_pool: BytesPool,
        butcher_pid: butcher::Pid,
        blockwheel_pid: blockwheel::Pid,
        params: Params,
    )
    {
        let terminate_result = restart::restartable(
            ero::Params {
                name: "ero-blockwheel-kv manager task",
                restart_strategy: RestartStrategy::Delay {
                    restart_after: Duration::from_secs(params.task_restart_sec as u64),
                },
            },
            State {
                fused_request_rx: self.fused_request_rx,
                blocks_pool,
                butcher_pid,
                blockwheel_pid,
                params,
            },
            |state| busyloop(state),
        ).await;
        if let Err(error) = terminate_result {
            log::error!("fatal error: {:?}", error);
        }
    }
}

struct State {
    fused_request_rx: stream::Fuse<mpsc::Receiver<Request>>,
    blocks_pool: BytesPool,
    butcher_pid: butcher::Pid,
    blockwheel_pid: blockwheel::Pid,
    params: Params,
}

enum Request {
    ButcherFlush {
        cache: Arc<butcher::Cache>,
        current_block_size: usize,
    },
    Lookup(proto::RequestLookup<<kv_context::Context as context::Context>::Lookup>),
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Flushed;

#[derive(Debug)]
pub enum LookupError {
    GenServer(ero::NoProcError),
}

impl Pid {
    pub async fn flush_cache(&mut self, cache: Arc<butcher::Cache>, current_block_size: usize) -> Result<Flushed, ero::NoProcError> {
        self.request_tx.send(Request::ButcherFlush { cache: cache.clone(), current_block_size, }).await
            .map_err(|_send_error| ero::NoProcError)?;
        Ok(Flushed)
    }

    pub async fn lookup(&mut self, key: kv::Key) -> Result<Option<kv::KeyValue>, LookupError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(Request::Lookup(proto::RequestLookup {
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

#[derive(Debug)]
enum Error {
}

struct LookupRequest {
    reply_tx: <kv_context::Context as context::Context>::Lookup,
    pending: usize,
}

async fn busyloop(mut state: State) -> Result<(), ErrorSeverity<State, Error>> {
    let mut lookup_requests = Set::new();
    let mut lookup_tasks = FuturesUnordered::new();

    loop {
        enum Event<R, L> {
            Request(Option<R>),
            LookupTask(L),
        }

        let event = if lookup_tasks.is_empty() {
            Event::Request(state.fused_request_rx.next().await)
        } else {
            select! {
                result = state.fused_request_rx.next() =>
                    Event::Request(result),
                result = lookup_tasks.next() => match result {
                    None =>
                        unreachable!(),
                    Some(lookup_task) =>
                        Event::LookupTask(lookup_task),
                },
            }
        };

        match event {
            Event::Request(None) => {
                log::info!("requests sink channel depleted: terminating");
                return Ok(());
            },

            Event::Request(Some(Request::ButcherFlush { cache, current_block_size, })) =>
                unimplemented!(),

            Event::Request(Some(Request::Lookup(proto::RequestLookup { key, context: reply_tx, }))) => {
                let request_ref = lookup_requests.insert(LookupRequest {
                    reply_tx,
                    pending: 1,
                });
                let mut butcher_pid = state.butcher_pid.clone();
                let key = key.clone();
                lookup_tasks.push(async move {
                    let result = butcher_pid.lookup(key).await
                        .map_err(|error| match error {
                            butcher::LookupError::GenServer(ero::NoProcError) =>
                                LookupError::GenServer(ero::NoProcError),
                        });
                    (request_ref, result)
                });

                unimplemented!()
            },

            Event::LookupTask((request_ref, result)) =>
                if let Some(lookup_request) = lookup_requests.get_mut(request_ref) {

                    unreachable!()
                } else {
                    log::error!("something went wrong: lookup task ready for nonexisting task ref = {:?}, ignoring", request_ref);
                },
        }
    }
}
