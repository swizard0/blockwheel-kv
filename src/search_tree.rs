use std::{
    sync::Arc,
    time::Duration,
};

use futures::{
    select,
    stream,
    pin_mut,
    channel::{
        mpsc,
        oneshot,
    },
    SinkExt,
    StreamExt,
    FutureExt,
};

use ero::{
    restart,
    ErrorSeverity,
    RestartStrategy,
    supervisor::SupervisorPid,
};

use alloc_pool::bytes::BytesPool;

use super::{
    kv,
    storage,
    butcher,
    blockwheel::{
        self,
        block,
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
            task_restart_sec: 1,
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

    pub async fn run_cache_bootstrap(
        self,
        parent_supervisor: SupervisorPid,
        blocks_pool: BytesPool,
        blockwheel_pid: blockwheel::Pid,
        params: Params,
        cache: Arc<butcher::Cache>,
    )
    {
        run(State {
            fused_request_rx: self.fused_request_rx,
            parent_supervisor,
            blocks_pool,
            blockwheel_pid,
            params,
            mode: Mode::CacheBootstrap { cache, },
        }).await
    }
}

#[derive(Clone, Debug)]
pub enum Found {
    Nothing,
    InCache { kv: kv::KeyValue, },
    InBlock { kv: kv::KeyValue, block_id: block::Id, },
}

#[derive(Debug)]
pub enum LookupError {
    GenServer(ero::NoProcError),
}

impl Pid {
    pub async fn lookup(&mut self, key: kv::Key) -> Result<Found, LookupError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(Request::Lookup {
                    key: key.clone(),
                    reply_tx,
                })
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
    parent_supervisor: SupervisorPid,
    blocks_pool: BytesPool,
    blockwheel_pid: blockwheel::Pid,
    params: Params,
    mode: Mode,
}

enum Mode {
    CacheBootstrap {
        cache: Arc<butcher::Cache>,
    },
    Regular {
        root_block_id: block::Id,
    },
}

async fn run(state: State) {
    let terminate_result = restart::restartable(
        ero::Params {
            name: "ero-blockwheel-kv search tree task",
            restart_strategy: RestartStrategy::Delay {
                restart_after: Duration::from_secs(state.params.task_restart_sec as u64),
            },
        },
        state,
        |mut state| async move {
            let child_supervisor_gen_server = state.parent_supervisor.child_supevisor();
            let child_supervisor_pid = child_supervisor_gen_server.pid();
            state.parent_supervisor.spawn_link_temporary(
                child_supervisor_gen_server.run(),
            );

            busyloop(child_supervisor_pid, state).await
        },
    ).await;
    if let Err(error) = terminate_result {
        log::error!("fatal error: {:?}", error);
    }
}

enum Request {
    Lookup { key: kv::Key, reply_tx: oneshot::Sender<Result<Found, SearchTreeLookupError>>, },
}

#[derive(Debug)]
enum Error {
    BootstrapSerializeBlockStorage(storage::Error),
    BootstrapSerializeBlockJoin(tokio::task::JoinError),
    BootstrapWriteBlock(blockwheel::WriteBlockError),
}

#[derive(Debug)]
enum SearchTreeLookupError {
}

async fn busyloop(_child_supervisor_pid: SupervisorPid, mut state: State) -> Result<(), ErrorSeverity<State, Error>> {

    let mut maybe_bootstrap_task = match &state.mode {
        Mode::CacheBootstrap { cache, } => {
            let fused_task = bootstrap_task(cache.clone(), state.blocks_pool.clone(), state.blockwheel_pid.clone())
                .fuse();
            pin_mut!(fused_task);
            Some(fused_task)
        },
        Mode::Regular { root_block_id, } =>
            None,
    };

    loop {
        enum Event<R, B> {
            Request(Option<R>),
            Bootstrap(B),
        }

        let event = if let Some(mut bootstrap_task) = maybe_bootstrap_task.as_mut() {
            select! {
                result = state.fused_request_rx.next() =>
                    Event::Request(result),
                result = bootstrap_task =>
                    Event::Bootstrap(result),
            }
        } else {
            Event::Request(state.fused_request_rx.next().await)
        };

        match event {
            Event::Request(None) => {
                log::info!("requests sink channel depleted: terminating");
                return Ok(());
            },

            Event::Request(Some(Request::Lookup { key, reply_tx, })) =>
                match &state.mode {
                    Mode::CacheBootstrap { cache, } => {
                        let result = if let Some((key, ())) = cache.get_key_value(key.data()) {
                            Found::InCache { kv: key.as_ref().clone(), }
                        } else {
                            Found::Nothing
                        };
                        if let Err(_send_error) = reply_tx.send(Ok(result)) {
                            log::warn!("client canceled lookup request");
                        }
                    },
                    Mode::Regular { root_block_id, } => {

                        unimplemented!()
                    },
                },
        }
    }
}

async fn bootstrap_task(
    cache: Arc<butcher::Cache>,
    blocks_pool: BytesPool,
    mut blockwheel_pid: blockwheel::Pid,
)
    -> Result<block::Id, Error>
{
    let mut block_bytes = blocks_pool.lend();
    let serialize_task = tokio::task::spawn_blocking(move || {
        for (key, &()) in cache.iter() {
            storage::serialize_key_value(&key, storage::JumpRef::None, &mut block_bytes)?;
        }
        Ok(block_bytes)
    });
    let block_bytes = serialize_task.await
        .map_err(Error::BootstrapSerializeBlockJoin)?
        .map_err(Error::BootstrapSerializeBlockStorage)?;

    blockwheel_pid.write_block(block_bytes.freeze()).await
        .map_err(Error::BootstrapWriteBlock)
}
