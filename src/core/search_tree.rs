use std::{
    mem,
    sync::Arc,
    time::Duration,
    collections::HashMap,
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

use alloc_pool::bytes::{
    Bytes,
    BytesPool,
};

use crate::{
    kv,
    wheels,
    storage,
    blockwheel::{
        self,
        block,
    },
    core::{
        butcher,
        BlockRef,
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
        wheels_pid: wheels::Pid,
        params: Params,
        cache: Arc<butcher::Cache>,
    )
    {
        run(State {
            fused_request_rx: self.fused_request_rx,
            parent_supervisor,
            blocks_pool,
            wheels_pid,
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
                .send(Request::Lookup(Lookup {
                    key: key.clone(),
                    reply_tx,
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
    parent_supervisor: SupervisorPid,
    blocks_pool: BytesPool,
    wheels_pid: wheels::Pid,
    params: Params,
    mode: Mode,
}

enum Mode {
    CacheBootstrap {
        cache: Arc<butcher::Cache>,
    },
    Regular {
        root_block: BlockRef,
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
    Lookup(Lookup),
}

struct Lookup {
    key: kv::Key,
    reply_tx: oneshot::Sender<Result<Found, SearchTreeLookupError>>,
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
    let mut tree_view: HashMap<BlockRef, Bytes> = HashMap::new();
    let mut tasks = FuturesUnordered::new();

    match &state.mode {
        Mode::CacheBootstrap { cache, } =>
            tasks.push(
                run_task_args(
                    TaskArgs::Bootstrap {
                        cache: cache.clone(),
                        blocks_pool: state.blocks_pool.clone(),
                        wheels_pid: state.wheels_pid.clone(),
                    },
                ),
            ),
        Mode::Regular { .. } =>
            (),
    };

    loop {
        enum Event<R, T> {
            Request(Option<R>),
            Task(T),
        }

        let event = if tasks.is_empty() {
            Event::Request(state.fused_request_rx.next().await)
        } else {
            select! {
                result = state.fused_request_rx.next() =>
                    Event::Request(result),
                result = tasks.next() => match result {
                    None =>
                        unreachable!(),
                    Some(task) =>
                        Event::Task(task),
                },
            }
        };

        match event {
            Event::Request(None) => {
                log::info!("requests sink channel depleted: terminating");
                return Ok(());
            },

            Event::Request(Some(Request::Lookup(lookup_request))) =>
                match &state.mode {
                    Mode::CacheBootstrap { cache, } => {
                        let result = if let Some((key, ())) = cache.get_key_value(lookup_request.key.data()) {
                            Found::InCache { kv: key.as_ref().clone(), }
                        } else {
                            Found::Nothing
                        };
                        if let Err(_send_error) = lookup_request.reply_tx.send(Ok(result)) {
                            log::warn!("client canceled lookup request");
                        }
                    },
                    Mode::Regular { root_block: _, } => {

                        unimplemented!()
                    },
                },

            Event::Task(TaskDone::Bootstrap(Ok(root_block))) =>
                match mem::replace(&mut state.mode, Mode::Regular { root_block, }) {
                    Mode::CacheBootstrap { .. } =>
                        (),
                    Mode::Regular { .. } =>
                        unreachable!(),
                },

            Event::Task(TaskDone::Bootstrap(Err(error))) =>
                return Err(ErrorSeverity::Fatal(error)),
        }
    }
}

enum TaskArgs {
    Bootstrap {
        cache: Arc<butcher::Cache>,
        blocks_pool: BytesPool,
        wheels_pid: wheels::Pid,
    },
}

enum TaskDone {
    Bootstrap(Result<block::Id, Error>),
}

async fn run_task_args(args: TaskArgs) -> TaskDone {
    match args {
        TaskArgs::Bootstrap { cache, blocks_pool, blockwheel_pid, } =>
            TaskDone::Bootstrap(
                task_bootstrap(cache, blocks_pool, blockwheel_pid).await,
            ),
    }
}

async fn task_bootstrap(
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
