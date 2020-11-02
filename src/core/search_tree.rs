use std::{
    mem,
    sync::Arc,
    cmp::Reverse,
    time::Duration,
    collections::{
        hash_map,
        HashMap,
        BinaryHeap,
    },
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
};

use ero::{
    restart,
    ErrorSeverity,
    RestartStrategy,
    supervisor::SupervisorPid,
};

use alloc_pool::{
    pool,
    bytes::{
        Bytes,
        BytesPool,
    },
};

use crate::{
    kv::{
        self,
        ContainsKey,
    },
    wheels,
    blockwheel::{
        block,
    },
    core::{
        OrdKey,
        BlockRef,
        MemCache,
    },
};

mod task;

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
        cache: Arc<MemCache>,
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
                .send(Request::Lookup(task::Lookup {
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
        cache: Arc<MemCache>,
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
    Lookup(task::Lookup),
}

#[derive(Debug)]
enum Error {
    Task(task::Error),
}

async fn busyloop(_child_supervisor_pid: SupervisorPid, mut state: State) -> Result<(), ErrorSeverity<State, Error>> {
    let mut async_tree = AsyncTree::new();
    let mut tasks = FuturesUnordered::new();
    let mut requests_queue_pool = pool::Pool::new();
    let mut outcomes_pool = pool::Pool::new();

    match &state.mode {
        Mode::CacheBootstrap { cache, } =>
            tasks.push(
                task::run_args(
                    task::TaskArgs::Bootstrap(task::bootstrap::Args {
                        cache: cache.clone(),
                        blocks_pool: state.blocks_pool.clone(),
                        wheels_pid: state.wheels_pid.clone(),
                    }),
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
                        let result = if let Some((key, ())) = cache.get_key_value(lookup_request.key.key_data()) {
                            Found::InCache { kv: key.as_ref().clone(), }
                        } else {
                            Found::Nothing
                        };
                        if let Err(_send_error) = lookup_request.reply_tx.send(Ok(result)) {
                            log::warn!("client canceled lookup request");
                        }
                    },
                    Mode::Regular { root_block, } =>
                        match async_tree.entry(root_block.clone()) {
                            hash_map::Entry::Occupied(oe) =>
                                unimplemented!(),
                            hash_map::Entry::Vacant(ve) => {
                                let mut requests_queue = requests_queue_pool.lend(BinaryHeap::new);
                                requests_queue.push(Reverse(OrdKey::new(lookup_request)));
                                ve.insert(AsyncBlock {
                                    parent: None,
                                    state: AsyncBlockState::Awaiting { requests_queue, },
                                });
                                tasks.push(
                                    task::run_args(
                                        task::TaskArgs::LoadBlockLookup(task::load_block_lookup::Args {
                                            block_ref: root_block.clone(),
                                            wheels_pid: state.wheels_pid.clone(),
                                        }),
                                    ),
                                );
                            },
                        },
                },

            Event::Task(Ok(task::TaskDone::Bootstrap(task::bootstrap::Done { block_ref: root_block, }))) =>
                match mem::replace(&mut state.mode, Mode::Regular { root_block, }) {
                    Mode::CacheBootstrap { .. } =>
                        (),
                    Mode::Regular { .. } =>
                        unreachable!(),
                },

            Event::Task(Ok(task::TaskDone::LoadBlockLookup(task::load_block_lookup::Done { block_ref, block_bytes, }))) =>
                match async_tree.remove(&block_ref) {
                    Some(AsyncBlock { parent, state: AsyncBlockState::Awaiting { requests_queue, }, }) => {
                        if let Some(ref parent_block_ref) = parent {
                            match async_tree.get_mut(parent_block_ref) {
                                Some(AsyncBlock { state: AsyncBlockState::Ready { activity_refs, .. }, .. }) =>
                                    *activity_refs += 1,
                                None | Some(AsyncBlock { state: AsyncBlockState::Awaiting { .. }, .. }) =>
                                    unreachable!(),
                            }
                        }
                        async_tree.insert(block_ref.clone(), AsyncBlock {
                            parent,
                            state: AsyncBlockState::Ready {
                                block_bytes: block_bytes.clone(),
                                activity_refs: 1,
                                barrier: Barrier::SearchInProgress {
                                    requests_queue: requests_queue_pool.lend(BinaryHeap::new),
                                },
                            },
                        });
                        tasks.push(
                            task::run_args(
                                task::TaskArgs::SearchBlock(task::search_block::Args {
                                    block_ref,
                                    blocks_pool: state.blocks_pool.clone(),
                                    block_bytes,
                                    requests_queue,
                                    outcomes: outcomes_pool.lend(Vec::new),
                                }),
                            ),
                        );
                    },
                    None | Some(AsyncBlock { state: AsyncBlockState::Ready { .. }, .. }) =>
                        unreachable!(),
                },

            Event::Task(Ok(task::TaskDone::SearchBlock(task::search_block::Done { block_ref, outcomes, }))) =>
                unimplemented!(),

            Event::Task(Err(error)) =>
                return Err(ErrorSeverity::Fatal(Error::Task(error))),
        }
    }
}

type AsyncTree = HashMap<BlockRef, AsyncBlock>;

struct AsyncBlock {
    parent: Option<BlockRef>,
    state: AsyncBlockState,
}

enum AsyncBlockState {
    Awaiting {
        requests_queue: task::RequestsQueue,
    },
    Ready {
        block_bytes: Bytes,
        activity_refs: usize,
        barrier: Barrier,
    },
}

enum Barrier {
    Opened,
    SearchInProgress { requests_queue: task::RequestsQueue, },
}
