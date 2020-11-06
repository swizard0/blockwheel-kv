use std::{
    mem,
    sync::Arc,
    ops::{
        Deref,
        DerefMut,
    },
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
    kv,
    wheels,
    core::{
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
    Something {
        value_cell: kv::ValueCell,
        location: FoundLocation,
    },
}

#[derive(Clone, Debug)]
pub enum FoundLocation {
    Cache,
    Block { block_ref: BlockRef, },
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
    let requests_queue_pool = pool::Pool::new();
    let outcomes_pool = pool::Pool::new();

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
                        let result = if let Some(value_cell) = cache.get(&**lookup_request.key.key_bytes).cloned() {
                            Found::Something { value_cell, location: FoundLocation::Cache, }
                        } else {
                            Found::Nothing
                        };
                        if let Err(_send_error) = lookup_request.reply_tx.send(Ok(result)) {
                            log::warn!("client canceled lookup request");
                        }
                    },
                    Mode::Regular { root_block, } => {
                        let maybe_task_args = async_tree.apply_lookup_request(
                            lookup_request,
                            root_block.clone(),
                            &state.blocks_pool,
                            &requests_queue_pool,
                            &outcomes_pool,
                            &state.wheels_pid,
                        );
                        if let Some(task_args) = maybe_task_args {
                            tasks.push(task::run_args(task_args));
                        }
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
                        async_tree.insert(block_ref.clone(), AsyncBlock {
                            parent,
                            state: AsyncBlockState::Ready {
                                block_bytes: block_bytes.clone(),
                                children_reqs: 0,
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

            Event::Task(Ok(task::TaskDone::SearchBlock(task::search_block::Done { block_ref, mut outcomes, }))) => {
                let mut dones_count = 0;
                let mut jumps_count = 0;
                for task::SearchOutcome { request: lookup_request, outcome, } in outcomes.drain(..) {
                    match outcome {
                        task::Outcome::Found { value_cell, } => {
                            let found = Found::Something {
                                value_cell,
                                location: FoundLocation::Block { block_ref: block_ref.clone(), },
                            };
                            if let Err(_send_error) = lookup_request.reply_tx.send(Ok(found)) {
                                log::warn!("client canceled lookup request");
                            }
                            dones_count += 1;
                        },
                        task::Outcome::NotFound => {
                            if let Err(_send_error) = lookup_request.reply_tx.send(Ok(Found::Nothing)) {
                                log::warn!("client canceled lookup request");
                            }
                            dones_count += 1;
                        },
                        task::Outcome::Jump { block_ref: jump_block_ref, } => {
                            let maybe_task_args = async_tree.apply_lookup_request(
                                lookup_request,
                                jump_block_ref,
                                &state.blocks_pool,
                                &requests_queue_pool,
                                &outcomes_pool,
                                &state.wheels_pid,
                            );
                            if let Some(task_args) = maybe_task_args {
                                tasks.push(task::run_args(task_args));
                            }
                            jumps_count += 1;
                        },
                    }
                }
                let maybe_task_args = async_tree.open_barrier(
                    &block_ref,
                    &state.blocks_pool,
                    &requests_queue_pool,
                    &outcomes_pool,
                );
                if let Some(task_args) = maybe_task_args {
                    tasks.push(task::run_args(task_args));
                }
                async_tree.inc_children_reqs(&block_ref, jumps_count);
                async_tree.dec_parent_reqs(&block_ref, dones_count);
            },

            Event::Task(Err(error)) =>
                return Err(ErrorSeverity::Fatal(Error::Task(error))),
        }
    }
}

struct AsyncTree {
    tree: HashMap<BlockRef, AsyncBlock>,
}

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
        children_reqs: usize,
        barrier: Barrier,
    },
}

enum Barrier {
    Opened,
    SearchInProgress { requests_queue: task::RequestsQueue, },
}

impl AsyncTree {
    fn new() -> AsyncTree {
        AsyncTree {
            tree: HashMap::new(),
        }
    }

    fn apply_lookup_request(
        &mut self,
        lookup_request: task::Lookup,
        block_ref: BlockRef,
        blocks_pool: &BytesPool,
        requests_queue_pool: &pool::Pool<task::RequestsQueueType>,
        outcomes_pool: &pool::Pool<Vec<task::SearchOutcome>>,
        wheels_pid: &wheels::Pid,
    )
        -> Option<task::TaskArgs>
    {
        match self.tree.entry(block_ref.clone()) {
            hash_map::Entry::Occupied(mut oe) =>
                match oe.get_mut() {
                    AsyncBlock { state: AsyncBlockState::Awaiting { requests_queue, }, .. } |
                    AsyncBlock { state: AsyncBlockState::Ready { barrier: Barrier::SearchInProgress { requests_queue, }, .. }, .. } => {
                        requests_queue.push(lookup_request);
                        None
                    },
                    AsyncBlock { state: AsyncBlockState::Ready { block_bytes, barrier: barrier @ Barrier::Opened, .. }, .. } => {
                        *barrier = Barrier::SearchInProgress { requests_queue: requests_queue_pool.lend(BinaryHeap::new), };
                        let mut requests_queue = requests_queue_pool.lend(BinaryHeap::new);
                        requests_queue.push(lookup_request);
                        Some(task::TaskArgs::SearchBlock(task::search_block::Args {
                            block_ref,
                            blocks_pool: blocks_pool.clone(),
                            block_bytes: block_bytes.clone(),
                            requests_queue,
                            outcomes: outcomes_pool.lend(Vec::new),
                        }))
                    },
                },

            hash_map::Entry::Vacant(ve) => {
                let mut requests_queue = requests_queue_pool.lend(BinaryHeap::new);
                requests_queue.push(lookup_request);
                ve.insert(AsyncBlock {
                    parent: None,
                    state: AsyncBlockState::Awaiting { requests_queue, },
                });
                Some(task::TaskArgs::LoadBlockLookup(task::load_block_lookup::Args {
                    block_ref,
                    wheels_pid: wheels_pid.clone(),
                }))
            },
        }
    }

    fn open_barrier(
        &mut self,
        block_ref: &BlockRef,
        blocks_pool: &BytesPool,
        requests_queue_pool: &pool::Pool<task::RequestsQueueType>,
        outcomes_pool: &pool::Pool<Vec<task::SearchOutcome>>,
    )
        -> Option<task::TaskArgs>
    {
        match self.tree.get_mut(block_ref) {
            None | Some(AsyncBlock { state: AsyncBlockState::Awaiting { .. }, .. }) =>
                unreachable!(),
            Some(AsyncBlock { state: AsyncBlockState::Ready { block_bytes, barrier, .. }, .. }) =>
                match mem::replace(barrier, Barrier::Opened) {
                    Barrier::Opened =>
                        unreachable!(),
                    Barrier::SearchInProgress { requests_queue, } if requests_queue.is_empty() =>
                        None,
                    Barrier::SearchInProgress { requests_queue, } => {
                        *barrier = Barrier::SearchInProgress {
                            requests_queue: requests_queue_pool.lend(BinaryHeap::new),
                        };
                        Some(task::TaskArgs::SearchBlock(task::search_block::Args {
                            block_ref: block_ref.clone(),
                            blocks_pool: blocks_pool.clone(),
                            block_bytes: block_bytes.clone(),
                            requests_queue,
                            outcomes: outcomes_pool.lend(Vec::new),
                        }))
                    },
                },
        }
    }

    fn inc_children_reqs(&mut self, block_ref: &BlockRef, inc: usize) {
        match self.tree.get_mut(&block_ref) {
            None | Some(AsyncBlock { state: AsyncBlockState::Awaiting { .. }, .. }) =>
                unreachable!(),
            Some(AsyncBlock { state: AsyncBlockState::Ready { children_reqs, .. }, ..}) =>
                *children_reqs += inc,
        }
    }

    fn dec_parent_reqs(&mut self, block_ref: &BlockRef, dec: usize) {
        let mut maybe_dec = None;
        let mut maybe_block_ref = Some(block_ref.clone());
        while let Some(block_ref) = maybe_block_ref {
            let (parent, children_reqs, barrier) = match self.tree.get_mut(&block_ref).unwrap() {
                AsyncBlock { state: AsyncBlockState::Awaiting { .. }, .. } =>
                    unreachable!(),
                AsyncBlock { parent, state: AsyncBlockState::Ready { children_reqs, barrier, .. }, .. } =>
                    (parent, children_reqs, barrier),
            };
            if let Some(dec) = maybe_dec {
                assert!(*children_reqs >= dec);
                *children_reqs -= dec;
            }
            maybe_block_ref = parent.clone();
            maybe_dec = Some(dec);

            let is_trash_block = match barrier {
                Barrier::Opened =>
                    *children_reqs == 0,
                Barrier::SearchInProgress { .. } =>
                    false,
            };
            if is_trash_block {
                self.tree.remove(&block_ref);
            }
        }
    }
}

impl Deref for AsyncTree {
    type Target = HashMap<BlockRef, AsyncBlock>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.tree
    }
}

impl DerefMut for AsyncTree {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tree
    }
}
