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

    pub async fn run(
        self,
        parent_supervisor: SupervisorPid,
        blocks_pool: BytesPool,
        lookup_requests_queue_pool: pool::Pool<task::LookupRequestsQueueType>,
        iter_requests_queue_pool: pool::Pool<task::IterRequestsQueueType>,
        outcomes_pool: pool::Pool<Vec<task::SearchOutcome>>,
        iters_pool: pool::Pool<Vec<SearchTreeIterTx>>,
        wheels_pid: wheels::Pid,
        params: Params,
        mode: Mode,
    )
    {
        run(State {
            fused_request_rx: self.fused_request_rx,
            parent_supervisor,
            blocks_pool,
            lookup_requests_queue_pool,
            iter_requests_queue_pool,
            outcomes_pool,
            iters_pool,
            wheels_pid,
            params,
            mode,
        }).await
    }
}

#[derive(Debug)]
pub enum LookupError {
    GenServer(ero::NoProcError),
}

pub struct SearchTreeIterTx {
    pub items_tx: mpsc::Sender<kv::KeyValuePair>,
}

pub struct SearchTreeIterRx {
    pub items_rx: mpsc::Receiver<kv::KeyValuePair>,
}

#[derive(Debug)]
pub enum IterError {
    GenServer(ero::NoProcError),
}

impl Pid {
    pub async fn lookup(&mut self, key: kv::Key) -> Result<Option<kv::ValueCell>, LookupError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(Request::Lookup(task::LookupRequest {
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

    pub async fn iter(&mut self) -> Result<SearchTreeIterRx, IterError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx.send(Request::Iter { reply_tx, }).await
                .map_err(|_send_error| IterError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(iter) =>
                    return Ok(iter),
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
    lookup_requests_queue_pool: pool::Pool<task::LookupRequestsQueueType>,
    iter_requests_queue_pool: pool::Pool<task::IterRequestsQueueType>,
    outcomes_pool: pool::Pool<Vec<task::SearchOutcome>>,
    iters_pool: pool::Pool<Vec<SearchTreeIterTx>>,
    wheels_pid: wheels::Pid,
    params: Params,
    mode: Mode,
}

pub enum Mode {
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
    Lookup(task::LookupRequest),
    Iter { reply_tx: oneshot::Sender<SearchTreeIterRx>, },
}

#[derive(Debug)]
enum Error {
    Task(task::Error),
}

async fn busyloop(_child_supervisor_pid: SupervisorPid, mut state: State) -> Result<(), ErrorSeverity<State, Error>> {
    let mut async_tree = AsyncTree::new();
    let mut tasks = FuturesUnordered::new();

    let (iter_rec_tx, mut iter_rec_rx) = mpsc::channel(0);

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
        enum Event<R, T, I> {
            Request(Option<R>),
            Task(T),
            IterRec(I),
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
                result = iter_rec_rx.next() => match result {
                    None =>
                        unreachable!(),
                    Some(iter_rec_request) =>
                        Event::IterRec(iter_rec_request),
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
                        let result = cache.get(&**lookup_request.key.key_bytes)
                            .cloned();
                        if let Err(_send_error) = lookup_request.reply_tx.send(Ok(result)) {
                            log::warn!("client canceled lookup request");
                        }
                    },
                    Mode::Regular { root_block, } => {
                        let maybe_task_args = async_tree.apply_lookup_request(
                            lookup_request,
                            root_block.clone(),
                            &state.blocks_pool,
                            &state.lookup_requests_queue_pool,
                            &state.iter_requests_queue_pool,
                            &state.outcomes_pool,
                            &state.wheels_pid,
                        );
                        if let Some(task_args) = maybe_task_args {
                            tasks.push(task::run_args(task_args));
                        }
                    },
                },

            Event::Request(Some(Request::Iter { reply_tx, })) =>
                match &state.mode {
                    Mode::CacheBootstrap { cache, } => {
                        tasks.push(task::run_args(task::TaskArgs::IterCache(
                            task::iter_cache::Args {
                                cache: cache.clone(),
                                reply_tx,
                            },
                        )))
                    },
                    Mode::Regular { root_block, } => {
                        let maybe_task_args = async_tree.apply_iter_request(
                            task::IterRequest { block_ref: root_block.clone(), reply_tx, },
                            &state.blocks_pool,
                            &state.lookup_requests_queue_pool,
                            &state.iter_requests_queue_pool,
                            &state.iters_pool,
                            &state.wheels_pid,
                            &iter_rec_tx,
                        );
                        if let Some(task_args) = maybe_task_args {
                            tasks.push(task::run_args(task_args));
                        }
                    }
                },

            Event::IterRec(iter_request) => {
                match &state.mode {
                    Mode::CacheBootstrap { .. } =>
                        unreachable!(),
                    Mode::Regular { .. } =>
                        (),
                }
                let block_ref = iter_request.block_ref.clone();
                let maybe_task_args = async_tree.apply_iter_request(
                    iter_request,
                    &state.blocks_pool,
                    &state.lookup_requests_queue_pool,
                    &state.iter_requests_queue_pool,
                    &state.iters_pool,
                    &state.wheels_pid,
                    &iter_rec_tx,
                );
                if let Some(task_args) = maybe_task_args {
                    tasks.push(task::run_args(task_args));
                }
                if let Some(AsyncBlock { parent: Some(parent_block_ref), .. }) = async_tree.get(&block_ref) {
                    let parent_block_ref = parent_block_ref.clone();
                    async_tree.inc_reqs(&parent_block_ref, 1);
                }
            },

            Event::Task(Ok(task::TaskDone::Bootstrap(task::bootstrap::Done { block_ref: root_block, }))) =>
                match mem::replace(&mut state.mode, Mode::Regular { root_block, }) {
                    Mode::CacheBootstrap { .. } =>
                        (),
                    Mode::Regular { .. } =>
                        unreachable!(),
                },

            Event::Task(Ok(task::TaskDone::LoadBlock(task::load_block::Done { block_ref, block_bytes, }))) =>
                match async_tree.remove(&block_ref) {
                    Some(AsyncBlock { parent, state: AsyncBlockState::Awaiting { lookup_requests_queue, iter_requests_queue, }, }) => {
                        let search_barrier = if lookup_requests_queue.is_empty() {
                            SearchBarrier::Opened
                        } else {
                            let mut lookup_requests_queue =
                                state.lookup_requests_queue_pool.lend(task::LookupRequestsQueueType::new);
                            lookup_requests_queue.clear();
                            SearchBarrier::InProgress { lookup_requests_queue, }
                        };
                        async_tree.insert(block_ref.clone(), AsyncBlock {
                            parent,
                            state: AsyncBlockState::Ready {
                                block_bytes: block_bytes.clone(),
                                reqs: lookup_requests_queue.len() + iter_requests_queue.len(),
                                search_barrier,
                            },
                        });
                        if !lookup_requests_queue.is_empty() {
                            let mut outcomes = state.outcomes_pool.lend(Vec::new);
                            outcomes.clear();
                            tasks.push(
                                task::run_args(
                                    task::TaskArgs::SearchBlock(task::search_block::Args {
                                        block_ref: block_ref.clone(),
                                        blocks_pool: state.blocks_pool.clone(),
                                        block_bytes: block_bytes.clone(),
                                        lookup_requests_queue,
                                        outcomes,
                                    }),
                                ),
                            );
                        }
                        if !iter_requests_queue.is_empty() {
                            let mut iters_tx = state.iters_pool.lend(Vec::new);
                            iters_tx.clear();
                            tasks.push(
                                task::run_args(
                                    task::TaskArgs::IterBlock(task::iter_block::Args {
                                        block_ref,
                                        blocks_pool: state.blocks_pool.clone(),
                                        block_bytes: block_bytes,
                                        iters_tx,
                                        iter_rec_tx: iter_rec_tx.clone(),
                                        iter_requests_queue,
                                    }),
                                ),
                            );
                        }
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
                            let found = Some(value_cell);
                            if let Err(_send_error) = lookup_request.reply_tx.send(Ok(found)) {
                                log::warn!("client canceled lookup request");
                            }
                            dones_count += 1;
                        },
                        task::Outcome::NotFound => {
                            if let Err(_send_error) = lookup_request.reply_tx.send(Ok(None)) {
                                log::warn!("client canceled lookup request");
                            }
                            dones_count += 1;
                        },
                        task::Outcome::Jump { block_ref: jump_block_ref, } => {
                            let maybe_task_args = async_tree.apply_lookup_request(
                                lookup_request,
                                jump_block_ref,
                                &state.blocks_pool,
                                &state.lookup_requests_queue_pool,
                                &state.iter_requests_queue_pool,
                                &state.outcomes_pool,
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
                    &state.lookup_requests_queue_pool,
                    &state.outcomes_pool,
                );
                if let Some(task_args) = maybe_task_args {
                    tasks.push(task::run_args(task_args));
                }
                async_tree.inc_reqs(&block_ref, jumps_count);
                async_tree.dec_reqs(&block_ref, dones_count, true);
            },

            Event::Task(Ok(task::TaskDone::IterCache(task::iter_cache::Done))) =>
                (),

            Event::Task(Ok(task::TaskDone::IterBlock(task::iter_block::Done { block_ref, }))) => {
                async_tree.dec_reqs(&block_ref, 1, false);
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
        lookup_requests_queue: task::LookupRequestsQueue,
        iter_requests_queue: task::IterRequestsQueue,
    },
    Ready {
        block_bytes: Bytes,
        reqs: usize,
        search_barrier: SearchBarrier,
    },
}

enum SearchBarrier {
    Opened,
    InProgress { lookup_requests_queue: task::LookupRequestsQueue, },
}

impl AsyncTree {
    fn new() -> AsyncTree {
        AsyncTree {
            tree: HashMap::new(),
        }
    }

    fn apply_lookup_request(
        &mut self,
        lookup_request: task::LookupRequest,
        block_ref: BlockRef,
        blocks_pool: &BytesPool,
        lookup_requests_queue_pool: &pool::Pool<task::LookupRequestsQueueType>,
        iter_requests_queue_pool: &pool::Pool<task::IterRequestsQueueType>,
        outcomes_pool: &pool::Pool<Vec<task::SearchOutcome>>,
        wheels_pid: &wheels::Pid,
    )
        -> Option<task::TaskArgs>
    {
        match self.tree.entry(block_ref.clone()) {
            hash_map::Entry::Occupied(mut oe) =>
                match oe.get_mut() {
                    AsyncBlock { state: AsyncBlockState::Awaiting { lookup_requests_queue, .. }, .. } => {
                        lookup_requests_queue.push(lookup_request);
                        None
                    },
                    AsyncBlock {
                        state: AsyncBlockState::Ready {
                            reqs,
                            search_barrier: SearchBarrier::InProgress { lookup_requests_queue, },
                            ..
                        },
                        ..
                    } => {
                        lookup_requests_queue.push(lookup_request);
                        *reqs += 1;
                        None
                    },
                    AsyncBlock {
                        state: AsyncBlockState::Ready { reqs, block_bytes, search_barrier: search_barrier @ SearchBarrier::Opened, .. },
                        ..
                    } => {
                        let mut lookup_requests_queue =
                            lookup_requests_queue_pool.lend(task::LookupRequestsQueueType::new);
                        lookup_requests_queue.clear();
                        *search_barrier = SearchBarrier::InProgress { lookup_requests_queue, };
                        let mut lookup_requests_queue =
                            lookup_requests_queue_pool.lend(task::LookupRequestsQueueType::new);
                        lookup_requests_queue.clear();
                        lookup_requests_queue.push(lookup_request);
                        let mut outcomes = outcomes_pool.lend(Vec::new);
                        outcomes.clear();
                        *reqs += 1;
                        Some(task::TaskArgs::SearchBlock(task::search_block::Args {
                            block_ref,
                            blocks_pool: blocks_pool.clone(),
                            block_bytes: block_bytes.clone(),
                            lookup_requests_queue,
                            outcomes,
                        }))
                    },
                },

            hash_map::Entry::Vacant(ve) => {
                let mut lookup_requests_queue =
                    lookup_requests_queue_pool.lend(task::LookupRequestsQueueType::new);
                lookup_requests_queue.clear();
                lookup_requests_queue.push(lookup_request);
                let mut iter_requests_queue =
                    iter_requests_queue_pool.lend(task::IterRequestsQueueType::new);
                iter_requests_queue.clear();
                ve.insert(AsyncBlock {
                    parent: None,
                    state: AsyncBlockState::Awaiting {
                        lookup_requests_queue,
                        iter_requests_queue,
                    },
                });
                Some(task::TaskArgs::LoadBlock(task::load_block::Args {
                    block_ref,
                    wheels_pid: wheels_pid.clone(),
                }))
            },
        }
    }

    fn apply_iter_request(
        &mut self,
        iter_request: task::IterRequest,
        blocks_pool: &BytesPool,
        lookup_requests_queue_pool: &pool::Pool<task::LookupRequestsQueueType>,
        iter_requests_queue_pool: &pool::Pool<task::IterRequestsQueueType>,
        iters_pool: &pool::Pool<Vec<SearchTreeIterTx>>,
        wheels_pid: &wheels::Pid,
        iter_rec_tx: &mpsc::Sender<task::IterRequest>,
    )
        -> Option<task::TaskArgs>
    {
        let block_ref = iter_request.block_ref.clone();
        match self.tree.entry(block_ref.clone()) {
            hash_map::Entry::Occupied(mut oe) =>
                match oe.get_mut() {
                    AsyncBlock { state: AsyncBlockState::Awaiting { iter_requests_queue, .. }, .. } => {
                        iter_requests_queue.push(iter_request);
                        None
                    },
                    AsyncBlock { state: AsyncBlockState::Ready { reqs, block_bytes, .. }, .. } => {
                        let mut iter_requests_queue =
                            iter_requests_queue_pool.lend(task::IterRequestsQueueType::new);
                        iter_requests_queue.clear();
                        iter_requests_queue.push(iter_request);
                        let mut iters_tx = iters_pool.lend(Vec::new);
                        iters_tx.clear();
                        *reqs += 1;
                        Some(task::TaskArgs::IterBlock(task::iter_block::Args {
                            block_ref,
                            blocks_pool: blocks_pool.clone(),
                            block_bytes: block_bytes.clone(),
                            iters_tx,
                            iter_rec_tx: iter_rec_tx.clone(),
                            iter_requests_queue,
                        }))
                    },
                },

            hash_map::Entry::Vacant(ve) => {
                let mut iter_requests_queue =
                    iter_requests_queue_pool.lend(task::IterRequestsQueueType::new);
                iter_requests_queue.clear();
                iter_requests_queue.push(iter_request);
                let mut lookup_requests_queue =
                    lookup_requests_queue_pool.lend(task::LookupRequestsQueueType::new);
                lookup_requests_queue.clear();
                ve.insert(AsyncBlock {
                    parent: None,
                    state: AsyncBlockState::Awaiting {
                        lookup_requests_queue,
                        iter_requests_queue,
                    },
                });
                Some(task::TaskArgs::LoadBlock(task::load_block::Args {
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
        lookup_requests_queue_pool: &pool::Pool<task::LookupRequestsQueueType>,
        outcomes_pool: &pool::Pool<Vec<task::SearchOutcome>>,
    )
        -> Option<task::TaskArgs>
    {
        match self.tree.get_mut(block_ref) {
            None | Some(AsyncBlock { state: AsyncBlockState::Awaiting { .. }, .. }) =>
                unreachable!(),
            Some(AsyncBlock { state: AsyncBlockState::Ready { block_bytes, search_barrier, .. }, .. }) =>
                match mem::replace(search_barrier, SearchBarrier::Opened) {
                    SearchBarrier::Opened =>
                        unreachable!(),
                    SearchBarrier::InProgress { lookup_requests_queue, } if lookup_requests_queue.is_empty() =>
                        None,
                    SearchBarrier::InProgress { lookup_requests_queue, } => {
                        *search_barrier = SearchBarrier::InProgress {
                            lookup_requests_queue: lookup_requests_queue_pool.lend(task::LookupRequestsQueueType::new),
                        };
                        Some(task::TaskArgs::SearchBlock(task::search_block::Args {
                            block_ref: block_ref.clone(),
                            blocks_pool: blocks_pool.clone(),
                            block_bytes: block_bytes.clone(),
                            lookup_requests_queue,
                            outcomes: outcomes_pool.lend(Vec::new),
                        }))
                    },
                },
        }
    }

    fn inc_reqs(&mut self, block_ref: &BlockRef, inc: usize) {
        match self.tree.get_mut(&block_ref) {
            None | Some(AsyncBlock { state: AsyncBlockState::Awaiting { .. }, .. }) =>
                unreachable!(),
            Some(AsyncBlock { state: AsyncBlockState::Ready { reqs, .. }, ..}) =>
                *reqs += inc,
        }
    }

    fn dec_reqs(&mut self, block_ref: &BlockRef, dec: usize, is_recursive: bool) {
        let mut maybe_block_ref = Some(block_ref.clone());
        while let Some(block_ref) = maybe_block_ref {
            let (parent, reqs) = match self.tree.get_mut(&block_ref).unwrap() {
                AsyncBlock { state: AsyncBlockState::Awaiting { .. }, .. } =>
                    unreachable!(),
                AsyncBlock { parent, state: AsyncBlockState::Ready { reqs, .. }, .. } =>
                    (parent, reqs),
            };
            assert!(*reqs >= dec);
            *reqs -= dec;
            maybe_block_ref = parent.clone();
            if *reqs == 0 {
                self.tree.remove(&block_ref);
            }
            if !is_recursive {
                break;
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
