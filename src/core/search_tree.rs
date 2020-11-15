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
        SearchRangeBounds,
    },
    Info,
    Flushed,
    KeyValueStreamItem,
};

mod task;

#[derive(Clone, Debug)]
pub struct Params {
    pub task_restart_sec: usize,
    pub tree_block_size: usize,
    pub remove_tasks_limit: usize,
    pub iter_send_buffer: usize,
}

impl Default for Params {
    fn default() -> Params {
        Params {
            task_restart_sec: 1,
            tree_block_size: 32,
            remove_tasks_limit: 64,
            iter_send_buffer: 4,
        }
    }
}

#[derive(Clone)]
pub struct Pools {
    blocks_pool: BytesPool,
    lookup_requests_queue_pool: pool::Pool<task::LookupRequestsQueueType>,
    iter_requests_queue_pool: pool::Pool<task::IterRequestsQueueType>,
    outcomes_pool: pool::Pool<Vec<task::SearchOutcome>>,
    iters_pool: pool::Pool<task::SearchTreeIterSinks>,
}

impl Pools {
    pub fn new(blocks_pool: BytesPool) -> Pools {
        Pools {
            blocks_pool,
            lookup_requests_queue_pool: pool::Pool::new(),
            iter_requests_queue_pool: pool::Pool::new(),
            outcomes_pool: pool::Pool::new(),
            iters_pool: pool::Pool::new(),
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
        pools: Pools,
        wheels_pid: wheels::Pid,
        params: Params,
        mode: Mode,
    )
    {
        run(State {
            fused_request_rx: self.fused_request_rx,
            parent_supervisor,
            pools,
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

#[derive(Debug)]
pub enum FlushError {
    GenServer(ero::NoProcError),
}

#[derive(Debug)]
pub enum IterError {
    GenServer(ero::NoProcError),
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Demolished;

#[derive(Debug)]
pub enum DemolishError {
    GenServer(ero::NoProcError),
}

pub struct SearchTreeIterItemsTx {
    pub range: SearchRangeBounds,
    pub items_tx: mpsc::Sender<KeyValueStreamItem>,
}

pub struct SearchTreeIterItemsRx {
    pub items_rx: mpsc::Receiver<KeyValueStreamItem>,
}

pub struct SearchTreeIterBlockRefsTx {
    pub block_refs_tx: mpsc::Sender<BlockRef>,
}

pub struct SearchTreeIterBlockRefsRx {
    pub block_refs_rx: mpsc::Receiver<BlockRef>,
}

impl Pid {
    pub async fn info(&mut self) -> Result<Info, ero::NoProcError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx.send(Request::Info { reply_tx, }).await
                .map_err(|_send_error| ero::NoProcError)?;
            match reply_rx.await {
                Ok(info) =>
                    return Ok(info),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

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

    pub async fn iter(&mut self) -> Result<SearchTreeIterItemsRx, IterError> {
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

    pub async fn flush(&mut self) -> Result<Flushed, FlushError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx.send(Request::Flush { reply_tx, }).await
                .map_err(|_send_error| FlushError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(Flushed) =>
                    return Ok(Flushed),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn demolish(&mut self) -> Result<Demolished, DemolishError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx.send(Request::Demolish { reply_tx, }).await
                .map_err(|_send_error| DemolishError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(Demolished) =>
                    return Ok(Demolished),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }
}

struct State {
    fused_request_rx: stream::Fuse<mpsc::Receiver<Request>>,
    parent_supervisor: SupervisorPid,
    pools: Pools,
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
    } else {
        log::debug!("search tree terminated with result = {:?}", terminate_result);
    }
}

enum Request {
    Info { reply_tx: oneshot::Sender<Info>, },
    Lookup(task::LookupRequest),
    Iter { range: SearchRangeBounds, reply_tx: oneshot::Sender<SearchTreeIterItemsRx>, },
    Flush { reply_tx: oneshot::Sender<Flushed>, },
    Demolish { reply_tx: oneshot::Sender<Demolished>, },
}

#[derive(Debug)]
enum Error {
    Task(task::Error),
}

async fn busyloop(_child_supervisor_pid: SupervisorPid, mut state: State) -> Result<(), ErrorSeverity<State, Error>> {
    let mut async_tree = AsyncTree::new();
    let mut tasks = FuturesUnordered::new();
    let mut tasks_count = 0;

    let (iter_rec_tx, mut iter_rec_rx) = mpsc::channel(0);

    match &state.mode {
        Mode::CacheBootstrap { cache, } => {
            tasks.push(
                task::run_args(
                    task::TaskArgs::Bootstrap(task::bootstrap::Args {
                        cache: cache.clone(),
                        blocks_pool: state.pools.blocks_pool.clone(),
                        wheels_pid: state.wheels_pid.clone(),
                    }),
                ),
            );
            tasks_count += 1;
        },
        Mode::Regular { .. } =>
            (),
    };

    enum FlushMode {
        NoFlush,
        InProgress {
            flush_reply_tx: Option<oneshot::Sender<Flushed>>,
            demolish_reply_tx: Option<oneshot::Sender<Demolished>>,
        },
    };
    let mut flush_mode = FlushMode::NoFlush;

    loop {
        enum Event<R, T, I> {
            Request(Option<R>),
            Task(T),
            IterRec(I),
        }
        let event = match mem::replace(&mut flush_mode, FlushMode::NoFlush) {

            FlushMode::NoFlush if tasks_count == 0 =>
                select! {
                    result = state.fused_request_rx.next() =>
                        Event::Request(result),
                    result = iter_rec_rx.next() => match result {
                        None =>
                            unreachable!(),
                        Some(iter_rec_request) =>
                            Event::IterRec(iter_rec_request),
                    },
                },

            FlushMode::NoFlush =>
                select! {
                    result = state.fused_request_rx.next() =>
                        Event::Request(result),
                    result = tasks.next() => match result {
                        None =>
                            unreachable!(),
                        Some(task) => {
                            tasks_count -= 1;
                            Event::Task(task)
                        },
                    },
                    result = iter_rec_rx.next() => match result {
                        None =>
                            unreachable!(),
                        Some(iter_rec_request) =>
                            Event::IterRec(iter_rec_request),
                    },
                },

            FlushMode::InProgress { flush_reply_tx, demolish_reply_tx, } if tasks_count == 0 => {
                log::debug!("FlushMode::InProgress: all tasks finished");

                if let Some(done_reply_tx) = flush_reply_tx {
                    log::debug!("responding Flushed");
                    if let Err(_send_error) = done_reply_tx.send(Flushed) {
                        log::warn!("client canceled flush request");
                    }
                }

                if let Some(done_reply_tx) = demolish_reply_tx {
                    match &state.mode {
                        Mode::Regular { root_block, } => {
                            log::debug!("starting demolish on root_block = {:?}", root_block);
                            let (reply_tx, reply_rx) = oneshot::channel();
                            let maybe_task_args = async_tree.apply_iter_request(
                                task::IterRequest {
                                    block_ref: root_block.clone(),
                                    kind: task::IterRequestKind::BlockRefs { reply_tx, },
                                },
                                &state.pools.blocks_pool,
                                &state.pools.lookup_requests_queue_pool,
                                &state.pools.iter_requests_queue_pool,
                                &state.pools.iters_pool,
                                &state.wheels_pid,
                                &iter_rec_tx,
                                state.params.iter_send_buffer,
                            );
                            if let Some(task_args) = maybe_task_args {
                                tasks.push(task::run_args(task_args));
                                tasks_count += 1;
                            }
                            tasks.push(task::run_args(task::TaskArgs::Demolish(task::demolish::Args {
                                done_reply_tx,
                                block_refs_rx_reply_rx: reply_rx,
                                wheels_pid: state.wheels_pid.clone(),
                                remove_tasks_limit: state.params.remove_tasks_limit,
                            })));
                            tasks_count += 1;
                        },
                        Mode::CacheBootstrap { .. } =>
                            unreachable!(),
                    }
                }

                continue;
            },

            FlushMode::InProgress { flush_reply_tx, demolish_reply_tx, } => {
                log::debug!("FlushMode::InProgress: {} tasks left", tasks_count);
                flush_mode = FlushMode::InProgress { flush_reply_tx, demolish_reply_tx, };
                select! {
                    result = tasks.next() => match result {
                        None =>
                            unreachable!(),
                        Some(task) => {
                            tasks_count -= 1;
                            Event::Task(task)
                        },
                    },
                    result = iter_rec_rx.next() => match result {
                        None =>
                            unreachable!(),
                        Some(iter_rec_request) =>
                            Event::IterRec(iter_rec_request),
                    },
                }
            },

        };

        match event {
            Event::Request(None) => {
                log::info!("requests sink channel depleted: terminating");
                return Ok(());
            },

            Event::Request(Some(Request::Info { reply_tx, })) => {
                let info = Info::default();
                if let Err(_send_error) = reply_tx.send(info) {
                    log::warn!("client canceled info request");
                }

                unimplemented!()
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
                            &state.pools.lookup_requests_queue_pool,
                            &state.pools.iter_requests_queue_pool,
                            &state.wheels_pid,
                        );
                        if let Some(task_args) = maybe_task_args {
                            tasks.push(task::run_args(task_args));
                            tasks_count += 1;
                        }
                    },
                },

            Event::Request(Some(Request::Iter { range, reply_tx, })) =>
                match &state.mode {
                    Mode::CacheBootstrap { cache, } => {
                        tasks.push(task::run_args(task::TaskArgs::IterCache(
                            task::iter_cache::Args {
                                cache: cache.clone(),
                                range, reply_tx,
                            },
                        )));
                        tasks_count += 1;
                    },
                    Mode::Regular { root_block, } => {
                        let maybe_task_args = async_tree.apply_iter_request(
                            task::IterRequest {
                                block_ref: root_block.clone(),
                                kind: task::IterRequestKind::Items { range, reply_tx, },
                            },
                            &state.pools.blocks_pool,
                            &state.pools.lookup_requests_queue_pool,
                            &state.pools.iter_requests_queue_pool,
                            &state.pools.iters_pool,
                            &state.wheels_pid,
                            &iter_rec_tx,
                            state.params.iter_send_buffer,
                        );
                        if let Some(task_args) = maybe_task_args {
                            tasks.push(task::run_args(task_args));
                            tasks_count += 1;
                        }
                    }
                },

            Event::Request(Some(Request::Flush { reply_tx, })) => {
                log::debug!("Request::Flush received: waiting for {} tasks to finish", tasks_count);
                match &mut flush_mode {
                    FlushMode::NoFlush =>
                        flush_mode = FlushMode::InProgress {
                            flush_reply_tx: Some(reply_tx),
                            demolish_reply_tx: None,
                        },
                    FlushMode::InProgress { flush_reply_tx: Some(..), .. } =>
                        unreachable!(),
                    FlushMode::InProgress { flush_reply_tx: flush_reply_tx @ None, .. } =>
                        *flush_reply_tx = Some(reply_tx),
                }
            },

            Event::Request(Some(Request::Demolish { reply_tx, })) => {
                log::debug!("Request::Demolish received: waiting for {} tasks to finish", tasks_count);
                match &mut flush_mode {
                    FlushMode::NoFlush =>
                        flush_mode = FlushMode::InProgress {
                            flush_reply_tx: None,
                            demolish_reply_tx: Some(reply_tx),
                        },
                    FlushMode::InProgress { demolish_reply_tx: Some(..), .. } =>
                        unreachable!(),
                    FlushMode::InProgress { demolish_reply_tx: demolish_reply_tx @ None, .. } =>
                        *demolish_reply_tx = Some(reply_tx),
                }
            },

            Event::IterRec(iter_request) => {
                match &state.mode {
                    Mode::CacheBootstrap { .. } =>
                        unreachable!(),
                    Mode::Regular { .. } =>
                        (),
                }
                let maybe_task_args = async_tree.apply_iter_request(
                    iter_request,
                    &state.pools.blocks_pool,
                    &state.pools.lookup_requests_queue_pool,
                    &state.pools.iter_requests_queue_pool,
                    &state.pools.iters_pool,
                    &state.wheels_pid,
                    &iter_rec_tx,
                    state.params.iter_send_buffer,
                );
                if let Some(task_args) = maybe_task_args {
                    tasks.push(task::run_args(task_args));
                    tasks_count += 1;
                }
            },

            Event::Task(Ok(task::TaskDone::Bootstrap(task::bootstrap::Done { block_ref: root_block, }))) =>
                match mem::replace(&mut state.mode, Mode::Regular { root_block: root_block.clone(), }) {
                    Mode::CacheBootstrap { .. } =>
                        (),
                    Mode::Regular { .. } =>
                        unreachable!(),
                },

            Event::Task(Ok(task::TaskDone::LoadBlock(task::load_block::Done { block_ref, block_bytes, }))) => {
                let async_block = async_tree.get_mut(&block_ref).unwrap();
                let prev_async_block = mem::replace(async_block, AsyncBlock::Ready {
                    block_bytes: block_bytes.clone(),
                    more_lookup_requests: None,
                });
                match prev_async_block {
                    AsyncBlock::Ready { .. } =>
                        unreachable!(),
                    AsyncBlock::Awaiting { lookup_requests_queue, iter_requests_queue, } => {
                        if !lookup_requests_queue.is_empty() {
                            let mut outcomes = state.pools.outcomes_pool.lend(Vec::new);
                            outcomes.clear();
                            tasks.push(
                                task::run_args(
                                    task::TaskArgs::SearchBlock(task::search_block::Args {
                                        block_ref: block_ref.clone(),
                                        blocks_pool: state.pools.blocks_pool.clone(),
                                        block_bytes: block_bytes.clone(),
                                        lookup_requests_queue,
                                        outcomes,
                                    }),
                                ),
                            );
                            tasks_count += 1;
                        }
                        if !iter_requests_queue.is_empty() {
                            let mut iters_tx = state.pools.iters_pool.lend(task::SearchTreeIterSinks::default);
                            iters_tx.clear();
                            tasks.push(
                                task::run_args(
                                    task::TaskArgs::IterBlock(task::iter_block::Args {
                                        block_ref,
                                        blocks_pool: state.pools.blocks_pool.clone(),
                                        block_bytes: block_bytes,
                                        iters_tx,
                                        iter_rec_tx: iter_rec_tx.clone(),
                                        iter_requests_queue,
                                        iter_send_buffer: state.params.iter_send_buffer,
                                    }),
                                ),
                            );
                            tasks_count += 1;
                        }
                    },
                }
            },

            Event::Task(Ok(task::TaskDone::SearchBlock(task::search_block::Done { block_ref, mut outcomes, }))) => {
                for task::SearchOutcome { request: lookup_request, outcome, } in outcomes.drain(..) {
                    match outcome {
                        task::Outcome::Found { value_cell, } => {
                            let found = Some(value_cell);
                            if let Err(_send_error) = lookup_request.reply_tx.send(Ok(found)) {
                                log::warn!("client canceled lookup request");
                            }
                        },
                        task::Outcome::NotFound => {
                            if let Err(_send_error) = lookup_request.reply_tx.send(Ok(None)) {
                                log::warn!("client canceled lookup request");
                            }
                        },
                        task::Outcome::Jump { block_ref: jump_block_ref, } => {
                            let maybe_task_args = async_tree.apply_lookup_request(
                                lookup_request,
                                jump_block_ref,
                                &state.pools.lookup_requests_queue_pool,
                                &state.pools.iter_requests_queue_pool,
                                &state.wheels_pid,
                            );
                            if let Some(task_args) = maybe_task_args {
                                tasks.push(task::run_args(task_args));
                                tasks_count += 1;
                            }
                        },
                    }
                }
                let maybe_task_args = async_tree.drop_or_search_more(
                    &block_ref,
                    &state.pools.blocks_pool,
                    &state.pools.outcomes_pool,
                );
                if let Some(task_args) = maybe_task_args {
                    tasks.push(task::run_args(task_args));
                    tasks_count += 1;
                }
            },

            Event::Task(Ok(task::TaskDone::IterCache(task::iter_cache::Done))) =>
                (),

            Event::Task(Ok(task::TaskDone::IterBlock(task::iter_block::Done { block_ref, }))) => {
                let maybe_task_args = async_tree.drop_or_search_more(
                    &block_ref,
                    &state.pools.blocks_pool,
                    &state.pools.outcomes_pool,
                );
                if let Some(task_args) = maybe_task_args {
                    tasks.push(task::run_args(task_args));
                    tasks_count += 1;
                }
            },

            Event::Task(Ok(task::TaskDone::Demolish(task::demolish::Done { blocks_deleted, done_reply_tx, }))) => {
                log::debug!("demolished, {} blocks actually deleted", blocks_deleted);
                if let Err(_send_error) = done_reply_tx.send(Demolished) {
                    log::warn!("client canceled demolish request");
                }
                return Ok(());
            },

            Event::Task(Err(error)) =>
                return Err(ErrorSeverity::Fatal(Error::Task(error))),
        }
    }
}

struct AsyncTree {
    tree: HashMap<BlockRef, AsyncBlock>,
}

enum AsyncBlock {
    Awaiting {
        lookup_requests_queue: task::LookupRequestsQueue,
        iter_requests_queue: task::IterRequestsQueue,
    },
    Ready {
        block_bytes: Bytes,
        more_lookup_requests: Option<task::LookupRequestsQueue>,
    },
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
        lookup_requests_queue_pool: &pool::Pool<task::LookupRequestsQueueType>,
        iter_requests_queue_pool: &pool::Pool<task::IterRequestsQueueType>,
        wheels_pid: &wheels::Pid,
    )
        -> Option<task::TaskArgs>
    {
        match self.tree.entry(block_ref.clone()) {
            hash_map::Entry::Occupied(mut oe) =>
                match oe.get_mut() {
                    AsyncBlock::Awaiting { lookup_requests_queue, .. } |
                    AsyncBlock::Ready { more_lookup_requests: Some(lookup_requests_queue), .. } => {
                        lookup_requests_queue.push(lookup_request);
                        None
                    },
                    AsyncBlock::Ready { more_lookup_requests: more @ None, .. } => {
                        let mut lookup_requests_queue =
                            lookup_requests_queue_pool.lend(task::LookupRequestsQueueType::new);
                        lookup_requests_queue.clear();
                        lookup_requests_queue.push(lookup_request);
                        *more = Some(lookup_requests_queue);
                        None
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
                ve.insert(AsyncBlock::Awaiting {
                    lookup_requests_queue,
                    iter_requests_queue,
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
        iters_pool: &pool::Pool<task::SearchTreeIterSinks>,
        wheels_pid: &wheels::Pid,
        iter_rec_tx: &mpsc::Sender<task::IterRequest>,
        iter_send_buffer: usize,
    )
        -> Option<task::TaskArgs>
    {
        let block_ref = iter_request.block_ref.clone();
        match self.tree.entry(block_ref.clone()) {
            hash_map::Entry::Occupied(mut oe) =>
                match oe.get_mut() {
                    AsyncBlock::Awaiting { iter_requests_queue, .. } => {
                        iter_requests_queue.push(iter_request);
                        None
                    },
                    AsyncBlock::Ready { block_bytes, .. } => {
                        let mut iter_requests_queue =
                            iter_requests_queue_pool.lend(task::IterRequestsQueueType::new);
                        iter_requests_queue.clear();
                        iter_requests_queue.push(iter_request);
                        let mut iters_tx = iters_pool.lend(task::SearchTreeIterSinks::default);
                        iters_tx.clear();
                        Some(task::TaskArgs::IterBlock(task::iter_block::Args {
                            block_ref,
                            range: SearchRangeBounds::unbounded(),
                            blocks_pool: blocks_pool.clone(),
                            block_bytes: block_bytes.clone(),
                            iters_tx,
                            iter_rec_tx: iter_rec_tx.clone(),
                            iter_requests_queue,
                            iter_send_buffer,
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
                ve.insert(AsyncBlock::Awaiting {
                    lookup_requests_queue,
                    iter_requests_queue,
                });
                Some(task::TaskArgs::LoadBlock(task::load_block::Args {
                    block_ref,
                    wheels_pid: wheels_pid.clone(),
                }))
            },
        }
    }

    fn drop_or_search_more(
        &mut self,
        block_ref: &BlockRef,
        blocks_pool: &BytesPool,
        outcomes_pool: &pool::Pool<Vec<task::SearchOutcome>>,
    )
        -> Option<task::TaskArgs>
    {
        match self.tree.entry(block_ref.clone()) {
            hash_map::Entry::Occupied(mut oe) =>
                match oe.get_mut() {
                    AsyncBlock::Awaiting { .. } =>
                        None,
                    AsyncBlock::Ready { block_bytes, more_lookup_requests, } =>
                        match more_lookup_requests.take() {
                            None => {
                                oe.remove();
                                None
                            },
                            Some(lookup_requests_queue) =>
                                Some(task::TaskArgs::SearchBlock(task::search_block::Args {
                                    block_ref: block_ref.clone(),
                                    blocks_pool: blocks_pool.clone(),
                                    block_bytes: block_bytes.clone(),
                                    lookup_requests_queue,
                                    outcomes: outcomes_pool.lend(Vec::new),
                                })),
                        },
                },

            hash_map::Entry::Vacant(..) =>
                None,
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
