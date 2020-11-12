use std::{
    mem,
    sync::Arc,
    cmp::Reverse,
    time::Duration,
    collections::BinaryHeap,
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

use o1::set::{
    Set,
    Ref,
};

use ero::{
    restart,
    ErrorSeverity,
    RestartStrategy,
    supervisor::SupervisorPid,
};

use alloc_pool::{
    pool,
    bytes::BytesPool,
};

use crate::{
    kv,
    wheels,
    core::{
        butcher,
        search_tree,
        MemCache,
    },
    Info,
    Flushed,
    Removed,
    Inserted,
    RequestInfo,
    RequestInsert,
    RequestLookup,
    RequestRemove,
    RequestFlush,
};

mod task;

#[derive(Clone, Debug)]
pub struct Params {
    pub task_restart_sec: usize,
    pub search_tree_params: search_tree::Params,
    pub standalone_search_trees_count: usize,
}

impl Default for Params {
    fn default() -> Params {
        Params {
            task_restart_sec: 4,
            search_tree_params: Default::default(),
            standalone_search_trees_count: 2,
        }
    }
}

pub struct GenServer {
    request_tx: mpsc::Sender<Request>,
    fused_request_rx: stream::Fuse<mpsc::Receiver<Request>>,
    flush_cache_tx: mpsc::Sender<ButcherFlush>,
    fused_flush_cache_rx: stream::Fuse<mpsc::Receiver<ButcherFlush>>,
}

#[derive(Clone)]
pub struct Pid {
    request_tx: mpsc::Sender<Request>,
    flush_cache_tx: mpsc::Sender<ButcherFlush>,
}

impl GenServer {
    pub fn new() -> GenServer {
        let (request_tx, request_rx) = mpsc::channel(0);
        let (flush_cache_tx, flush_cache_rx) = mpsc::channel(0);
        GenServer {
            request_tx,
            fused_request_rx: request_rx.fuse(),
            flush_cache_tx,
            fused_flush_cache_rx: flush_cache_rx.fuse(),
        }
    }

    pub fn pid(&self) -> Pid {
        Pid {
            request_tx: self.request_tx.clone(),
            flush_cache_tx: self.flush_cache_tx.clone(),
        }
    }

    pub async fn run(
        self,
        parent_supervisor: SupervisorPid,
        blocks_pool: BytesPool,
        butcher_pid: butcher::Pid,
        wheels_pid: wheels::Pid,
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
                fused_flush_cache_rx: self.fused_flush_cache_rx,
                parent_supervisor,
                blocks_pool,
                butcher_pid,
                wheels_pid,
                params,
            },
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
}

struct State {
    fused_request_rx: stream::Fuse<mpsc::Receiver<Request>>,
    fused_flush_cache_rx: stream::Fuse<mpsc::Receiver<ButcherFlush>>,
    parent_supervisor: SupervisorPid,
    blocks_pool: BytesPool,
    butcher_pid: butcher::Pid,
    wheels_pid: wheels::Pid,
    params: Params,
}

enum Request {
    Info(RequestInfo),
    Insert(RequestInsert),
    Lookup(RequestLookup),
    Remove(RequestRemove),
    FlushAll(RequestFlush),
}

struct ButcherFlush {
    cache: Arc<MemCache>,
}

#[derive(Debug)]
pub enum InsertError {
    GenServer(ero::NoProcError),
}

#[derive(Debug)]
pub enum LookupError {
    GenServer(ero::NoProcError),
}

#[derive(Debug)]
pub enum RemoveError {
    GenServer(ero::NoProcError),
}

#[derive(Debug)]
pub enum FlushError {
    GenServer(ero::NoProcError),
}

impl Pid {
    pub async fn flush_cache(&mut self, cache: Arc<MemCache>) -> Result<Flushed, ero::NoProcError> {
        self.flush_cache_tx.send(ButcherFlush { cache: cache.clone(), }).await
            .map_err(|_send_error| ero::NoProcError)?;
        Ok(Flushed)
    }

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

    pub async fn insert(&mut self, key: kv::Key, value: kv::Value) -> Result<Inserted, InsertError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(Request::Insert(RequestInsert {
                    key: key.clone(),
                    value: value.clone(),
                    reply_tx,
                }))
                .await
                .map_err(|_send_error| InsertError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(inserted) =>
                    return Ok(inserted),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn lookup(&mut self, key: kv::Key) -> Result<Option<kv::ValueCell>, LookupError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(Request::Lookup(RequestLookup {
                    key: key.clone(),
                    reply_tx,
                }))
                .await
                .map_err(|_send_error| LookupError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(result) =>
                    return Ok(result),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn remove(&mut self, key: kv::Key) -> Result<Removed, RemoveError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(Request::Remove(RequestRemove {
                    key: key.clone(),
                    reply_tx,
                }))
                .await
                .map_err(|_send_error| RemoveError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(result) =>
                    return Ok(result),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn flush_all(&mut self) -> Result<Flushed, FlushError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(Request::FlushAll(RequestFlush { reply_tx, })).await
                .map_err(|_send_error| FlushError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(Flushed) =>
                    return Ok(Flushed),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }
}

#[derive(Debug)]
enum Error {
    Task(task::Error),
}

struct InfoRequest {
    reply_tx: oneshot::Sender<Info>,
    pending_count: usize,
    info_fold: Info,
}

struct LookupRequest {
    reply_tx: oneshot::Sender<Option<kv::ValueCell>>,
    pending_count: usize,
    found_fold: Option<kv::ValueCell>,
}

struct FlushRequest {
    butcher_done: bool,
    search_trees_pending_count: usize,
}

fn replace_fold_found(current: &Option<kv::ValueCell>, incoming: &Option<kv::ValueCell>) -> bool {
    match (current, incoming) {
        (None, None) | (Some(..), None) =>
            false,
        (None, Some(..)) =>
            true,
        (Some(kv::ValueCell { version: version_current, .. }), Some(kv::ValueCell { version: version_incoming, .. })) =>
            version_current < version_incoming,
    }
}

async fn busyloop(mut child_supervisor_pid: SupervisorPid, mut state: State) -> Result<(), ErrorSeverity<State, Error>> {
    let mut search_trees = Set::new();
    let mut search_tree_refs = BinaryHeap::new();

    let mut info_requests = Set::new();
    let mut lookup_requests = Set::new();
    let mut flush_requests = Set::new();
    let mut tasks = FuturesUnordered::new();
    let mut tasks_count = 0;

    let search_tree_lookup_requests_queue_pool = pool::Pool::new();
    let search_tree_iter_requests_queue_pool = pool::Pool::new();
    let search_tree_outcomes_pool = pool::Pool::new();
    let search_tree_iters_pool = pool::Pool::new();

    enum Mode {
        Regular,
        Flushing { done_reply_tx: oneshot::Sender<Flushed>, },
    };

    let mut current_mode = Mode::Regular;

    loop {
        enum Event<R, F, T> {
            Request(Option<R>),
            FlushCache(Option<F>),
            Task(T),
        }

        let event = match mem::replace(&mut current_mode, Mode::Regular) {
            Mode::Regular if tasks_count == 0 =>
                select! {
                    result = state.fused_request_rx.next() =>
                        Event::Request(result),
                    result = state.fused_flush_cache_rx.next() =>
                        Event::FlushCache(result),
                },
            Mode::Regular =>
                select! {
                    result = state.fused_request_rx.next() =>
                        Event::Request(result),
                    result = state.fused_flush_cache_rx.next() =>
                        Event::FlushCache(result),
                    result = tasks.next() => match result {
                        None =>
                            unreachable!(),
                        Some(task) => {
                            tasks_count -= 1;
                            Event::Task(task)
                        },
                    },
                },
            Mode::Flushing { done_reply_tx, } if tasks_count == 0 => {
                log::debug!("Mode::Flushing: all tasks finished, responding Flushed and switching mode");
                if let Err(_send_error) = done_reply_tx.send(Flushed) {
                    log::warn!("client canceled flush request");
                }
                continue;
            },
            Mode::Flushing { done_reply_tx, } => {
                log::debug!("FlushMode::InProgress: {} tasks left", tasks_count);
                current_mode = Mode::Flushing { done_reply_tx, };
                select! {
                    result = state.fused_flush_cache_rx.next() =>
                        Event::FlushCache(result),
                    result = tasks.next() => match result {
                        None =>
                            unreachable!(),
                        Some(task) => {
                            tasks_count -= 1;
                            Event::Task(task)
                        },
                    },
                }
            },
        };

        match event {
            Event::FlushCache(None) => {
                log::info!("butcher channel depleted: terminating");
                return Ok(());
            },

            Event::FlushCache(Some(ButcherFlush { cache, })) => {
                let items_count = cache.len();
                let search_tree_gen_server = search_tree::GenServer::new();
                let search_tree_pid = search_tree_gen_server.pid();
                child_supervisor_pid.spawn_link_temporary(
                    search_tree_gen_server.run(
                        child_supervisor_pid.clone(),
                        state.blocks_pool.clone(),
                        search_tree_lookup_requests_queue_pool.clone(),
                        search_tree_iter_requests_queue_pool.clone(),
                        search_tree_outcomes_pool.clone(),
                        search_tree_iters_pool.clone(),
                        state.wheels_pid.clone(),
                        state.params.search_tree_params.clone(),
                        search_tree::Mode::CacheBootstrap { cache, },
                    ),
                );
                let search_tree_ref = search_trees.insert(search_tree_pid);
                let maybe_task_args = push_search_tree_refs(
                    &mut search_tree_refs,
                    SearchTreeRef { search_tree_ref, items_count, },
                    state.params.standalone_search_trees_count,
                    &search_trees,
                    &state.blocks_pool,
                    &state.wheels_pid,
                    state.params.search_tree_params.tree_block_size,
                );
                if let Some(task_args) = maybe_task_args {
                    tasks.push(task::run_args(task_args));
                    tasks_count += 1;
                }
            },

            Event::Request(None) => {
                log::info!("requests sink channel depleted: terminating");
                return Ok(());
            },

            Event::Request(Some(Request::Info(RequestInfo { reply_tx, }))) => {
                let request_ref = info_requests.insert(InfoRequest {
                    reply_tx,
                    pending_count: 1 + search_trees.len(),
                    info_fold: Info::default(),
                });
                tasks.push(task::run_args(task::TaskArgs::InfoButcher(
                    task::info_butcher::Args {
                        request_ref: request_ref.clone(),
                        butcher_pid: state.butcher_pid.clone(),
                    },
                )));
                tasks_count += 1;
                for (_search_tree_ref, search_tree_pid) in search_trees.iter() {
                    tasks.push(task::run_args(task::TaskArgs::InfoSearchTree(
                        task::info_search_tree::Args {
                            request_ref: request_ref.clone(),
                            search_tree_pid: search_tree_pid.clone(),
                        },
                    )));
                    tasks_count += 1;
                }
            },

            Event::Request(Some(Request::Insert(request))) => {
                tasks.push(task::run_args(task::TaskArgs::InsertButcher(
                    task::insert_butcher::Args {
                        request,
                        butcher_pid: state.butcher_pid.clone(),
                    },
                )));
                tasks_count += 1;
            },

            Event::Request(Some(Request::Lookup(RequestLookup { key, reply_tx, }))) => {
                let request_ref = lookup_requests.insert(LookupRequest {
                    reply_tx,
                    pending_count: 1 + search_trees.len(),
                    found_fold: None,
                });
                tasks.push(task::run_args(task::TaskArgs::LookupButcher(
                    task::lookup_butcher::Args {
                        key: key.clone(),
                        request_ref: request_ref.clone(),
                        butcher_pid: state.butcher_pid.clone(),
                    },
                )));
                tasks_count += 1;
                for (_search_tree_ref, search_tree_pid) in search_trees.iter() {
                    tasks.push(task::run_args(task::TaskArgs::LookupSearchTree(
                        task::lookup_search_tree::Args {
                            key: key.clone(),
                            request_ref: request_ref.clone(),
                            search_tree_pid: search_tree_pid.clone(),
                        },
                    )));
                    tasks_count += 1;
                }
            },

            Event::Request(Some(Request::Remove(request))) => {
                tasks.push(task::run_args(task::TaskArgs::RemoveButcher(
                    task::remove_butcher::Args {
                        request,
                        butcher_pid: state.butcher_pid.clone(),
                    },
                )));
                tasks_count += 1;
            },

            Event::Request(Some(Request::FlushAll(RequestFlush { reply_tx, }))) => {
                log::debug!("Request::FlushAll for butcher first");

                let request_ref = flush_requests.insert(FlushRequest {
                    butcher_done: false,
                    search_trees_pending_count: 0,
                });
                tasks.push(task::run_args(task::TaskArgs::FlushButcher(
                    task::flush_butcher::Args {
                        request_ref,
                        butcher_pid: state.butcher_pid.clone(),
                    },
                )));
                tasks_count += 1;
                current_mode = Mode::Flushing { done_reply_tx: reply_tx, };
            },

            Event::Task(Ok(task::TaskDone::InfoButcher(task::info_butcher::Done { request_ref, info, }))) |
            Event::Task(Ok(task::TaskDone::InfoSearchTree(task::info_search_tree::Done { request_ref, info, }))) => {
                let info_request = info_requests.get_mut(request_ref).unwrap();
                assert!(info_request.pending_count > 0);
                info_request.pending_count -= 1;
                info_request.info_fold += info;
                if info_request.pending_count == 0 {
                    let info_request = info_requests.remove(request_ref).unwrap();
                    let info = info_request.info_fold;
                    if let Err(_send_error) = info_request.reply_tx.send(info) {
                        log::warn!("client canceled info request");
                    }
                }
            },

            Event::Task(Ok(task::TaskDone::InsertButcher(task::insert_butcher::Done))) =>
                (),

            Event::Task(Ok(task::TaskDone::LookupButcher(task::lookup_butcher::Done { request_ref, found, }))) |
            Event::Task(Ok(task::TaskDone::LookupSearchTree(task::lookup_search_tree::Done { request_ref, found, }))) => {
                let lookup_request = lookup_requests.get_mut(request_ref).unwrap();
                assert!(lookup_request.pending_count > 0);
                lookup_request.pending_count -= 1;
                if replace_fold_found(&lookup_request.found_fold, &found) {
                    lookup_request.found_fold = found;
                }
                if lookup_request.pending_count == 0 {
                    let lookup_request = lookup_requests.remove(request_ref).unwrap();
                    let lookup_result = lookup_request.found_fold;
                    if let Err(_send_error) = lookup_request.reply_tx.send(lookup_result) {
                        log::warn!("client canceled lookup request");
                    }
                }
            },

            Event::Task(Ok(task::TaskDone::RemoveButcher(task::remove_butcher::Done))) =>
                (),

            Event::Task(Ok(task::TaskDone::FlushButcher(task::flush_butcher::Done { request_ref, }))) => {
                log::debug!("task::TaskDone::FlushButcher received, proceeding with {} search_trees", search_trees.len());
                assert!(matches!(current_mode, Mode::Flushing { .. }));
                let flush_request = flush_requests.get_mut(request_ref).unwrap();
                assert!(!flush_request.butcher_done);
                flush_request.butcher_done = true;
                for (_search_tree_ref, search_tree_pid) in search_trees.iter() {
                    tasks.push(task::run_args(task::TaskArgs::FlushSearchTree(
                        task::flush_search_tree::Args {
                            request_ref: request_ref.clone(),
                            search_tree_pid: search_tree_pid.clone(),
                        },
                    )));
                    tasks_count += 1;
                    flush_request.search_trees_pending_count += 1;
                }
            },

            Event::Task(Ok(task::TaskDone::FlushSearchTree(task::flush_search_tree::Done { request_ref, }))) => {
                assert!(matches!(current_mode, Mode::Flushing { .. }));
                let flush_request = flush_requests.get_mut(request_ref).unwrap();
                assert!(flush_request.butcher_done);
                assert!(flush_request.search_trees_pending_count > 0);
                flush_request.search_trees_pending_count -= 1;
                log::debug!("task::TaskDone::FlushSearchTree received ({} left)", flush_request.search_trees_pending_count);
                if flush_request.search_trees_pending_count == 0 {
                    log::debug!("task::TaskDone::FlushSearchTree finished, waiting for all tasks to be done");
                    flush_requests.remove(request_ref).unwrap();
                }
            },

            Event::Task(Ok(task::TaskDone::MergeSearchTrees(done))) => {
                let search_tree_a_pid = search_trees.remove(done.search_tree_a_ref).unwrap();
                tasks.push(task::run_args(task::TaskArgs::DemolishSearchTree(
                    task::demolish_search_tree::Args {
                        search_tree_pid: search_tree_a_pid,
                    },
                )));
                tasks_count += 1;

                let search_tree_b_pid = search_trees.remove(done.search_tree_b_ref).unwrap();
                tasks.push(task::run_args(task::TaskArgs::DemolishSearchTree(
                    task::demolish_search_tree::Args {
                        search_tree_pid: search_tree_b_pid,
                    },
                )));
                tasks_count += 1;

                let search_tree_gen_server = search_tree::GenServer::new();
                let search_tree_pid = search_tree_gen_server.pid();
                child_supervisor_pid.spawn_link_temporary(
                    search_tree_gen_server.run(
                        child_supervisor_pid.clone(),
                        state.blocks_pool.clone(),
                        search_tree_lookup_requests_queue_pool.clone(),
                        search_tree_iter_requests_queue_pool.clone(),
                        search_tree_outcomes_pool.clone(),
                        search_tree_iters_pool.clone(),
                        state.wheels_pid.clone(),
                        state.params.search_tree_params.clone(),
                        search_tree::Mode::Regular { root_block: done.root_block, },
                    ),
                );
                let search_tree_ref = search_trees.insert(search_tree_pid);
                let maybe_task_args = push_search_tree_refs(
                    &mut search_tree_refs,
                    SearchTreeRef { search_tree_ref, items_count: done.items_count, },
                    state.params.standalone_search_trees_count,
                    &search_trees,
                    &state.blocks_pool,
                    &state.wheels_pid,
                    state.params.search_tree_params.tree_block_size,
                );
                if let Some(task_args) = maybe_task_args {
                    tasks.push(task::run_args(task_args));
                    tasks_count += 1;
                }
            },

            Event::Task(Ok(task::TaskDone::DemolishSearchTree(task::demolish_search_tree::Done))) => {
                log::debug!("search tree DEMOLISHED");
            },

            Event::Task(Err(error)) =>
                return Err(ErrorSeverity::Fatal(Error::Task(error))),
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct SearchTreeRef {
    items_count: usize,
    search_tree_ref: Ref,
}

fn push_search_tree_refs(
    search_tree_refs: &mut BinaryHeap<Reverse<SearchTreeRef>>,
    search_tree_ref: SearchTreeRef,
    standalone_search_trees_count: usize,
    search_trees: &Set<search_tree::Pid>,
    blocks_pool: &BytesPool,
    wheels_pid: &wheels::Pid,
    tree_block_size: usize,
)
    -> Option<task::TaskArgs>
{
    search_tree_refs.push(Reverse(search_tree_ref));
    if search_tree_refs.len() <= standalone_search_trees_count {
        None
    } else {
        let Reverse(SearchTreeRef { search_tree_ref: search_tree_a_ref, .. }) = search_tree_refs.pop().unwrap();
        let Reverse(SearchTreeRef { search_tree_ref: search_tree_b_ref, .. }) = search_tree_refs.pop().unwrap();
        let search_tree_a_pid = search_trees.get(search_tree_a_ref).unwrap().clone();
        let search_tree_b_pid = search_trees.get(search_tree_b_ref).unwrap().clone();
        Some(task::TaskArgs::MergeSearchTrees(
            task::merge_search_trees::Args {
                search_tree_a_ref,
                search_tree_b_ref,
                search_tree_a_pid,
                search_tree_b_pid,
                blocks_pool: blocks_pool.clone(),
                wheels_pid: wheels_pid.clone(),
                tree_block_size,
            },
        ))
    }
}
