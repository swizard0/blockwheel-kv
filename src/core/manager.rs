use std::{
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
    RequestLookup,
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
    parent_supervisor: SupervisorPid,
    blocks_pool: BytesPool,
    butcher_pid: butcher::Pid,
    wheels_pid: wheels::Pid,
    params: Params,
}

enum Request {
    ButcherFlush { cache: Arc<MemCache>, },
    Lookup(RequestLookup),
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Flushed;

#[derive(Debug)]
pub enum LookupError {
    GenServer(ero::NoProcError),
}

impl Pid {
    pub async fn flush_cache(&mut self, cache: Arc<MemCache>) -> Result<Flushed, ero::NoProcError> {
        self.request_tx.send(Request::ButcherFlush { cache: cache.clone(), }).await
            .map_err(|_send_error| ero::NoProcError)?;
        Ok(Flushed)
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
}

#[derive(Debug)]
enum Error {
    Task(task::Error),
}

struct LookupRequest {
    reply_tx: oneshot::Sender<Option<kv::ValueCell>>,
    pending_count: usize,
    found_fold: Option<kv::ValueCell>,
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

    let mut lookup_requests = Set::new();
    let mut tasks = FuturesUnordered::new();

    let search_tree_lookup_requests_queue_pool = pool::Pool::new();
    let search_tree_iter_requests_queue_pool = pool::Pool::new();
    let search_tree_outcomes_pool = pool::Pool::new();
    let search_tree_iters_pool = pool::Pool::new();

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

            Event::Request(Some(Request::ButcherFlush { cache, })) => {
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
                }
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
                for (_search_tree_ref, search_tree_pid) in search_trees.iter() {
                    tasks.push(task::run_args(task::TaskArgs::LookupSearchTree(
                        task::lookup_search_tree::Args {
                            key: key.clone(),
                            request_ref: request_ref.clone(),
                            search_tree_pid: search_tree_pid.clone(),
                        },
                    )));
                }
            },

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

            Event::Task(Ok(task::TaskDone::MergeSearchTrees(done))) => {
                let search_tree_a_pid = search_trees.remove(done.search_tree_a_ref).unwrap();
                tasks.push(task::run_args(task::TaskArgs::DemolishSearchTree(
                    task::demolish_search_tree::Args {
                        search_tree_pid: search_tree_a_pid,
                    },
                )));

                let search_tree_b_pid = search_trees.remove(done.search_tree_b_ref).unwrap();
                tasks.push(task::run_args(task::TaskArgs::DemolishSearchTree(
                    task::demolish_search_tree::Args {
                        search_tree_pid: search_tree_b_pid,
                    },
                )));

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
                }
            },

            Event::Task(Ok(task::TaskDone::DemolishSearchTree(task::demolish_search_tree::Done))) =>
                (),

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
