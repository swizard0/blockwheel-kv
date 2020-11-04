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
        Ref,
        Set,
    },
};

use ero::{
    restart,
    ErrorSeverity,
    RestartStrategy,
    supervisor::SupervisorPid,
};

use alloc_pool::bytes::BytesPool;

use crate::{
    kv,
    proto,
    wheels,
    context,
    kv_context,
    core::{
        butcher,
        search_tree,
        MemCache,
        BlockRef,
        ValueCell,
    },
};

#[derive(Clone, Debug)]
pub struct Params {
    pub task_restart_sec: usize,
    pub search_tree_params: search_tree::Params,
}

impl Default for Params {
    fn default() -> Params {
        Params {
            task_restart_sec: 4,
            search_tree_params: Default::default(),
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
    Lookup(proto::RequestLookup<<kv_context::Context as context::Context>::Lookup>),
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

    pub async fn lookup(&mut self, key: kv::Key) -> Result<Option<kv::Value>, LookupError> {
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
    ButcherLookup(ero::NoProcError),
    SearchTreeLookup(search_tree::LookupError),
}

struct LookupRequest {
    reply_tx: <kv_context::Context as context::Context>::Lookup,
    pending_count: usize,
    found_fold: Option<FoundFold>,
}

struct FoundFold {
    value_cell: ValueCell,
    found_where: FoundWhere,
}

enum FoundWhere {
    Butcher,
    SearchTree(FoundWhereSearchTree),
}

struct FoundWhereSearchTree {
    generation: Ref,
    location: SearchTreeLocation,
}

enum SearchTreeLocation {
    BootstrapCache,
    StorageBlock {
        block_ref: BlockRef,
    },
}

fn replace_fold_found(current: &Option<FoundFold>, incoming: &Option<FoundFold>) -> bool {
    match (current, incoming) {
        (None, None) |
        (Some(..), None) |
        (Some(FoundFold { found_where: FoundWhere::Butcher, .. }), Some(FoundFold { found_where: FoundWhere::SearchTree(..), .. })) |
        (
            Some(FoundFold {
                found_where: FoundWhere::SearchTree(FoundWhereSearchTree { location: SearchTreeLocation::BootstrapCache, .. }),
                ..
            }),
            Some(FoundFold {
                found_where: FoundWhere::SearchTree(FoundWhereSearchTree { location: SearchTreeLocation::StorageBlock { .. }, .. }),
                ..
            }),
        ) =>
            false,
        (
            Some(FoundFold {
                found_where: FoundWhere::SearchTree(FoundWhereSearchTree {
                    location: SearchTreeLocation::BootstrapCache,
                    generation: gen_a,
                }),
                ..
            }),
            Some(FoundFold {
                found_where: FoundWhere::SearchTree(FoundWhereSearchTree {
                    location: SearchTreeLocation::BootstrapCache,
                    generation: gen_b,
                }),
                ..
            }),
        ) =>
            gen_a < gen_b,
        (
            Some(FoundFold { found_where: FoundWhere::SearchTree(FoundWhereSearchTree { generation: gen_a, .. }), .. }),
            Some(FoundFold { found_where: FoundWhere::SearchTree(FoundWhereSearchTree { generation: gen_b, .. }), .. }),
        ) if gen_a < gen_b =>
            true,
        (
            Some(FoundFold { found_where: FoundWhere::SearchTree(FoundWhereSearchTree { generation: gen_a, .. }), .. }),
            Some(FoundFold { found_where: FoundWhere::SearchTree(FoundWhereSearchTree { generation: gen_b, .. }), .. }),
        ) if gen_a > gen_b =>
            false,
        (
            Some(FoundFold {
                found_where: FoundWhere::SearchTree(FoundWhereSearchTree {
                    location: SearchTreeLocation::StorageBlock { block_ref: ref_a, },
                    ..
                }),
                ..
            }),
            Some(FoundFold {
                found_where: FoundWhere::SearchTree(FoundWhereSearchTree {
                    location: SearchTreeLocation::StorageBlock { block_ref: ref_b, },
                    ..
                }),
                ..
            }),
        ) if ref_a.block_id >= ref_b.block_id =>
            false,
        (None, Some(..)) |
        (Some(FoundFold { found_where: FoundWhere::SearchTree(..), .. }), Some(FoundFold { found_where: FoundWhere::Butcher, .. })) |
        (
            Some(FoundFold {
                found_where: FoundWhere::SearchTree(FoundWhereSearchTree { location: SearchTreeLocation::StorageBlock { .. }, .. }),
                ..
            }),
            Some(FoundFold {
                found_where: FoundWhere::SearchTree(FoundWhereSearchTree { location: SearchTreeLocation::BootstrapCache, .. }),
                ..
            }),
        ) |
        (
            Some(FoundFold {
                found_where: FoundWhere::SearchTree(FoundWhereSearchTree { location: SearchTreeLocation::StorageBlock { .. }, .. }),
                ..
            }),
            Some(FoundFold {
                found_where: FoundWhere::SearchTree(FoundWhereSearchTree { location: SearchTreeLocation::StorageBlock { .. }, .. }),
                ..
            }),
        ) =>
            true,
        (Some(FoundFold { found_where: FoundWhere::Butcher, .. }), Some(FoundFold { found_where: FoundWhere::Butcher, .. })) =>
            unreachable!(),
    }
}

async fn busyloop(mut child_supervisor_pid: SupervisorPid, mut state: State) -> Result<(), ErrorSeverity<State, Error>> {
    let mut search_trees = Set::new();
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

            Event::Request(Some(Request::ButcherFlush { cache, })) => {
                let search_tree_gen_server = search_tree::GenServer::new();
                let search_tree_pid = search_tree_gen_server.pid();
                child_supervisor_pid.spawn_link_temporary(
                    search_tree_gen_server.run_cache_bootstrap(
                        child_supervisor_pid.clone(),
                        state.blocks_pool.clone(),
                        state.wheels_pid.clone(),
                        state.params.search_tree_params.clone(),
                        cache,
                    ),
                );
                search_trees.insert(search_tree_pid);
            },

            Event::Request(Some(Request::Lookup(proto::RequestLookup { key, context: reply_tx, }))) => {
                let request_ref = lookup_requests.insert(LookupRequest {
                    reply_tx,
                    pending_count: 1 + search_trees.len(),
                    found_fold: None,
                });
                lookup_tasks.push(run_lookup_task(LookupTaskArg {
                    key: key.clone(),
                    request_ref: request_ref.clone(),
                    kind: LookupTaskArgKind::Butcher(ButcherLookupTaskArg {
                        butcher_pid: state.butcher_pid.clone(),
                    }),
                }));
                for (search_tree_ref, search_tree_pid) in search_trees.iter() {
                    lookup_tasks.push(run_lookup_task(LookupTaskArg {
                        key: key.clone(),
                        request_ref: request_ref.clone(),
                        kind: LookupTaskArgKind::SearchTree(SearchTreeLookupTaskArg {
                            search_tree_generation: search_tree_ref.clone(),
                            search_tree_pid: search_tree_pid.clone(),
                        }),
                    }));
                }
            },

            Event::LookupTask(Ok(LookupTaskDone { request_ref, found, })) => {
                let lookup_request = lookup_requests.get_mut(request_ref).unwrap();
                assert!(lookup_request.pending_count > 0);
                lookup_request.pending_count -= 1;
                if replace_fold_found(&lookup_request.found_fold, &found) {
                    lookup_request.found_fold = found;
                }
                if lookup_request.pending_count == 0 {
                    let lookup_request = lookup_requests.remove(request_ref).unwrap();
                    let lookup_result = match lookup_request.found_fold {
                        None =>
                            None,
                        Some(FoundFold { value_cell: ValueCell::Value(value), .. }) =>
                            Some(value),
                        Some(FoundFold { value_cell: ValueCell::Tombstone, .. }) =>
                            None,
                        Some(FoundFold { value_cell: ValueCell::Blackmark, .. }) =>
                            None,
                    };
                    if let Err(_send_error) = lookup_request.reply_tx.send(Ok(lookup_result)) {
                        log::warn!("client canceled lookup request");
                    }
                }
            },

            Event::LookupTask(Err(error)) =>
                return Err(ErrorSeverity::Fatal(error)),
        }
    }
}

struct LookupTaskArg {
    key: kv::Key,
    request_ref: Ref,
    kind: LookupTaskArgKind,
}

enum LookupTaskArgKind {
    Butcher(ButcherLookupTaskArg),
    SearchTree(SearchTreeLookupTaskArg),
}

struct ButcherLookupTaskArg {
    butcher_pid: butcher::Pid,
}

struct SearchTreeLookupTaskArg {
    search_tree_generation: Ref,
    search_tree_pid: search_tree::Pid,
}

struct LookupTaskDone {
    request_ref: Ref,
    found: Option<FoundFold>,
}

async fn run_lookup_task(arg: LookupTaskArg) -> Result<LookupTaskDone, Error> {
    match arg {
        LookupTaskArg { key, request_ref, kind: LookupTaskArgKind::Butcher(args), } =>
            run_lookup_task_butcher(request_ref, key, args).await,
        LookupTaskArg { key, request_ref, kind: LookupTaskArgKind::SearchTree(args), } =>
            run_lookup_task_search_tree(request_ref, key, args).await,
    }
}

async fn run_lookup_task_butcher(
    request_ref: Ref,
    key: kv::Key,
    mut args: ButcherLookupTaskArg,
)
    -> Result<LookupTaskDone, Error>
{
    let maybe_value_cell = args.butcher_pid.lookup(key).await
        .map_err(Error::ButcherLookup)?;
    Ok(LookupTaskDone {
        request_ref,
        found: maybe_value_cell
            .map(|value_cell| FoundFold {
                value_cell,
                found_where: FoundWhere::Butcher,
            }),
    })
}

async fn run_lookup_task_search_tree(
    request_ref: Ref,
    key: kv::Key,
    mut args: SearchTreeLookupTaskArg,
)
    -> Result<LookupTaskDone, Error>
{
    let search_tree_found = args.search_tree_pid.lookup(key).await
        .map_err(Error::SearchTreeLookup)?;
    Ok(LookupTaskDone {
        request_ref,
        found: match search_tree_found {
            search_tree::Found::Nothing =>
                None,
            search_tree::Found::Something {
                value_cell,
                location: search_tree::FoundLocation::Cache,
            } =>
                Some(FoundFold {
                    value_cell,
                    found_where: FoundWhere::SearchTree(FoundWhereSearchTree {
                        generation: args.search_tree_generation,
                        location: SearchTreeLocation::BootstrapCache,
                    }),
                }),
            search_tree::Found::Something {
                value_cell,
                location: search_tree::FoundLocation::Block { block_ref, },
            } =>
                Some(FoundFold {
                    value_cell,
                    found_where: FoundWhere::SearchTree(FoundWhereSearchTree {
                        generation: args.search_tree_generation,
                        location: SearchTreeLocation::StorageBlock {
                            block_ref,
                        },
                    }),
                }),
        },
    })
}
