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
    blockwheel::{
        block,
    },
    core::{
        butcher,
        search_tree,
        Found,
        MemCache,
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
    ButcherLookup(butcher::LookupError),
    SearchTreeLookup(search_tree::LookupError),
}

struct LookupRequest {
    reply_tx: <kv_context::Context as context::Context>::Lookup,
    pending_count: usize,
    found_fold: FoundFold,
}

struct FoundFold {
    current: Found,
    found_where: FoundWhere,
}

enum FoundWhere {
    Initial,
    Butcher,
    SearchTree {
        generation: Ref,
        block_id: block::Id,
    },
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
                    found_fold: FoundFold {
                        current: Found::Nothing,
                        found_where: FoundWhere::Initial,
                    },
                });
                lookup_tasks.push(run_lookup_task(LookupTaskArg {
                    key: key.clone(),
                    request_ref: request_ref.clone(),
                    kind: LookupTaskArgKind::Butcher(ButcherLookupTaskArg {
                        butcher_pid: state.butcher_pid.clone(),
                    }),
                }));
                for (_search_tree_ref, search_tree_pid) in search_trees.iter() {
                    lookup_tasks.push(run_lookup_task(LookupTaskArg {
                        key: key.clone(),
                        request_ref: request_ref.clone(),
                        kind: LookupTaskArgKind::SearchTree(SearchTreeLookupTaskArg {
                            search_tree_pid: search_tree_pid.clone(),
                        }),
                    }));
                }
            },

            Event::LookupTask(Ok(LookupTaskDone { request_ref, found, })) => {
                let lookup_request = lookup_requests.get_mut(request_ref).unwrap();
                assert!(lookup_request.pending_count > 0);
                lookup_request.pending_count -= 1;
                if lookup_request.pending_count == 0 {


                    unimplemented!()
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
    search_tree_pid: search_tree::Pid,
}

struct LookupTaskDone {
    request_ref: Ref,
    found: Option<kv::KeyValue>,
    found_where: FoundWhere,
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
    let found = args.butcher_pid.lookup(key).await
        .map_err(Error::ButcherLookup)?;
    Ok(LookupTaskDone { request_ref, found, found_where: FoundWhere::Butcher, })
}

async fn run_lookup_task_search_tree(
    request_ref: Ref,
    key: kv::Key,
    mut args: SearchTreeLookupTaskArg,
)
    -> Result<LookupTaskDone, Error>
{
    let found = args.search_tree_pid.lookup(key).await
        .map_err(Error::SearchTreeLookup)?;
    Ok(LookupTaskDone {
        request_ref,
        found,
        found_where: FoundWhere::SearchTree {
            generation: args.generation,
            site: match found {

            },
        },
    })
}