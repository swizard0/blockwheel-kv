use std::{
    time::{
        Duration,
    },
    ops::{
        RangeBounds,
    },
};

 use futures::{
     stream::{
         self,
         FuturesUnordered,
     },
     channel::{
         mpsc,
         oneshot,
     },
     select,
     StreamExt,
     SinkExt,
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
        BytesPool,
    },
};

use crate::{
    kv,
    job,
    wheels,
    storage,
    version,
    core::{
        performer,
        search_tree_walker,
        search_tree_builder,
        Context,
        RequestInfo,
        RequestInsert,
        RequestLookupKind,
        RequestLookupKindSingle,
        RequestLookupKindRange,
        RequestLookupRange,
        RequestRemove,
        RequestFlush,
        SearchRangeBounds,
        SearchTreeBuilderBlockEntry,
    },
    Info,
    Flushed,
    Removed,
    Inserted,
    LookupRange,
};

pub mod task;

#[derive(Clone, Debug)]
pub struct Params {
    pub task_restart_sec: usize,
    pub performer_params: performer::Params,
    pub iter_send_buffer: usize,
}

impl Default for Params {
    fn default() -> Params {
        Params {
            task_restart_sec: 4,
            performer_params: Default::default(),
            iter_send_buffer: 4,
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

    pub async fn run<J>(
        self,
        parent_supervisor: SupervisorPid,
        thread_pool: edeltraud::Edeltraud<J>,
        blocks_pool: BytesPool,
        version_provider: version::Provider,
        wheels_pid: wheels::Pid,
        params: Params,
    )
    where J: edeltraud::Job + From<job::Job>,
          J::Output: From<job::JobOutput>,
          job::JobOutput: From<J::Output>,
    {
        let terminate_result = restart::restartable(
            ero::Params {
                name: "ero-blockwheel-kv manager task".to_string(),
                restart_strategy: RestartStrategy::Delay {
                    restart_after: Duration::from_secs(params.task_restart_sec as u64),
                },
            },
            State {
                fused_request_rx: self.fused_request_rx,
                parent_supervisor,
                thread_pool,
                blocks_pool,
                version_provider,
                wheels_pid,
                params,
            },
            |mut state| async move {
                let child_supervisor_gen_server = state.parent_supervisor.child_supervisor();
                let child_supervisor_pid = child_supervisor_gen_server.pid();
                state.parent_supervisor.spawn_link_temporary(
                    child_supervisor_gen_server.run(),
                );

                load(child_supervisor_pid, state).await
            },
        ).await;
        if let Err(error) = terminate_result {
            log::error!("fatal error: {:?}", error);
        }
    }
}

struct State<J> where J: edeltraud::Job {
    fused_request_rx: stream::Fuse<mpsc::Receiver<Request>>,
    parent_supervisor: SupervisorPid,
    thread_pool: edeltraud::Edeltraud<J>,
    blocks_pool: BytesPool,
    version_provider: version::Provider,
    wheels_pid: wheels::Pid,
    params: Params,
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
pub enum LookupRangeError {
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

    pub async fn lookup(&mut self, key: kv::Key) -> Result<Option<kv::ValueCell<kv::Value>>, LookupError> {
        let search_range = SearchRangeBounds::single(key);
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(Request::LookupRange(RequestLookupRange {
                    search_range: search_range.clone(),
                    reply_kind: RequestLookupKind::Single(
                        RequestLookupKindSingle { reply_tx, },
                    ),
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

    pub async fn lookup_range<R>(&mut self, range: R) -> Result<LookupRange, LookupRangeError> where R: RangeBounds<kv::Key> {
        let search_range: SearchRangeBounds = range.into();
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(Request::LookupRange(RequestLookupRange {
                    search_range: search_range.clone(),
                    reply_kind: RequestLookupKind::Range(
                        RequestLookupKindRange { reply_tx, },
                    ),
                }))
                .await
                .map_err(|_send_error| LookupRangeError::GenServer(ero::NoProcError))?;

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

enum Request {
    Info(RequestInfo),
    Insert(RequestInsert),
    LookupRange(RequestLookupRange),
    Remove(RequestRemove),
    FlushAll(RequestFlush),
}

#[derive(Debug)]
pub enum Error {
    Task(task::Error),
    WheelsIterBlocks(wheels::IterBlocksError),
    WheelsIterBlocksRxDropped,
    DeserializeBlock {
        block_ref: wheels::BlockRef,
        error: storage::Error,
    },
}

async fn load<J>(
    child_supervisor_pid: SupervisorPid,
    mut state: State<J>,
)
    -> Result<(), ErrorSeverity<State<J>, Error>>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    let mut forest = performer::SearchForest::new();
    let mut blocks_total = 0;

    log::info!("loading search_tree roots from wheels");

    let mut iter_blocks = state.wheels_pid.iter_blocks().await
        .map_err(Error::WheelsIterBlocks)
        .map_err(ErrorSeverity::Fatal)?;

    loop {
        match iter_blocks.block_refs_rx.next().await {
            None =>
                return Err(ErrorSeverity::Fatal(Error::WheelsIterBlocksRxDropped)),
            Some(wheels::IterBlocksItem::Block { block_ref, block_bytes, }) => {
                blocks_total += 1;
                let deserializer = match storage::block_deserialize_iter(&block_bytes) {
                    Ok(deserializer) =>
                        deserializer,
                    Err(storage::Error::InvalidBlockMagic { expected, provided, }) => {
                        log::debug!("skipping block {:?} (invalid magic provided: {}, expected: {})", block_ref, provided, expected);
                        continue;
                    },
                    Err(error) =>
                        return Err(ErrorSeverity::Fatal(Error::DeserializeBlock {
                            block_ref,
                            error,
                        })),
                };
                match deserializer.block_header().node_type {
                    storage::NodeType::Root { tree_entries_count, } => {
                        log::debug!("root search_tree found with {:?} entries in {:?}", tree_entries_count, block_ref);
                        forest.add_constructed(block_ref, tree_entries_count);
                    },
                    storage::NodeType::Leaf =>
                        (),
                }
            },
            Some(wheels::IterBlocksItem::NoMoreBlocks) =>
                break,
        }
    }

    log::info!("loading done, {} search_trees restored within {} blocks", forest.len(), blocks_total);

    let pools = Pools::new();

    let performer = performer::Performer::new(
        state.params.performer_params.clone(),
        state.version_provider.clone(),
        pools.kv_pool.clone(),
        pools.sources_pool.clone(),
        pools.block_entry_steps_pool.clone(),
        forest,
    );

    busyloop(
        child_supervisor_pid,
        performer,
        pools,
        state,
    ).await
}

struct Pools {
    kv_pool: pool::Pool<Vec<kv::KeyValuePair<kv::Value>>>,
    block_entries_pool: pool::Pool<Vec<SearchTreeBuilderBlockEntry>>,
    sources_pool: pool::Pool<Vec<performer::LookupRangeSource>>,
    block_entry_steps_pool: pool::Pool<Vec<search_tree_walker::BlockEntryStep>>,
}

impl Pools {
    fn new() -> Self {
        Self {
            kv_pool: pool::Pool::new(),
            block_entries_pool: pool::Pool::new(),
            sources_pool: pool::Pool::new(),
            block_entry_steps_pool: pool::Pool::new(),
        }
    }
}

async fn busyloop<J>(
    _child_supervisor_pid: SupervisorPid,
    performer: performer::Performer<Context>,
    pools: Pools,
    mut state: State<J>,
)
    -> Result<(), ErrorSeverity<State<J>, Error>>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    enum PerformerState {
        Ready {
            job_args: task::performer::JobArgs,
        },
        InProgress,
    }

    let mut performer_state = PerformerState::Ready {
        job_args: task::performer::JobArgs {
            env: task::performer::Env::default(),
            kont: task::performer::Kont::Start { performer, },
        },
    };

    let mut incoming = task::performer::Incoming::default();

    enum Mode {
        Regular,
        Flushing,
    }

    let mut mode = Mode::Regular;

    let mut tasks = FuturesUnordered::new();
    let mut tasks_count = 0;

    loop {
        enum PerformerAction<A, S> {
            Run(A),
            KeepState(S),
        }

        let performer_action = match performer_state {
            PerformerState::Ready {
                job_args: job_args @ task::performer::JobArgs { kont: task::performer::Kont::Start { .. }, .. },
            } =>
                PerformerAction::Run(job_args),
            PerformerState::Ready {
                job_args: job_args @ task::performer::JobArgs { kont: task::performer::Kont::StepPoll { .. }, .. },
            } if !incoming.is_empty() =>
                PerformerAction::Run(job_args),
            other =>
                PerformerAction::KeepState(other),
        };

        match performer_action {
            PerformerAction::Run(mut job_args) => {
                job_args.env.incoming.transfill_from(&mut incoming);
                if job_args.env.incoming.is_empty() {
                    performer_state = PerformerState::Ready { job_args, };
                } else {
                    tasks.push(task::run_args(task::TaskArgs::Performer(
                        task::performer::Args {
                            job_args,
                            thread_pool: state.thread_pool.clone(),
                        },
                    )));
                    tasks_count += 1;
                    performer_state = PerformerState::InProgress;
                }
            },
            PerformerAction::KeepState(state) =>
                performer_state = state,
        }

        enum Event<R, T> {
            Request(Option<R>),
            Task(T),
        }

        let event = match mode {
            Mode::Regular if tasks_count == 0 =>
                Event::Request(state.fused_request_rx.next().await),
            Mode::Regular =>
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
                },
            Mode::Flushing => {
                let task = tasks.next().await.unwrap();
                tasks_count -= 1;
                Event::Task(task)
            },
        };

        match event {

            Event::Request(None) => {
                log::info!("requests sink channel depleted: terminating");
                return Ok(());
            },

            Event::Request(Some(Request::Info(RequestInfo { reply_tx: _, }))) => {
                todo!();
//                 let request_ref = info_requests.insert(InfoRequest {
//                     reply_tx,
//                     pending_count: 1 + search_trees.len(),
//                     info_fold: Info::default(),
//                 });
//                 tasks.push(task::run_args(task::TaskArgs::InfoButcher(
//                     task::info_butcher::Args {
//                         request_ref: request_ref.clone(),
//                         butcher_pid: state.butcher_pid.clone(),
//                     },
//                 )));
//                 tasks_count += 1;
//                 for (_search_tree_ref, search_tree_pid) in search_trees.iter() {
//                     tasks.push(task::run_args(task::TaskArgs::InfoSearchTree(
//                         task::info_search_tree::Args {
//                             request_ref: request_ref.clone(),
//                             search_tree_pid: search_tree_pid.clone(),
//                         },
//                     )));
//                     tasks_count += 1;
//                 }
            },

            Event::Request(Some(Request::Insert(request))) =>
                incoming.request_insert.push(request),

            Event::Request(Some(Request::LookupRange(request))) =>
                incoming.request_lookup_range.push(request),

            Event::Request(Some(Request::Remove(request))) =>
                incoming.request_remove.push(request),

            Event::Request(Some(Request::FlushAll(request))) => {
                incoming.request_flush.push(request);
                mode = Mode::Flushing;
            },

            Event::Task(Ok(task::TaskDone::Performer(task::performer::Done { env, next: task::performer::Next::Poll { next, }, }))) => {
                let mut job_args = task::performer::JobArgs {
                    env,
                    kont: task::performer::Kont::StepPoll { next, },
                };
                // flush butcher
                for flush_butcher_query in job_args.env.outgoing.flush_butcher.drain(..) {
                    let task::performer::FlushButcherQuery { search_tree_id, frozen_memcache, } = flush_butcher_query;
                    tasks.push(task::run_args(task::TaskArgs::FlushButcher(
                        task::flush_butcher::Args {
                            search_tree_builder_params: search_tree_builder::Params {
                                tree_items_count: frozen_memcache.len(),
                                tree_block_size: state.params.performer_params.tree_block_size,
                            },
                            values_inline_size_limit: state.params.performer_params.values_inline_size_limit,
                            search_tree_id,
                            frozen_memcache,
                            wheels_pid: state.wheels_pid.clone(),
                            blocks_pool: state.blocks_pool.clone(),
                            block_entries_pool: pools.block_entries_pool.clone(),
                            thread_pool: state.thread_pool.clone(),
                        },
                    )));
                    tasks_count += 1;
                }
                // lookup range merger ready
                for lookup_range_merger_ready_query in job_args.env.outgoing.lookup_range_merger_ready.drain(..) {
                    let task::performer::LookupRangeMergerReadyQuery { ranges_merger, lookup_context, } = lookup_range_merger_ready_query;
                    tasks.push(task::run_args(task::TaskArgs::LookupRangeMerge(
                        task::lookup_range_merge::Args {
                            ranges_merger,
                            lookup_context,
                            wheels_pid: state.wheels_pid.clone(),
                            thread_pool: state.thread_pool.clone(),
                            iter_send_buffer: state.params.iter_send_buffer,
                        },
                    )));
                    tasks_count += 1;
                }
                // flushed
                for flushed_query in job_args.env.outgoing.flushed.drain(..) {
                    let task::performer::FlushedQuery { flush_context: reply_tx, } = flushed_query;
                    if let Err(_send_error) = reply_tx.send(Flushed) {
                        log::warn!("client canceled flush request");
                    }
                    mode = Mode::Regular;
                }
                // merge search trees
                for merge_search_trees_query in job_args.env.outgoing.merge_search_trees.drain(..) {
                    let task::performer::MergeSearchTreesQuery { ranges_merger, } = merge_search_trees_query;
                    tasks.push(task::run_args(task::TaskArgs::MergeSearchTrees(
                        task::merge_search_trees::Args {
                            ranges_merger,
                            wheels_pid: state.wheels_pid.clone(),
                            blocks_pool: state.blocks_pool.clone(),
                            thread_pool: state.thread_pool.clone(),
                            tree_block_size: state.params.performer_params.tree_block_size,
                        },
                    )));
                    tasks_count += 1;
                }

                performer_state = match performer_state {
                    PerformerState::InProgress =>
                        PerformerState::Ready { job_args, },
                    PerformerState::Ready { .. } =>
                        unreachable!(),
                };
            },

            Event::Task(Ok(task::TaskDone::FlushButcher(task::flush_butcher::Done { search_tree_id, root_block, }))) =>
                incoming.butcher_flushed.push(task::performer::EventButcherFlushed {
                    search_tree_id,
                    root_block,
                }),

            Event::Task(Ok(task::TaskDone::LookupRangeMerge(task::lookup_range_merge::Done { lookup_range_token, }))) =>
                incoming.lookup_range_merge_done.push(task::performer::EventLookupRangeMergeDone {
                    lookup_range_token,
                }),

            Event::Task(Ok(task::TaskDone::MergeSearchTrees(
                task::merge_search_trees::Done {
                    merged_search_tree_ref,
                    merged_search_tree_items_count,
                    lookup_range_token,
                }))) =>
                incoming.merge_search_trees_done.push(task::performer::EventMergeSearchTreesDone {
                    merged_search_tree_ref,
                    merged_search_tree_items_count,
                    lookup_range_token,
                }),

            Event::Task(Err(error)) =>
                return Err(ErrorSeverity::Fatal(Error::Task(error))),

        }
    }

//         match event {
//             Event::FlushCache(None) => {
//                 log::info!("butcher channel depleted: terminating");
//                 return Ok(());
//             },

//             Event::FlushCache(Some(ButcherFlush { cache, })) => {
//                 let items_count = cache.len();
//                 let search_tree_gen_server = search_tree::GenServer::new();
//                 let search_tree_pid = search_tree_gen_server.pid();
//                 child_supervisor_pid.spawn_link_temporary(
//                     search_tree_gen_server.run(
//                         child_supervisor_pid.clone(),
//                         state.thread_pool.clone(),
//                         search_tree_pools.clone(),
//                         state.wheels_pid.clone(),
//                         state.params.search_tree_params.clone(),
//                         search_tree::Mode::CacheBootstrap { cache: cache.clone(), },
//                     ),
//                 );
//                 let search_tree_ref = search_trees.insert(search_tree_pid.clone());
//                 search_tree_refs.push(SearchTreeRef { search_tree_ref, items_count, }, items_count);
//                 let maybe_task_args = maybe_merge_search_trees(
//                     &mut search_tree_refs,
//                     &search_trees,
//                     &state.thread_pool,
//                     &state.blocks_pool,
//                     &merge_blocks_pool,
//                     &merger_iters_pool,
//                     &state.wheels_pid,
//                     state.params.search_tree_params.tree_block_size,
//                     state.params.search_tree_params.merge_tasks_count_limit,
//                 );
//                 if let Some(task_args) = maybe_task_args {
//                     bg_tasks_push(task_args);
//                     bg_tasks_count += 1;
//                     merge_search_trees_tasks_count += 1;
//                 }

//                 let mut invalidated_count = 0;

//                 // maybe invalidate on-fly butcher requests
//                 for (request_ref, LookupRequest { key, butcher_status, pending_count, .. }) in lookup_requests.iter_mut() {
//                     if let LookupRequestButcherStatus::NotReady = butcher_status {
//                         log::debug!("lookup request for {:?} invalidated due to cache flush", key);
//                         *butcher_status = LookupRequestButcherStatus::Invalidated;
//                         tasks.push(task::run_args::<J>(task::TaskArgs::LookupSearchTree(
//                             task::lookup_search_tree::Args {
//                                 key: key.clone(),
//                                 request_ref: request_ref.clone(),
//                                 search_tree_pid: search_tree_pid.clone(),
//                             },
//                         )));
//                         tasks_count += 1;
//                         *pending_count += 1;
//                         invalidated_count += 1;
//                     }
//                 }

//                 log::info!(
//                     "cache flushed: {} invalidated, currently {} in action, {} merging",
//                     invalidated_count,
//                     search_trees.len(),
//                     merge_search_trees_tasks_count,
//                 );
//             },

//             Event::Task(Ok(task::TaskDone::MergeSearchTrees(done))) => {
//                 let search_tree_a_pid = search_trees.remove(done.search_tree_a_ref).unwrap();
//                 tasks.push(task::run_args(task::TaskArgs::DemolishSearchTree(
//                     task::demolish_search_tree::Args {
//                         search_tree_pid: search_tree_a_pid,
//                     },
//                 )));
//                 tasks_count += 1;

//                 let search_tree_b_pid = search_trees.remove(done.search_tree_b_ref).unwrap();
//                 tasks.push(task::run_args(task::TaskArgs::DemolishSearchTree(
//                     task::demolish_search_tree::Args {
//                         search_tree_pid: search_tree_b_pid,
//                     },
//                 )));
//                 tasks_count += 1;

//                 let search_tree_gen_server = search_tree::GenServer::new();
//                 let search_tree_pid = search_tree_gen_server.pid();
//                 child_supervisor_pid.spawn_link_temporary(
//                     search_tree_gen_server.run(
//                         child_supervisor_pid.clone(),
//                         state.thread_pool.clone(),
//                         search_tree_pools.clone(),
//                         state.wheels_pid.clone(),
//                         state.params.search_tree_params.clone(),
//                         search_tree::Mode::Regular { root_block: done.root_block, },
//                     ),
//                 );
//                 let search_tree_ref = search_trees.insert(search_tree_pid);
//                 search_tree_refs.push(SearchTreeRef { search_tree_ref, items_count: done.items_count, }, done.items_count);
//                 let maybe_task_args = maybe_merge_search_trees(
//                     &mut search_tree_refs,
//                     &search_trees,
//                     &state.thread_pool,
//                     &state.blocks_pool,
//                     &merge_blocks_pool,
//                     &merger_iters_pool,
//                     &state.wheels_pid,
//                     state.params.search_tree_params.tree_block_size,
//                     state.params.search_tree_params.merge_tasks_count_limit,
//                 );
//                 if let Some(task_args) = maybe_task_args {
//                     bg_tasks_push(task_args);
//                     bg_tasks_count += 1;
//                     merge_search_trees_tasks_count += 1;
//                 }

//                 merge_search_trees_tasks_count -= 1;
//                 log::info!(
//                     "two search_tree of {} merged in {:?}: currently {} in action, {} merging",
//                     done.items_count,
//                     done.timings,
//                     search_trees.len(),
//                     merge_search_trees_tasks_count,
//                 );
//             },

//             Event::Task(Ok(task::TaskDone::DemolishSearchTree(task::demolish_search_tree::Done))) => {
//                 log::debug!("search tree DEMOLISHED");
//             },

//             Event::Task(Ok(task::TaskDone::RetrieveValue(task::retrieve_value::Done::RetrieveSuccess))) =>
//                 (),

//             Event::Task(Ok(task::TaskDone::RetrieveValue(task::retrieve_value::Done::DeprecatedResults { key, reply_tx, }))) => {
//                 log::debug!("task::TaskDone::RetrieveValue deprecated results: retrying LOOKUP request");
//                 launch_lookup_request(
//                     key,
//                     reply_tx,
//                     &mut lookup_requests,
//                     &search_trees,
//                     &state.butcher_pid,
//                     |args| {
//                         tasks.push(task::run_args(args));
//                         tasks_count += 1;
//                     },
//                 );
//             },

//             Event::Task(Err(error)) =>
//                 return Err(ErrorSeverity::Fatal(Error::Task(error))),
//         }
//     }
}
