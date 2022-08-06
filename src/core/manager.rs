use std::{
    mem,
//     sync::Arc,
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

// use o1::set::{
//     Set,
//     Ref,
// };

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
//     Shared,
//     Unique,
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
//         merger,
//         butcher,
//         bin_merger,
//         search_tree,
//         MemCache,
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
}

impl Default for Params {
    fn default() -> Params {
        Params {
            task_restart_sec: 4,
            performer_params: Default::default(),
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

// struct InfoRequest {
//     reply_tx: oneshot::Sender<Info>,
//     pending_count: usize,
//     info_fold: Info,
// }

// struct LookupRequest {
//     key: kv::Key,
//     reply_tx: oneshot::Sender<Option<kv::ValueCell<kv::Value>>>,
//     butcher_status: LookupRequestButcherStatus,
//     pending_count: usize,
//     found_fold: Option<kv::ValueCell<storage::OwnedValueBlockRef>>,
// }

// enum LookupRequestButcherStatus {
//     NotReady,
//     Done,
//     Invalidated,
// }

// struct LookupRangeRequest {
//     range: SearchRangeBounds,
//     key_values_tx: mpsc::Sender<KeyValueStreamItem>,
//     butcher_iter_items: Shared<Vec<kv::KeyValuePair<kv::Value>>>,
//     merger_iters: Unique<Vec<merger::KeyValuesIterRx>>,
//     pending_count: usize,
// }

// struct FlushRequest {
//     butcher_done: bool,
//     search_trees_pending_count: usize,
// }

// fn replace_fold_found(
//     current: &Option<kv::ValueCell<storage::OwnedValueBlockRef>>,
//     incoming: &Option<kv::ValueCell<storage::OwnedValueBlockRef>>,
// )
//     -> bool
// {
//     match (current, incoming) {
//         (None, None) | (Some(..), None) =>
//             false,
//         (None, Some(..)) =>
//             true,
//         (Some(kv::ValueCell { version: version_current, .. }), Some(kv::ValueCell { version: version_incoming, .. })) =>
//             version_current < version_incoming,
//     }
// }

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

    let mut tasks = FuturesUnordered::new();
    let mut tasks_count = 0;

    loop {
        match performer_state {
            PerformerState::Ready { mut job_args, } => {
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
            PerformerState::InProgress =>
                performer_state = PerformerState::InProgress,
        }

        enum Event<R, T> {
            Request(Option<R>),
            Task(T),
        }

        let event = if tasks_count == 0 {
            Event::Request(state.fused_request_rx.next().await)
        } else {
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
            }
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

            Event::Request(Some(Request::LookupRange(RequestLookupRange { search_range: _, reply_kind: _, }))) => {
                todo!();
//                 let (key_values_tx, key_values_rx) =
//                     mpsc::channel(state.params.search_tree_params.iter_send_buffer);
//                 let lookup_range = LookupRange { key_values_rx, };
//                 if let Err(_send_error) = reply_tx.send(lookup_range) {
//                     log::warn!("client canceled lookup_range request");
//                 }
//                 tasks.push(task::run_args(task::TaskArgs::LookupRangeButcher(
//                     task::lookup_range_butcher::Args {
//                         range,
//                         key_values_tx,
//                         iter_items_pool: iter_items_pool.clone(),
//                         butcher_pid: state.butcher_pid.clone(),
//                     },
//                 )));
//                 tasks_count += 1;
            },

            Event::Request(Some(Request::Remove(_request))) => {
                todo!();
//                 tasks.push(task::run_args(task::TaskArgs::RemoveButcher(
//                     task::remove_butcher::Args {
//                         request,
//                         butcher_pid: state.butcher_pid.clone(),
//                     },
//                 )));
//                 tasks_count += 1;
            },

            Event::Request(Some(Request::FlushAll(RequestFlush { reply_tx: _, }))) => {
                todo!();
//                 log::debug!("Request::FlushAll for butcher first");

//                 let request_ref = flush_requests.insert(FlushRequest {
//                     butcher_done: false,
//                     search_trees_pending_count: 0,
//                 });
//                 tasks.push(task::run_args(task::TaskArgs::FlushButcher(
//                     task::flush_butcher::Args {
//                         request_ref,
//                         butcher_pid: state.butcher_pid.clone(),
//                     },
//                 )));
//                 tasks_count += 1;
//                 current_mode = Mode::Flushing { done_reply_tx: reply_tx, };
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
                            thread_pool: state.thread_pool.clone(),
                        },
                    )));
                    tasks_count += 1;
                }

                let prev_performer_state = mem::replace(
                    &mut performer_state,
                    PerformerState::Ready { job_args, },
                );
                assert!(matches!(prev_performer_state, PerformerState::InProgress));

                todo!();
            },

            Event::Task(Ok(task::TaskDone::FlushButcher(task::flush_butcher::Done { search_tree_id, root_block, }))) =>
                incoming.butcher_flushed.push(task::performer::EventButcherFlushed {
                    search_tree_id,
                    root_block,
                }),

            Event::Task(Ok(task::TaskDone::LookupRangeMerge(task::lookup_range_merge::Done {  }))) => {
                todo!()
            },

            Event::Task(Err(error)) =>
                return Err(ErrorSeverity::Fatal(Error::Task(error))),

        }
    }

//     let (bg_tasks_tx, bg_tasks_rx) = mpsc::channel(0);
//     let mut fused_bg_tasks_rx = bg_tasks_rx.fuse();
//     let mut bg_tasks_count = 0;

//     let mut bg_tasks_supervisor_pid = child_supervisor_pid.clone();
//     let mut bg_tasks_push = |args| {
//         let mut bg_tasks_tx = bg_tasks_tx.clone();
//         let bg_task = task::run_args(args);
//         bg_tasks_supervisor_pid.spawn_link_temporary(async move {
//             let task_result = bg_task.await;
//             bg_tasks_tx.send(task_result).await.ok();
//         });
//     };

//     let mut merge_search_trees_tasks_count = 0;

//     enum Mode {
//         Regular,
//         Flushing { done_reply_tx: oneshot::Sender<Flushed>, },
//     }

//     let mut current_mode = Mode::Regular;

//     loop {
//         let maybe_task_args = maybe_merge_search_trees(
//             &mut search_tree_refs,
//             &search_trees,
//             &state.thread_pool,
//             &state.blocks_pool,
//             &merge_blocks_pool,
//             &merger_iters_pool,
//             &state.wheels_pid,
//             state.params.search_tree_params.tree_block_size,
//             state.params.search_tree_params.merge_tasks_count_limit,
//         );
//         if let Some(task_args) = maybe_task_args {
//             bg_tasks_push(task_args);
//             bg_tasks_count += 1;
//             merge_search_trees_tasks_count += 1;
//             continue;
//         }
//         break;
//     }

//     loop {
//         enum Event<R, F, T> {
//             Request(Option<R>),
//             FlushCache(Option<F>),
//             Task(T),
//         }

//         let event = match mem::replace(&mut current_mode, Mode::Regular) {
//             Mode::Regular if tasks_count == 0 =>
//                 select! {
//                     result = state.fused_request_rx.next() =>
//                         Event::Request(result),
//                     result = state.fused_flush_cache_rx.next() =>
//                         Event::FlushCache(result),
//                     result = fused_bg_tasks_rx.next() => match result {
//                         None =>
//                             unreachable!(),
//                         Some(task) => {
//                             bg_tasks_count -= 1;
//                             Event::Task(task)
//                         },
//                     },
//                 },
//             Mode::Regular =>
//                 select! {
//                     result = state.fused_request_rx.next() =>
//                         Event::Request(result),
//                     result = state.fused_flush_cache_rx.next() =>
//                         Event::FlushCache(result),
//                     result = fused_bg_tasks_rx.next() => match result {
//                         None =>
//                             unreachable!(),
//                         Some(task) => {
//                             bg_tasks_count -= 1;
//                             Event::Task(task)
//                         },
//                     },
//                     result = tasks.next() => match result {
//                         None =>
//                             unreachable!(),
//                         Some(task) => {
//                             tasks_count -= 1;
//                             Event::Task(task)
//                         },
//                     },
//                 },
//             Mode::Flushing { done_reply_tx, } if tasks_count + bg_tasks_count == 0 => {
//                 log::debug!("Mode::Flushing: all tasks finished, responding Flushed and switching mode");
//                 if let Err(_send_error) = done_reply_tx.send(Flushed) {
//                     log::warn!("client canceled flush request");
//                 }
//                 continue;
//             },
//             Mode::Flushing { done_reply_tx, } if tasks_count == 0 => {
//                 log::debug!("FlushMode::InProgress: {} tasks left", bg_tasks_count);
//                 current_mode = Mode::Flushing { done_reply_tx, };
//                 select! {
//                     result = state.fused_flush_cache_rx.next() =>
//                         Event::FlushCache(result),
//                     result = fused_bg_tasks_rx.next() => match result {
//                         None =>
//                             unreachable!(),
//                         Some(task) => {
//                             bg_tasks_count -= 1;
//                             Event::Task(task)
//                         },
//                     },
//                 }
//             },
//             Mode::Flushing { done_reply_tx, } if bg_tasks_count == 0 => {
//                 log::debug!("FlushMode::InProgress: {} tasks left", tasks_count);
//                 current_mode = Mode::Flushing { done_reply_tx, };
//                 select! {
//                     result = state.fused_flush_cache_rx.next() =>
//                         Event::FlushCache(result),
//                     result = tasks.next() => match result {
//                         None =>
//                             unreachable!(),
//                         Some(task) => {
//                             tasks_count -= 1;
//                             Event::Task(task)
//                         },
//                     },
//                 }
//             },
//             Mode::Flushing { done_reply_tx, } => {
//                 log::debug!("FlushMode::InProgress: {} tasks left", tasks_count + bg_tasks_count);
//                 current_mode = Mode::Flushing { done_reply_tx, };
//                 select! {
//                     result = state.fused_flush_cache_rx.next() =>
//                         Event::FlushCache(result),
//                     result = fused_bg_tasks_rx.next() => match result {
//                         None =>
//                             unreachable!(),
//                         Some(task) => {
//                             bg_tasks_count -= 1;
//                             Event::Task(task)
//                         },
//                     },
//                     result = tasks.next() => match result {
//                         None =>
//                             unreachable!(),
//                         Some(task) => {
//                             tasks_count -= 1;
//                             Event::Task(task)
//                         },
//                     },
//                 }
//             },
//         };

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

//             Event::Request(None) => {
//                 log::info!("requests sink channel depleted: terminating");
//                 return Ok(());
//             },

//             Event::Request(Some(Request::Info(RequestInfo { reply_tx, }))) => {
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
//             },

//             Event::Request(Some(Request::Insert(request))) => {
//                 tasks.push(task::run_args(task::TaskArgs::InsertButcher(
//                     task::insert_butcher::Args {
//                         request,
//                         butcher_pid: state.butcher_pid.clone(),
//                     },
//                 )));
//                 tasks_count += 1;
//             },

//             Event::Request(Some(Request::Lookup(RequestLookup { key, reply_tx, }))) =>
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
//                 ),

//             Event::Request(Some(Request::LookupRange(RequestLookupRange { range, reply_tx, }))) => {
//                 let (key_values_tx, key_values_rx) =
//                     mpsc::channel(state.params.search_tree_params.iter_send_buffer);
//                 let lookup_range = LookupRange { key_values_rx, };
//                 if let Err(_send_error) = reply_tx.send(lookup_range) {
//                     log::warn!("client canceled lookup_range request");
//                 }
//                 tasks.push(task::run_args(task::TaskArgs::LookupRangeButcher(
//                     task::lookup_range_butcher::Args {
//                         range,
//                         key_values_tx,
//                         iter_items_pool: iter_items_pool.clone(),
//                         butcher_pid: state.butcher_pid.clone(),
//                     },
//                 )));
//                 tasks_count += 1;
//             },

//             Event::Request(Some(Request::Remove(request))) => {
//                 tasks.push(task::run_args(task::TaskArgs::RemoveButcher(
//                     task::remove_butcher::Args {
//                         request,
//                         butcher_pid: state.butcher_pid.clone(),
//                     },
//                 )));
//                 tasks_count += 1;
//             },

//             Event::Request(Some(Request::FlushAll(RequestFlush { reply_tx, }))) => {
//                 log::debug!("Request::FlushAll for butcher first");

//                 let request_ref = flush_requests.insert(FlushRequest {
//                     butcher_done: false,
//                     search_trees_pending_count: 0,
//                 });
//                 tasks.push(task::run_args(task::TaskArgs::FlushButcher(
//                     task::flush_butcher::Args {
//                         request_ref,
//                         butcher_pid: state.butcher_pid.clone(),
//                     },
//                 )));
//                 tasks_count += 1;
//                 current_mode = Mode::Flushing { done_reply_tx: reply_tx, };
//             },

//             Event::Task(Ok(task::TaskDone::InfoButcher(task::info_butcher::Done { request_ref, info, }))) |
//             Event::Task(Ok(task::TaskDone::InfoSearchTree(task::info_search_tree::Done { request_ref, info, }))) => {
//                 let info_request = info_requests.get_mut(request_ref).unwrap();
//                 assert!(info_request.pending_count > 0);
//                 info_request.pending_count -= 1;
//                 info_request.info_fold += info;
//                 if info_request.pending_count == 0 {
//                     let info_request = info_requests.remove(request_ref).unwrap();
//                     let info = info_request.info_fold;
//                     if let Err(_send_error) = info_request.reply_tx.send(info) {
//                         log::warn!("client canceled info request");
//                     }
//                 }
//             },

//             Event::Task(Ok(task::TaskDone::InsertButcher(task::insert_butcher::Done))) =>
//                 (),

//             Event::Task(Ok(task::TaskDone::LookupButcher(task::lookup_butcher::Done { request_ref, found, }))) => {
//                 let lookup_request = lookup_requests.get_mut(request_ref).unwrap();
//                 assert!(lookup_request.pending_count > 0);
//                 lookup_request.pending_count -= 1;
//                 match lookup_request.butcher_status {
//                     LookupRequestButcherStatus::NotReady => {
//                         lookup_request.butcher_status = LookupRequestButcherStatus::Done;
//                         if replace_fold_found(&lookup_request.found_fold, &found) {
//                             lookup_request.found_fold = found;
//                         }
//                     },
//                     LookupRequestButcherStatus::Done =>
//                         unreachable!(),
//                     LookupRequestButcherStatus::Invalidated =>
//                         log::debug!("invalidating butcher lookup reply"),
//                 }
//                 if lookup_request.pending_count == 0 {
//                     let lookup_request = lookup_requests.remove(request_ref).unwrap();
//                     tasks.push(task::run_args(task::TaskArgs::RetrieveValue(
//                         task::retrieve_value::Args {
//                             key: lookup_request.key,
//                             found_fold: lookup_request.found_fold,
//                             reply_tx: lookup_request.reply_tx,
//                             wheels_pid: state.wheels_pid.clone(),
//                         },
//                     )));
//                     tasks_count += 1;
//                 }
//             },

//             Event::Task(Ok(task::TaskDone::LookupSearchTree(task::lookup_search_tree::Done { request_ref, found, }))) => {
//                 let lookup_request = lookup_requests.get_mut(request_ref).unwrap();
//                 assert!(lookup_request.pending_count > 0);
//                 lookup_request.pending_count -= 1;
//                 if replace_fold_found(&lookup_request.found_fold, &found) {
//                     lookup_request.found_fold = found;
//                 }
//                 if lookup_request.pending_count == 0 {
//                     let lookup_request = lookup_requests.remove(request_ref).unwrap();
//                     tasks.push(task::run_args(task::TaskArgs::RetrieveValue(
//                         task::retrieve_value::Args {
//                             key: lookup_request.key,
//                             found_fold: lookup_request.found_fold,
//                             reply_tx: lookup_request.reply_tx,
//                             wheels_pid: state.wheels_pid.clone(),
//                         },
//                     )));
//                     tasks_count += 1;
//                 }
//             },

//             Event::Task(Ok(task::TaskDone::LookupRangeButcher(task::lookup_range_butcher::Done { range, key_values_tx, iter_items, }))) => {
//                 let mut merger_iters = merger_iters_pool.lend(Vec::new);
//                 merger_iters.clear();

//                 if search_trees.is_empty() {
//                     bg_tasks_push(task::TaskArgs::MergeLookupRange(
//                         task::merge_lookup_range::Args {
//                             range,
//                             key_values_tx,
//                             butcher_iter_items: iter_items,
//                             merger_iters,
//                             wheels_pid: state.wheels_pid.clone(),
//                             thread_pool: state.thread_pool.clone(),
//                         },
//                     ));
//                     bg_tasks_count += 1;
//                 } else {
//                     let lookup_range_request = LookupRangeRequest {
//                         range: range.clone(),
//                         key_values_tx,
//                         butcher_iter_items: iter_items,
//                         merger_iters,
//                         pending_count: search_trees.len(),
//                     };
//                     let request_ref = lookup_range_requests.insert(lookup_range_request);
//                     for (_search_tree_ref, search_tree_pid) in search_trees.iter() {
//                         tasks.push(task::run_args(task::TaskArgs::LookupRangeSearchTree(
//                             task::lookup_range_search_tree::Args {
//                                 range: range.clone(),
//                                 request_ref: request_ref.clone(),
//                                 search_tree_pid: search_tree_pid.clone(),
//                             },
//                         )));
//                         tasks_count += 1;
//                     }
//                 }
//             },

//             Event::Task(Ok(task::TaskDone::LookupRangeSearchTree(task::lookup_range_search_tree::Done { request_ref, items_iter, }))) => {
//                 let lookup_range_request = lookup_range_requests.get_mut(request_ref).unwrap();
//                 assert!(lookup_range_request.pending_count > 0);
//                 lookup_range_request.pending_count -= 1;
//                 lookup_range_request.merger_iters.push(items_iter.items_rx);
//                 if lookup_range_request.pending_count == 0 {
//                     let lookup_range_request = lookup_range_requests.remove(request_ref).unwrap();
//                     bg_tasks_push(task::TaskArgs::MergeLookupRange(
//                         task::merge_lookup_range::Args {
//                             range: lookup_range_request.range,
//                             key_values_tx: lookup_range_request.key_values_tx,
//                             butcher_iter_items: lookup_range_request.butcher_iter_items,
//                             merger_iters: lookup_range_request.merger_iters,
//                             wheels_pid: state.wheels_pid.clone(),
//                             thread_pool: state.thread_pool.clone(),
//                         },
//                     ));
//                     bg_tasks_count += 1;
//                 }
//             },

//             Event::Task(Ok(task::TaskDone::MergeLookupRange(task::merge_lookup_range::Done::MergeSuccess))) =>
//                 (),

//             Event::Task(Ok(task::TaskDone::MergeLookupRange(task::merge_lookup_range::Done::DeprecatedResults {
//                 modified_range,
//                 key_values_tx,
//             }))) => {
//                 log::debug!("task::TaskDone::MergeLookupRange deprecated results: retrying LOOKUP RANGE request");
//                 tasks.push(task::run_args(task::TaskArgs::LookupRangeButcher(
//                     task::lookup_range_butcher::Args {
//                         range: modified_range,
//                         key_values_tx,
//                         iter_items_pool: iter_items_pool.clone(),
//                         butcher_pid: state.butcher_pid.clone(),
//                     },
//                 )));
//                 tasks_count += 1;
//             },

//             Event::Task(Ok(task::TaskDone::RemoveButcher(task::remove_butcher::Done))) =>
//                 (),

//             Event::Task(Ok(task::TaskDone::FlushButcher(task::flush_butcher::Done { request_ref, }))) => {
//                 log::debug!("task::TaskDone::FlushButcher received, proceeding with {} search_trees", search_trees.len());
//                 assert!(matches!(current_mode, Mode::Flushing { .. }));
//                 let flush_request = flush_requests.get_mut(request_ref).unwrap();
//                 assert!(!flush_request.butcher_done);
//                 flush_request.butcher_done = true;

//                 if search_trees.is_empty() {
//                     log::debug!("task::TaskDone::FlushSearchTree finished, waiting for all tasks to be done");
//                     flush_requests.remove(request_ref).unwrap();
//                 } else {
//                     for (_search_tree_ref, search_tree_pid) in search_trees.iter() {
//                         tasks.push(task::run_args(task::TaskArgs::FlushSearchTree(
//                             task::flush_search_tree::Args {
//                                 request_ref: request_ref.clone(),
//                                 search_tree_pid: search_tree_pid.clone(),
//                             },
//                         )));
//                         tasks_count += 1;
//                         flush_request.search_trees_pending_count += 1;
//                     }
//                 }
//             },

//             Event::Task(Ok(task::TaskDone::FlushSearchTree(task::flush_search_tree::Done { request_ref, }))) => {
//                 assert!(matches!(current_mode, Mode::Flushing { .. }));
//                 let flush_request = flush_requests.get_mut(request_ref).unwrap();
//                 assert!(flush_request.butcher_done);
//                 assert!(flush_request.search_trees_pending_count > 0);
//                 flush_request.search_trees_pending_count -= 1;
//                 log::debug!("task::TaskDone::FlushSearchTree received ({} left)", flush_request.search_trees_pending_count);
//                 if flush_request.search_trees_pending_count == 0 {
//                     log::debug!("task::TaskDone::FlushSearchTree finished, waiting for all tasks to be done");
//                     flush_requests.remove(request_ref).unwrap();
//                 }
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

// fn launch_lookup_request<T, J>(
//     key: kv::Key,
//     reply_tx: oneshot::Sender<Option<kv::ValueCell<kv::Value>>>,
//     lookup_requests: &mut Set<LookupRequest>,
//     search_trees: &Set<search_tree::Pid>,
//     butcher_pid: &butcher::Pid,
//     mut tasks_push: T,
// )
// where T: FnMut(task::TaskArgs<J>),
//       J: edeltraud::Job,
// {
//     let request_ref = lookup_requests.insert(LookupRequest {
//         key: key.clone(),
//         reply_tx,
//         butcher_status: LookupRequestButcherStatus::NotReady,
//         pending_count: 1 + search_trees.len(),
//         found_fold: None,
//     });
//     tasks_push(task::TaskArgs::LookupButcher(
//         task::lookup_butcher::Args {
//             key: key.clone(),
//             request_ref: request_ref.clone(),
//             butcher_pid: butcher_pid.clone(),
//         },
//     ));
//     for (_search_tree_ref, search_tree_pid) in search_trees.iter() {
//         tasks_push(task::TaskArgs::LookupSearchTree(
//             task::lookup_search_tree::Args {
//                 key: key.clone(),
//                 request_ref: request_ref.clone(),
//                 search_tree_pid: search_tree_pid.clone(),
//             },
//         ));
//     }
// }

// #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
// struct SearchTreeRef {
//     items_count: usize,
//     search_tree_ref: Ref,
// }

// fn maybe_merge_search_trees<J>(
//     search_tree_refs: &mut bin_merger::BinMerger<SearchTreeRef>,
//     search_trees: &Set<search_tree::Pid>,
//     thread_pool: &edeltraud::Edeltraud<J>,
//     blocks_pool: &BytesPool,
//     merge_blocks_pool: &pool::Pool<Vec<storage::OwnedEntry>>,
//     merger_iters_pool: &pool::Pool<Vec<merger::KeyValuesIterRx>>,
//     wheels_pid: &wheels::Pid,
//     tree_block_size: usize,
//     tasks_count_limit: usize,
// )
//     -> Option<task::TaskArgs<J>>
// where J: edeltraud::Job
// {
//     let (search_tree_a_ref, search_tree_b_ref) = search_tree_refs.pop()?;
//     let search_tree_a_pid = search_trees.get(search_tree_a_ref.search_tree_ref).unwrap().clone();
//     let search_tree_b_pid = search_trees.get(search_tree_b_ref.search_tree_ref).unwrap().clone();
//     Some(task::TaskArgs::MergeSearchTrees(
//         task::merge_search_trees::Args {
//             search_tree_a_ref: search_tree_a_ref.search_tree_ref,
//             search_tree_b_ref: search_tree_b_ref.search_tree_ref,
//             search_tree_a_pid,
//             search_tree_b_pid,
//             thread_pool: thread_pool.clone(),
//             blocks_pool: blocks_pool.clone(),
//             merge_blocks_pool: merge_blocks_pool.clone(),
//             merger_iters_pool: merger_iters_pool.clone(),
//             wheels_pid: wheels_pid.clone(),
//             tree_block_size,
//             tasks_count_limit,
//         },
//     ))
// }
