use std::{
    mem,
};

use futures::{
    channel::{
        mpsc,
    },
    stream::{
        FuturesUnordered,
    },
    StreamExt,
};

use alloc_pool::{
    Unique,
    bytes::{
        Bytes,
    },
};

use crate::{
    kv,
    job,
    wheels,
    storage,
    core::{
        manager::{
            task::{
                WheelsPid,
            },
        },
        performer,
        search_ranges_merge,
        BlockRef,
        RequestLookupKind,
        RequestLookupKindSingle,
        RequestLookupKindRange,
    },
    LookupRange,
};

pub struct Args<J> where J: edeltraud::Job {
    pub ranges_merger: performer::LookupRangesMerger,
    pub lookup_context: RequestLookupKind,
    pub wheels_pid: wheels::Pid,
    pub thread_pool: edeltraud::Edeltraud<J>,
    pub iter_send_buffer: usize,
}

pub struct Done {
    pub lookup_range_sources: Unique<Vec<performer::LookupRangeSource>>,
}

#[derive(Debug)]
pub enum Error {
    WheelsGone,
    ThreadPoolGone,
    SearchRangesMerge(search_ranges_merge::Error),
    WheelNotFound {
        blockwheel_filename: wheels::WheelFilename,
    },
    ValueNotFoundFor { key: kv::Key, },
    ReadBlock(ero_blockwheel_fs::ReadBlockError),
    ValueDeserialize(storage::Error),
}

pub async fn run<J>(
    Args {
        ranges_merger,
        lookup_context,
        wheels_pid,
        thread_pool,
        iter_send_buffer,
    }: Args<J>,
)
    -> Result<Done, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    inner_run(
        ranges_merger,
        lookup_context,
        WheelsPid::Regular(wheels_pid),
        thread_pool,
        iter_send_buffer,
    ).await
}

async fn inner_run<J>(
    ranges_merger: performer::LookupRangesMerger,
    lookup_context: RequestLookupKind,
    wheels_pid: WheelsPid,
    thread_pool: edeltraud::Edeltraud<J>,
    iter_send_buffer: usize,
)
    -> Result<Done, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    enum Task<J> where J: edeltraud::Job + From<job::Job> {
        Job(edeltraud::Handle<J::Output>),
        ValueLoad {
            key: kv::Key,
            version: u64,
            wheels_pid: WheelsPid,
            block_ref: BlockRef,
        },
        BlockLoad {
            async_token: search_ranges_merge::AsyncToken<performer::LookupRangeSource>,
            wheels_pid: WheelsPid,
            block_ref: BlockRef,
        },
    }

    enum TaskOutput {
        Job(JobDone),
        ValueLoad {
            kv_pair: kv::KeyValuePair<kv::Value>,
        },
        BlockLoad {
            async_token: search_ranges_merge::AsyncToken<performer::LookupRangeSource>,
            block_bytes: Bytes,
        },
    }

    impl<J> Task<J>
    where J: edeltraud::Job + From<job::Job>,
          J::Output: From<job::JobOutput>,
          job::JobOutput: From<J::Output>,
    {
        async fn run(self) -> Result<TaskOutput, Error> {
            match self {
                Task::Job(job_handle) => {
                    let job_output = job_handle.await
                        .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
                    let job_output: job::JobOutput = job_output.into();
                    let job::ManagerTaskLookupRangeMergeDone(job_done) = job_output.into();
                    Ok(TaskOutput::Job(job_done?))
                },
                Task::ValueLoad { key, version, mut wheels_pid, block_ref, } => {
                    let mut wheel_ref = wheels_pid.get(block_ref.blockwheel_filename.clone()).await
                        .map_err(|ero::NoProcError| Error::WheelsGone)?
                        .ok_or_else(|| Error::WheelNotFound {
                            blockwheel_filename: block_ref.blockwheel_filename.clone(),
                        })?;
                    let block_bytes = match wheel_ref.blockwheel_pid.read_block(block_ref.block_id.clone()).await {
                        Ok(block_bytes) =>
                            block_bytes,
                        Err(ero_blockwheel_fs::ReadBlockError::NotFound) =>
                            return Err(Error::ValueNotFoundFor { key, }),
                        Err(error) =>
                            return Err(Error::ReadBlock(error)),
                    };
                    let value_bytes = storage::value_block_deserialize(&block_bytes)
                        .map_err(Error::ValueDeserialize)?;
                    Ok(TaskOutput::ValueLoad {
                        kv_pair: kv::KeyValuePair {
                            key,
                            value_cell: kv::ValueCell {
                                version,
                                cell: kv::Cell::Value(kv::Value { value_bytes, }),
                            },
                        },
                    })
                },
                Task::BlockLoad { async_token, mut wheels_pid, block_ref, } => {
                    let mut wheel_ref = wheels_pid.get(block_ref.blockwheel_filename.clone()).await
                        .map_err(|ero::NoProcError| Error::WheelsGone)?
                        .ok_or_else(|| Error::WheelNotFound {
                            blockwheel_filename: block_ref.blockwheel_filename.clone(),
                        })?;
                    let block_bytes = match wheel_ref.blockwheel_pid.read_block(block_ref.block_id.clone()).await {
                        Ok(block_bytes) =>
                            block_bytes,
                        Err(error) =>
                            return Err(Error::ReadBlock(error)),
                    };
                    Ok(TaskOutput::BlockLoad { async_token, block_bytes, })
                },
            }
        }
    }

    let mut tasks = FuturesUnordered::new();

    enum ActiveItem {
        ReadyToSend {
            item: kv::KeyValuePair<kv::Value>,
        },
        PendingValueLoad,
        Finish,
    }

    let mut maybe_active_item: Option<ActiveItem> = None;

    enum TxState<O, S> {
        Oneshot { reply_tx: O, },
        Stream { key_values_tx: S, },
        Sending,
    }

    let mut tx_state = match lookup_context {
        RequestLookupKind::Single(RequestLookupKindSingle { reply_tx, }) =>
            TxState::Oneshot { reply_tx, },
        RequestLookupKind::Range(RequestLookupKindRange { reply_tx, }) => {
            let (key_values_tx, key_values_rx) =
                mpsc::channel(iter_send_buffer);
            if let Err(_send_error) = reply_tx.send(LookupRange { key_values_rx, }) {
                log::warn!("client canceled lookup_range request");
                return Ok(Done {
                    lookup_range_sources: ranges_merger.source.decompose(),
                });
            }
            TxState::Stream { key_values_tx, }
        },
    };

    enum MergerState {
        Ready { job_args: JobArgs, },
        InProgress,
        Finished { lookup_range_sources: Unique<Vec<performer::LookupRangeSource>>, },
    }

    let mut merger_state = MergerState::Ready {
        job_args: JobArgs {
            env: Env {
                incoming: Incoming::default(),
                outgoing: Outgoing::default(),
            },
            kont: Kont::Start {
                merger: ranges_merger.source,
            },
        },
    };

    let mut incoming = Incoming::default();

    loop {
        match &tx_state {
            TxState::Sending =>
                (),
            TxState::Oneshot { .. } | TxState::Stream { .. } =>
                if let None = &maybe_active_item {
                    if let MergerState::Finished { lookup_range_sources } = merger_state {
                        return Ok(Done { lookup_range_sources, });
                    }
                },
        }

        enum MergerAction<A, S> {
            Run(A),
            KeepState(S),
        }

        let merger_action = match merger_state {
            MergerState::Ready { job_args: job_args @ JobArgs { kont: Kont::Start { .. }, .. }, } =>
                MergerAction::Run(job_args),
            MergerState::Ready { job_args, } if !incoming.is_empty() || maybe_active_item.is_none() =>
                MergerAction::Run(job_args),
            other =>
                MergerAction::KeepState(other),
        };

        merger_state = match merger_action {
            MergerAction::Run(mut job_args) => {
                job_args.env.incoming.transfill_from(&mut incoming);
                let job = job::Job::ManagerTaskLookupRangeMerge(job_args);
                let job_handle = thread_pool.spawn_handle(job)
                    .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
                tasks.push(Task::<J>::Job(job_handle).run());
                MergerState::InProgress
            },
            MergerAction::KeepState(state) =>
                state,
        };

        match tasks.next().await.unwrap()? {

            TaskOutput::Job(JobDone::AwaitRetrieveBlockTasks { mut env, next, }) => {
                for RetrieveBlockTask { block_ref, async_token, } in env.outgoing.retrieve_block_tasks.drain(..) {
                    let wheels_pid = wheels_pid.clone();
                    tasks.push(Task::BlockLoad { async_token, wheels_pid, block_ref, }.run());
                }
                let next_merger_state = MergerState::Ready {
                    job_args: JobArgs { env, kont: Kont::ProceedAwaitBlocks { next, }, },
                };
                match mem::replace(&mut merger_state, next_merger_state) {
                    MergerState::InProgress =>
                        (),
                    MergerState::Ready { .. } | MergerState::Finished { .. } =>
                        unreachable!(),
                }
            },

            TaskOutput::Job(JobDone::ItemArrived { item, env, next, }) => {
                assert!(maybe_active_item.is_none());
                match item {
                    kv::KeyValuePair {
                        key,
                        value_cell: kv::ValueCell {
                            version,
                            cell: kv::Cell::Value(storage::OwnedValueBlockRef::Inline(value)),
                        },
                    } =>
                        maybe_active_item = Some(ActiveItem::ReadyToSend {
                            item: kv::KeyValuePair { key, value_cell: kv::ValueCell { version, cell: kv::Cell::Value(value), }, },
                        }),
                    kv::KeyValuePair { key, value_cell: kv::ValueCell { version, cell: kv::Cell::Tombstone, }, } =>
                        maybe_active_item = Some(ActiveItem::ReadyToSend {
                            item: kv::KeyValuePair { key, value_cell: kv::ValueCell { version, cell: kv::Cell::Tombstone, }, },
                        }),
                    kv::KeyValuePair {
                        key,
                        value_cell: kv::ValueCell {
                            version,
                            cell: kv::Cell::Value(storage::OwnedValueBlockRef::Ref(block_ref)),
                        },
                    } => {
                        let wheels_pid = wheels_pid.clone();
                        tasks.push(Task::ValueLoad { key, version, wheels_pid, block_ref, }.run());
                        maybe_active_item = Some(ActiveItem::PendingValueLoad);
                    },
                }
                let next_merger_state = MergerState::Ready {
                    job_args: JobArgs { env, kont: Kont::ProceedItem { next, }, },
                };
                match mem::replace(&mut merger_state, next_merger_state) {
                    MergerState::InProgress =>
                        (),
                    MergerState::Ready { .. } | MergerState::Finished { .. } =>
                        unreachable!(),
                }
            },

            TaskOutput::Job(JobDone::Finished { lookup_range_sources, }) => {
                assert!(incoming.is_empty());
                match mem::replace(&mut merger_state, MergerState::Finished { lookup_range_sources, }) {
                    MergerState::InProgress =>
                        (),
                    MergerState::Ready { .. } | MergerState::Finished { .. } =>
                        unreachable!(),
                }
            },

            TaskOutput::ValueLoad { kv_pair, } =>
                match mem::replace(&mut maybe_active_item, Some(ActiveItem::ReadyToSend { item: kv_pair, })) {
                    Some(ActiveItem::PendingValueLoad) =>
                        (),
                    _ =>
                        unreachable!(),
                },

            TaskOutput::BlockLoad { async_token, block_bytes, } =>
                incoming.received_block_tasks.push(ReceivedBlockTask { async_token, block_bytes, }),

        }
    }
}

pub struct JobArgs {
    env: Env,
    kont: Kont,
}

pub struct Env {
    incoming: Incoming,
    outgoing: Outgoing,
}

#[derive(Default)]
struct Incoming {
    received_block_tasks: Vec<ReceivedBlockTask>,
}

impl Incoming {
    fn is_empty(&self) -> bool {
        self.received_block_tasks.is_empty()
    }

    pub fn transfill_from(&mut self, from: &mut Self) {
        self.received_block_tasks.extend(from.received_block_tasks.drain(..));
    }
}

struct ReceivedBlockTask {
    async_token: search_ranges_merge::AsyncToken<performer::LookupRangeSource>,
    block_bytes: Bytes,
}

#[derive(Default)]
struct Outgoing {
    retrieve_block_tasks: Vec<RetrieveBlockTask>,
}

impl Outgoing {
    fn is_empty(&self) -> bool {
        self.retrieve_block_tasks.is_empty()
    }
}

struct RetrieveBlockTask {
    block_ref: BlockRef,
    async_token: search_ranges_merge::AsyncToken<performer::LookupRangeSource>,
}

pub enum Kont {
    Start {
        merger: search_ranges_merge::RangesMergeCpsInit<Unique<Vec<performer::LookupRangeSource>>, performer::LookupRangeSource>,
    },
    ProceedAwaitBlocks {
        next: search_ranges_merge::KontAwaitBlocksNext<Unique<Vec<performer::LookupRangeSource>>, performer::LookupRangeSource>,
    },
    ProceedItem {
        next: search_ranges_merge::KontEmitItemNext<Unique<Vec<performer::LookupRangeSource>>, performer::LookupRangeSource>,
    },
}

pub enum JobDone {
    AwaitRetrieveBlockTasks {
        env: Env,
        next: search_ranges_merge::KontAwaitBlocksNext<Unique<Vec<performer::LookupRangeSource>>, performer::LookupRangeSource>,
    },
    ItemArrived {
        item: kv::KeyValuePair<storage::OwnedValueBlockRef>,
        env: Env,
        next: search_ranges_merge::KontEmitItemNext<Unique<Vec<performer::LookupRangeSource>>, performer::LookupRangeSource>,
    },
    Finished {
        lookup_range_sources: Unique<Vec<performer::LookupRangeSource>>,
    },
}

pub type Output = Result<JobDone, Error>;

pub fn job(JobArgs { mut env, mut kont, }: JobArgs) -> Output {
    loop {
        let mut merger_kont = match kont {
            Kont::Start { merger, } => {
                search_ranges_merge::RangesMergeCps::from(merger)
                    .step()
                    .map_err(Error::SearchRangesMerge)?
            },
            Kont::ProceedAwaitBlocks { next, } =>
                if let Some(ReceivedBlockTask { async_token, block_bytes, }) = env.incoming.received_block_tasks.pop() {
                    next.block_arrived(async_token, block_bytes)
                        .map_err(Error::SearchRangesMerge)?
                } else {
                    return Ok(JobDone::AwaitRetrieveBlockTasks { env, next, });
                },
            Kont::ProceedItem { next, } =>
                next.proceed().map_err(Error::SearchRangesMerge)?,
        };

        loop {
            match merger_kont {
                search_ranges_merge::Kont::RequireBlockAsync(
                    search_ranges_merge::KontRequireBlockAsync { block_ref, async_token, next, },
                ) => {
                    env.outgoing.retrieve_block_tasks.push(RetrieveBlockTask { block_ref, async_token, });
                    merger_kont = next.scheduled()
                        .map_err(Error::SearchRangesMerge)?;
                },
                search_ranges_merge::Kont::AwaitBlocks(search_ranges_merge::KontAwaitBlocks { next, }) => {
                    kont = Kont::ProceedAwaitBlocks { next, };
                    break;
                },
                search_ranges_merge::Kont::EmitDeprecated(search_ranges_merge::KontEmitDeprecated { next, .. }) => {
                    merger_kont = next.proceed()
                        .map_err(Error::SearchRangesMerge)?;
                },
                search_ranges_merge::Kont::EmitItem(
                    search_ranges_merge::KontEmitItem { item, next, },
                ) => {
                    return Ok(JobDone::ItemArrived { item, env, next, });
                },
                search_ranges_merge::Kont::Finished(search_ranges_merge::KontFinished { sources, }) => {
                    assert!(env.outgoing.is_empty());
                    assert!(env.incoming.is_empty());
                    return Ok(JobDone::Finished { lookup_range_sources: sources, });
                },
            }
        }
    }
}
