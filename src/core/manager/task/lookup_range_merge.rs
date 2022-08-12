use futures::{
    channel::{
        mpsc,
    },
    stream::{
        FuturesUnordered,
    },
    SinkExt,
    StreamExt,
};

use alloc_pool::{
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
                Wheels,
            },
        },
        performer,
        search_ranges_merge,
        BlockRef,
        RequestLookupKind,
        RequestLookupKindSingle,
        RequestLookupKindRange,
        SearchRangesMergeCps,
        SearchRangesMergeBlockNext,
        SearchRangesMergeItemNext,
    },
    LookupRange,
    KeyValueStreamItem,
};

#[cfg(test)]
mod tests;

pub struct Args<J> where J: edeltraud::Job {
    pub ranges_merger: performer::LookupRangesMerger,
    pub lookup_context: RequestLookupKind,
    pub wheels: wheels::Wheels,
    pub thread_pool: edeltraud::Edeltraud<J>,
    pub iter_send_buffer: usize,
}

pub struct Done {
    pub access_token: performer::AccessToken,
}

#[derive(Debug)]
pub enum Error {
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
        wheels,
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
        Wheels::Regular(wheels),
        thread_pool,
        iter_send_buffer,
    ).await
}

async fn inner_run<J>(
    ranges_merger: performer::LookupRangesMerger,
    lookup_context: RequestLookupKind,
    wheels: Wheels,
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
            wheels: Wheels,
            block_ref: BlockRef,
        },
        BlockLoad {
            async_token: search_ranges_merge::AsyncToken<performer::LookupRangeSource>,
            wheels: Wheels,
            block_ref: BlockRef,
        },
        TxItem {
            item: KeyValueStreamItem,
            key_values_tx: mpsc::Sender<KeyValueStreamItem>,
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
        TxItemSent {
            key_values_tx: mpsc::Sender<KeyValueStreamItem>,
        },
        TxDropped,
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
                Task::ValueLoad { key, version, mut wheels, block_ref, } => {
                    let mut wheel_ref = wheels.get(block_ref.blockwheel_filename.clone())
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
                Task::BlockLoad { async_token, mut wheels, block_ref, } => {
                    let mut wheel_ref = wheels.get(block_ref.blockwheel_filename.clone())
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
                Task::TxItem { item, mut key_values_tx, } =>
                    if let Err(_send_error) = key_values_tx.send(item).await {
                        log::debug!("client dropped iterator in TxItem task");
                        Ok(TaskOutput::TxDropped)
                    } else {
                        Ok(TaskOutput::TxItemSent { key_values_tx, })
                    },
            }
        }
    }

    let mut tasks = FuturesUnordered::new();

    enum ActiveItem {
        ReadyToSend {
            item: KeyValueStreamItem,
        },
        PendingValueLoad,
    }

    let mut maybe_active_item: Option<ActiveItem> = None;

    enum TxState<O, S> {
        Idle(TxStateIdle<O, S>),
        Sending,
    }

    enum TxStateIdle<O, S> {
        Oneshot { maybe_reply_tx: Option<O>, },
        Stream { key_values_tx: S, },
    }

    let mut tx_state = match lookup_context {
        RequestLookupKind::Single(RequestLookupKindSingle { reply_tx, }) =>
            TxState::Idle(TxStateIdle::Oneshot { maybe_reply_tx: Some(reply_tx), }),
        RequestLookupKind::Range(RequestLookupKindRange { reply_tx, }) => {
            let (key_values_tx, key_values_rx) =
                mpsc::channel(iter_send_buffer);
            if let Err(_send_error) = reply_tx.send(LookupRange { key_values_rx, }) {
                log::warn!("client canceled lookup_range request");
                return Ok(Done {
                    access_token: ranges_merger.token,
                });
            }
            TxState::Idle(TxStateIdle::Stream { key_values_tx, })
        },
    };

    enum MergerState {
        Ready { job_args: JobArgs, },
        InProgress,
        Finished,
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
        if let TxState::Idle(..) = &tx_state {
            if let None = &maybe_active_item {
                if let MergerState::Finished = merger_state {
                    return Ok(Done { access_token: ranges_merger.token, });
                }
            }
        }

        match tx_state {
            TxState::Idle(tx_state_idle) =>
                match maybe_active_item.take() {
                    None =>
                        tx_state = TxState::Idle(tx_state_idle),
                    Some(ActiveItem::ReadyToSend { item, }) =>
                        match tx_state_idle {
                            TxStateIdle::Oneshot { maybe_reply_tx, } => {
                                let send_result = match (maybe_reply_tx, item) {
                                    (Some(reply_tx), KeyValueStreamItem::KeyValue(kv::KeyValuePair { value_cell, .. })) =>
                                        reply_tx.send(Some(value_cell)),
                                    (Some(reply_tx), KeyValueStreamItem::NoMore) =>
                                        reply_tx.send(None),
                                    (None, KeyValueStreamItem::KeyValue(..)) =>
                                        panic!("something went wrong: another item arrived for single lookup"),
                                    (None, KeyValueStreamItem::NoMore) =>
                                        Ok(()),
                                };
                                if let Err(_send_error) = send_result {
                                    log::warn!("client canceled lookup request");
                                }
                                tx_state = TxState::Idle(TxStateIdle::Oneshot { maybe_reply_tx: None, });
                                continue;
                            },
                            TxStateIdle::Stream { key_values_tx, } => {
                                tasks.push(Task::TxItem { item, key_values_tx, }.run());
                                tx_state = TxState::Sending;
                            },
                        },
                    Some(ActiveItem::PendingValueLoad) => {
                        tx_state = TxState::Idle(tx_state_idle);
                        maybe_active_item = Some(ActiveItem::PendingValueLoad);
                    },
                },
            TxState::Sending =>
                tx_state = TxState::Sending,
        }

        enum MergerAction<A, S> {
            Run(A),
            KeepState(S),
        }

        let merger_action = match merger_state {
            MergerState::Ready { job_args: job_args @ JobArgs { kont: Kont::Start { .. }, .. }, } =>
                MergerAction::Run(job_args),
            MergerState::Ready { job_args: job_args @ JobArgs { kont: Kont::ProceedAwaitBlocks { .. }, .. }, } if !incoming.is_empty() =>
                MergerAction::Run(job_args),
            MergerState::Ready { job_args: job_args @ JobArgs { kont: Kont::ProceedItem { .. }, .. }, } if maybe_active_item.is_none() =>
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
                    let wheels = wheels.clone();
                    tasks.push(Task::BlockLoad { async_token, wheels, block_ref, }.run());
                }
                merger_state = match merger_state {
                    MergerState::InProgress =>
                        MergerState::Ready {
                            job_args: JobArgs { env, kont: Kont::ProceedAwaitBlocks { next, }, },
                        },
                    MergerState::Ready { .. } | MergerState::Finished { .. } =>
                        unreachable!(),
                };
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
                            item: KeyValueStreamItem::KeyValue(
                                kv::KeyValuePair {
                                    key,
                                    value_cell: kv::ValueCell {
                                        version,
                                        cell: kv::Cell::Value(value),
                                    },
                                },
                            ),
                        }),
                    kv::KeyValuePair { key, value_cell: kv::ValueCell { version, cell: kv::Cell::Tombstone, }, } =>
                        maybe_active_item = Some(ActiveItem::ReadyToSend {
                            item: KeyValueStreamItem::KeyValue(
                                kv::KeyValuePair {
                                    key,
                                    value_cell: kv::ValueCell {
                                        version,
                                        cell: kv::Cell::Tombstone,
                                    },
                                },
                            ),
                        }),
                    kv::KeyValuePair {
                        key,
                        value_cell: kv::ValueCell {
                            version,
                            cell: kv::Cell::Value(storage::OwnedValueBlockRef::Ref(block_ref)),
                        },
                    } => {
                        let wheels = wheels.clone();
                        tasks.push(Task::ValueLoad { key, version, wheels, block_ref, }.run());
                        maybe_active_item = Some(ActiveItem::PendingValueLoad);
                    },
                }
                merger_state = match merger_state {
                    MergerState::InProgress =>
                        MergerState::Ready {
                            job_args: JobArgs { env, kont: Kont::ProceedItem { next, }, },
                        },
                    MergerState::Ready { .. } | MergerState::Finished { .. } =>
                        unreachable!(),
                };
            },

            TaskOutput::Job(JobDone::Finished) => {
                assert!(incoming.is_empty());
                merger_state = match merger_state {
                    MergerState::InProgress =>
                        MergerState::Finished,
                    MergerState::Ready { .. } | MergerState::Finished { .. } =>
                        unreachable!(),
                };
                maybe_active_item = match maybe_active_item {
                    None =>
                        Some(ActiveItem::ReadyToSend { item: KeyValueStreamItem::NoMore, }),
                    _ =>
                        unreachable!(),
                };
            },

            TaskOutput::ValueLoad { kv_pair, } =>
                maybe_active_item = match maybe_active_item {
                    Some(ActiveItem::PendingValueLoad) =>
                        Some(ActiveItem::ReadyToSend { item: KeyValueStreamItem::KeyValue(kv_pair), }),
                    _ =>
                        unreachable!(),
                },

            TaskOutput::BlockLoad { async_token, block_bytes, } =>
                incoming.received_block_tasks.push(ReceivedBlockTask { async_token, block_bytes, }),

            TaskOutput::TxItemSent { key_values_tx, } =>
                tx_state = match tx_state {
                    TxState::Sending =>
                        TxState::Idle(TxStateIdle::Stream { key_values_tx, }),
                    TxState::Idle(..) =>
                        unreachable!(),
                },

            TaskOutput::TxDropped =>
                return Ok(Done { access_token: ranges_merger.token, }),

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
        merger: SearchRangesMergeCps,
    },
    ProceedAwaitBlocks {
        next: SearchRangesMergeBlockNext,
    },
    ProceedItem {
        next: SearchRangesMergeItemNext,
    },
}

pub enum JobDone {
    AwaitRetrieveBlockTasks {
        env: Env,
        next: SearchRangesMergeBlockNext,
    },
    ItemArrived {
        item: kv::KeyValuePair<storage::OwnedValueBlockRef>,
        env: Env,
        next: SearchRangesMergeItemNext,
    },
    Finished,
}

pub type Output = Result<JobDone, Error>;

pub fn job(JobArgs { mut env, mut kont, }: JobArgs) -> Output {
    loop {
        let mut merger_kont = match kont {
            Kont::Start { merger, } => {
                merger.step()
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
                search_ranges_merge::Kont::BlockFinished(search_ranges_merge::KontBlockFinished { next, .. }) => {
                    merger_kont = next.proceed()
                        .map_err(Error::SearchRangesMerge)?;
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
                search_ranges_merge::Kont::Finished => {
                    assert!(env.outgoing.is_empty());
                    assert!(env.incoming.is_empty());
                    return Ok(JobDone::Finished);
                },
            }
        }
    }
}
