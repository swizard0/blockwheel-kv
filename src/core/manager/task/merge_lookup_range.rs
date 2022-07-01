use std::{
    mem,
    ops::{
        Bound,
    },
    collections::{
        VecDeque,
    },
};

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

use o1::set::{
    Set,
    Ref,
};

use alloc_pool::{
    bytes::{
        Bytes,
    },
    Shared,
    Unique,
};

use ero_blockwheel_fs as blockwheel;

use crate::{
    kv,
    job,
    wheels,
    storage,
    core::{
        merger,
        KeyValueRef,
        SearchRangeBounds,
    },
    KeyValueStreamItem,
};

pub mod merge_cps;

#[cfg(test)]
mod tests;

pub struct Args<J> where J: edeltraud::Job {
    pub range: SearchRangeBounds,
    pub key_values_tx: mpsc::Sender<KeyValueStreamItem>,
    pub butcher_iter_items: Shared<Vec<kv::KeyValuePair<kv::Value>>>,
    pub merger_iters: Unique<Vec<merger::KeyValuesIterRx>>,
    pub wheels_pid: wheels::Pid,
    pub thread_pool: edeltraud::Edeltraud<J>,
}

pub enum Done {
    MergeSuccess,
    DeprecatedResults {
        modified_range: SearchRangeBounds,
        key_values_tx: mpsc::Sender<KeyValueStreamItem>,
    },
}

#[derive(Debug)]
pub enum Error {
    WheelsGone,
    WheelNotFound {
        blockwheel_filename: wheels::WheelFilename,
    },
    ReadBlock(blockwheel::ReadBlockError),
    ValueDeserialize(storage::Error),
    ThreadPoolGone,
    BackendIterPeerLost,
}

pub async fn run<J>(
    Args { range, key_values_tx, butcher_iter_items, merger_iters, wheels_pid, thread_pool, }: Args<J>,
)
    -> Result<Done, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    enum QueueEntry {
        ReadyToSend {
            item: kv::KeyValuePair<kv::Value>,
        },
        PendingValueLoad {
            key: kv::Key,
            version: u64,
        },
        Finish,
    }

    let mut queue = VecDeque::new();
    let mut queue_entries = Set::new();

    enum MergeCpsState {
        Ready {
            job_args: merge_cps::JobArgs,
        },
        InProgress,
        AwaitIters {
            env: merge_cps::Env,
            kont_await_ready: merge_cps::KontAwaitReady,
            pending_count: usize,
        },
        Finished,
    }

    let mut merge_cps_state = MergeCpsState::Ready {
        job_args: merge_cps::JobArgs {
            env: merge_cps::Env::default(),
            kont: merge_cps::Kont::Start { butcher_iter_items, merger_iters, },
        },
    };

    enum TxState {
        Idle {
            key_values_tx: mpsc::Sender<KeyValueStreamItem>,
        },
        Sending,
    }

    let mut tx_state = TxState::Idle { key_values_tx, };

    let mut pending_deprecated_lookup_key = None;

    let mut tasks = FuturesUnordered::new();
    loop {
        if let (MergeCpsState::Finished, TxState::Idle { .. }, true) = (&merge_cps_state, &tx_state, queue.is_empty()) {
            return Ok(Done::MergeSuccess);
        }

        match merge_cps_state {
            MergeCpsState::Ready { job_args, } if !queue.is_empty() =>
                merge_cps_state = MergeCpsState::Ready { job_args, },
            MergeCpsState::Ready { job_args, } => {
                tasks.push(Task::MergeCps { job_args, thread_pool: thread_pool.clone(), }.make());
                merge_cps_state = MergeCpsState::InProgress;
            },
            MergeCpsState::InProgress =>
                merge_cps_state = MergeCpsState::InProgress,
            MergeCpsState::AwaitIters { env, kont_await_ready, pending_count, } if pending_count == 0 => {
                let job_args = merge_cps::JobArgs {
                    env,
                    kont: merge_cps::Kont::AwaitReady(kont_await_ready),
                };
                merge_cps_state = MergeCpsState::Ready { job_args, };
                continue;
            },
            MergeCpsState::AwaitIters { env, kont_await_ready, pending_count, } =>
                merge_cps_state = MergeCpsState::AwaitIters { env, kont_await_ready, pending_count, },
            MergeCpsState::Finished =>
                merge_cps_state = MergeCpsState::Finished,
        }

        match tx_state {
            TxState::Idle { key_values_tx, } => {
                if let Some(key) = pending_deprecated_lookup_key.take() {
                    return Ok(Done::DeprecatedResults {
                        modified_range: SearchRangeBounds {
                            range_from: Bound::Included(key),
                            ..range
                        },
                        key_values_tx,
                    });
                }

                if let Some(queue_tx_ref) = queue.pop_front() {
                    match queue_entries.get(queue_tx_ref) {
                        None =>
                            unreachable!(),
                        Some(QueueEntry::ReadyToSend { .. }) =>
                            match queue_entries.remove(queue_tx_ref) {
                                Some(QueueEntry::ReadyToSend { item, }) => {
                                    tasks.push(Task::TxItem { item: KeyValueStreamItem::KeyValue(item), key_values_tx, }.make());
                                    tx_state = TxState::Sending;
                                    continue;
                                },
                                _ =>
                                    unreachable!(),
                            },
                        Some(QueueEntry::PendingValueLoad { .. }) =>
                            queue.push_front(queue_tx_ref),
                        Some(QueueEntry::Finish) => {
                            queue_entries.remove(queue_tx_ref).unwrap();
                            tasks.push(Task::TxItem { item: KeyValueStreamItem::NoMore, key_values_tx, }.make());
                            tx_state = TxState::Sending;
                            continue;
                        },
                    }
                }
                tx_state = TxState::Idle { key_values_tx, };
            },
            TxState::Sending =>
                tx_state = TxState::Sending,
        }

        let task_done = tasks.next().await;
        match task_done {

            None =>
                unreachable!(),

            Some(Ok(TaskOutput::MergeCps { job_done: merge_cps::JobDone { mut env, done, }, })) => {
                for key_value_ref in env.ready_items.drain(..) {
                    let queue_entry_ref = match key_value_ref {
                        kv::KeyValuePair {
                            key,
                            value_cell: kv::ValueCell {
                                version,
                                cell: kv::Cell::Value(storage::OwnedValueBlockRef::Inline(value)),
                            },
                        } =>
                            queue_entries.insert(QueueEntry::ReadyToSend {
                                item: kv::KeyValuePair { key, value_cell: kv::ValueCell { version, cell: kv::Cell::Value(value), }, },
                            }),
                        kv::KeyValuePair { key, value_cell: kv::ValueCell { version, cell: kv::Cell::Tombstone, }, } =>
                            queue_entries.insert(QueueEntry::ReadyToSend {
                                item: kv::KeyValuePair { key, value_cell: kv::ValueCell { version, cell: kv::Cell::Tombstone, }, },
                            }),
                        kv::KeyValuePair {
                            key,
                            value_cell: kv::ValueCell {
                                version,
                                cell: kv::Cell::Value(storage::OwnedValueBlockRef::Ref(block_ref)),
                            },
                        } => {
                            let queue_entry_ref =
                                queue_entries.insert(QueueEntry::PendingValueLoad { key, version, });
                            tasks.push(Task::ValueLoad { queue_entry_ref, block_ref, wheels_pid: wheels_pid.clone(), }.make());
                            queue_entry_ref
                        },
                    };
                    queue.push_back(queue_entry_ref);
                }

                let mut pending_count = 0;
                for await_search_tree_iter in env.await_search_tree_iters.drain(..) {
                    tasks.push(Task::AwaitIterRx { await_iter_rx: await_search_tree_iter, }.make());
                    pending_count += 1;
                }

                match merge_cps_state {
                    MergeCpsState::InProgress =>
                        match done {
                            merge_cps::Done::AwaitIters { next, } =>
                                merge_cps_state = MergeCpsState::AwaitIters {
                                    env,
                                    kont_await_ready: merge_cps::KontAwaitReady { next, },
                                    pending_count,
                                },
                            merge_cps::Done::Finish => {
                                assert_eq!(pending_count, 0);
                                merge_cps_state = MergeCpsState::Finished;
                                let queue_entry_ref = queue_entries.insert(QueueEntry::Finish);
                                queue.push_back(queue_entry_ref);
                            },
                        },
                    MergeCpsState::Ready { .. } |
                    MergeCpsState::AwaitIters { .. } |
                    MergeCpsState::Finished =>
                        unreachable!(),
                }
            },

            Some(Ok(TaskOutput::ValueLoad { queue_entry_ref, value_bytes, })) => {
                let queue_entry = queue_entries.get_mut(queue_entry_ref).unwrap();
                match mem::replace(queue_entry, QueueEntry::Finish) {
                    QueueEntry::PendingValueLoad { key, version, } =>
                        *queue_entry = QueueEntry::ReadyToSend {
                            item: kv::KeyValuePair {
                                key,
                                value_cell: kv::ValueCell {
                                    version,
                                    cell: kv::Cell::Value(kv::Value { value_bytes, }),
                                },
                            },
                        },
                    QueueEntry::ReadyToSend { .. } | QueueEntry::Finish =>
                        unreachable!(),
                }
            },

            Some(Ok(TaskOutput::ValueNotFound { queue_entry_ref, })) =>
                if let Some(QueueEntry::PendingValueLoad { key, .. }) = queue_entries.remove(queue_entry_ref) {
                    pending_deprecated_lookup_key = Some(key);
                } else {
                    unreachable!();
                },

            Some(Ok(TaskOutput::TxItemSent { key_values_tx, })) =>
                tx_state = match tx_state {
                    TxState::Sending =>
                        TxState::Idle { key_values_tx, },
                    TxState::Idle { .. } =>
                        unreachable!(),
                },

            Some(Ok(TaskOutput::TxDropped)) =>
                return Ok(Done::MergeSuccess),

            Some(Ok(TaskOutput::AwaitIterRxItem { await_iter_rx, item, })) =>
                if let MergeCpsState::AwaitIters { env, pending_count, .. } = &mut merge_cps_state {
                    assert!(*pending_count > 0);
                    env.await_outcomes.push(merge_cps::AwaitOutcome {
                        iter: await_iter_rx,
                        item,
                    });
                    *pending_count -= 1;
                } else {
                    unreachable!();
                },

            Some(Err(error)) =>
                return Err(error),

        }
    }
}

enum Task<J> where J: edeltraud::Job {
    MergeCps {
        job_args: merge_cps::JobArgs,
        thread_pool: edeltraud::Edeltraud<J>,
    },
    ValueLoad {
        queue_entry_ref: Ref,
        block_ref: wheels::BlockRef,
        wheels_pid: wheels::Pid,
    },
    TxItem {
        item: KeyValueStreamItem,
        key_values_tx: mpsc::Sender<KeyValueStreamItem>,
    },
    AwaitIterRx {
        await_iter_rx: mpsc::Receiver<KeyValueRef>,
    },
}

enum TaskOutput {
    MergeCps {
        job_done: merge_cps::JobDone,
    },
    ValueLoad {
        queue_entry_ref: Ref,
        value_bytes: Bytes,
    },
    ValueNotFound {
        queue_entry_ref: Ref,
    },
    TxItemSent {
        key_values_tx: mpsc::Sender<KeyValueStreamItem>,
    },
    TxDropped,
    AwaitIterRxItem {
        await_iter_rx: mpsc::Receiver<KeyValueRef>,
        item: KeyValueRef,
    },
}

impl<J> Task<J> where J: edeltraud::Job + From<job::Job>, J::Output: From<job::JobOutput>, job::JobOutput: From<J::Output> {
    async fn make(self) -> Result<TaskOutput, Error> {
        match self {

            Task::MergeCps { job_args, thread_pool, } => {
                let job = job::Job::MergeCps(job_args);
                let job_output = thread_pool.spawn(job).await
                    .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
                let job_output: job::JobOutput = job_output.into();
                let job::MergeCpsDone(merge_cps_result) = job_output.into();
                let job_done = merge_cps_result;
                Ok(TaskOutput::MergeCps { job_done, })
            },

            Task::ValueLoad { queue_entry_ref, block_ref, mut wheels_pid, } => {
                let mut wheel_ref = wheels_pid.get(block_ref.blockwheel_filename.clone()).await
                    .map_err(|ero::NoProcError| Error::WheelsGone)?
                    .ok_or_else(|| Error::WheelNotFound {
                        blockwheel_filename: block_ref.blockwheel_filename.clone(),
                    })?;
                let block_bytes = match wheel_ref.blockwheel_pid.read_block(block_ref.block_id.clone()).await {
                    Ok(block_bytes) =>
                        block_bytes,
                    Err(blockwheel::ReadBlockError::NotFound) =>
                        return Ok(TaskOutput::ValueNotFound { queue_entry_ref, }),
                    Err(error) =>
                        return Err(Error::ReadBlock(error)),
                };
                let value_bytes = storage::value_block_deserialize(&block_bytes)
                    .map_err(Error::ValueDeserialize)?;
                Ok(TaskOutput::ValueLoad { queue_entry_ref, value_bytes, })
            },

            Task::TxItem { item, mut key_values_tx, } => {
                if let Err(_send_error) = key_values_tx.send(item).await {
                    log::warn!("client dropped iterator in TxItem task");
                    return Ok(TaskOutput::TxDropped);
                }
                Ok(TaskOutput::TxItemSent { key_values_tx, })
            },

            Task::AwaitIterRx { mut await_iter_rx, } =>
                match await_iter_rx.next().await {
                    None =>
                        Err(Error::BackendIterPeerLost),
                    Some(item) =>
                        Ok(TaskOutput::AwaitIterRxItem { await_iter_rx, item, }),
                },

        }
    }
}
