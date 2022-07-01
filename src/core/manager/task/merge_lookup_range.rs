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
                    match queue_entries.remove(queue_tx_ref) {
                        None =>
                            unreachable!(),
                        Some(QueueEntry::ReadyToSend { item, }) => {
                            tasks.push(Task::TxItem { item: KeyValueStreamItem::KeyValue(item), key_values_tx, }.make());
                            tx_state = TxState::Sending;
                            continue;
                        },
                        Some(QueueEntry::PendingValueLoad { key, version, }) => {
                            let queue_tx_ref =
                                queue_entries.insert(QueueEntry::PendingValueLoad { key, version, });
                            queue.push_front(queue_tx_ref);
                        },
                        Some(QueueEntry::Finish) => {
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

pub mod merge_cps {
    use alloc_pool::{
        Unique,
        Shared,
    };

    use futures::{
        channel::{
            mpsc,
        },
    };

    use crate::{
        kv,
        storage,
        core::{
            merger,
            KeyValueRef,
        },
    };

    #[derive(Default)]
    pub struct Env {
        pub ready_items: Vec<kv::KeyValuePair<storage::OwnedValueBlockRef>>,
        pub await_butcher_iters: Vec<ButcherJoinedIter>,
        pub await_search_tree_iters: Vec<mpsc::Receiver<KeyValueRef>>,
        pub await_outcomes: Vec<AwaitOutcome>,
    }

    pub struct JobArgs {
        pub env: Env,
        pub kont: Kont,
    }

    pub enum Kont {
        Start {
            butcher_iter_items: Shared<Vec<kv::KeyValuePair<kv::Value>>>,
            merger_iters: Unique<Vec<mpsc::Receiver<KeyValueRef>>>,
        },
        AwaitReady(KontAwaitReady),
    }

    pub struct KontAwaitReady {
        pub next: merger::KontAwaitScheduledNext<JoinedIters, JoinedIter>,
    }

    pub struct JobDone {
        pub env: Env,
        pub done: Done,
    }

    pub enum Done {
        AwaitIters {
            next: merger::KontAwaitScheduledNext<JoinedIters, JoinedIter>,
        },
        Finish,
    }

    pub type JobOutput = JobDone;

    pub struct AwaitOutcome {
        pub iter: mpsc::Receiver<KeyValueRef>,
        pub item: KeyValueRef,
    }

    pub struct JoinedIters {
        iters: Vec<JoinedIter>,
    }

    pub enum JoinedIter {
        Butcher(ButcherJoinedIter),
        SearchTree(mpsc::Receiver<KeyValueRef>),
    }

    pub struct ButcherJoinedIter {
        items: Shared<Vec<kv::KeyValuePair<kv::Value>>>,
        index: usize,
    }

    pub fn job(JobArgs { mut env, kont, }: JobArgs) -> JobOutput {
        assert!(env.ready_items.is_empty());
        assert!(env.await_butcher_iters.is_empty());
        assert!(env.await_search_tree_iters.is_empty());

        let mut merger_kont = match kont {
            Kont::Start { butcher_iter_items, mut merger_iters, } => {
                let mut iters = Vec::with_capacity(1 + merger_iters.len());
                iters.push(JoinedIter::Butcher(ButcherJoinedIter {
                    items: butcher_iter_items,
                    index: 0,
                }));
                iters.extend(merger_iters.drain(..).map(JoinedIter::SearchTree));
                merger::ItersMergerCps::new(JoinedIters { iters, })
                    .step()
            },
            Kont::AwaitReady(KontAwaitReady { next, }) => {
                let await_outcome = env.await_outcomes.pop().unwrap();
                next.proceed_with_item(JoinedIter::SearchTree(await_outcome.iter), await_outcome.item)
            },
        };

        loop {
            merger_kont = match merger_kont {
                merger::Kont::ScheduleIterAwait { await_iter, next, } => {
                    match await_iter {
                        JoinedIter::Butcher(await_butcher_iter) =>
                            env.await_butcher_iters.push(await_butcher_iter),
                        JoinedIter::SearchTree(await_search_tree_iter) =>
                            env.await_search_tree_iters.push(await_search_tree_iter),
                    }
                    next.proceed()
                },
                merger::Kont::AwaitScheduled { next, } => {
                    if let Some(await_outcome) = env.await_outcomes.pop() {
                        next.proceed_with_item(JoinedIter::SearchTree(await_outcome.iter), await_outcome.item)
                    } else if let Some(mut await_butcher_iter) = env.await_butcher_iters.pop() {
                        let item = if let Some(key_value) = await_butcher_iter.items.get(await_butcher_iter.index) {
                            await_butcher_iter.index += 1;
                            KeyValueRef::Item {
                                key: key_value.key.clone(),
                                value_cell: key_value.value_cell
                                    .clone()
                                    .into(),
                            }
                        } else {
                            KeyValueRef::NoMore
                        };
                        next.proceed_with_item(JoinedIter::Butcher(await_butcher_iter), item)
                    } else {
                        assert!(!env.await_search_tree_iters.is_empty());
                        return JobDone { env, done: Done::AwaitIters { next, }, };
                    }
                },
                merger::Kont::Deprecated { next, .. } => {
                    assert!(env.await_outcomes.is_empty());
                    next.proceed()
                },
                merger::Kont::Item { item, next, } => {
                    assert!(env.await_outcomes.is_empty());
                    env.ready_items.push(item);
                    next.proceed()
                },
                merger::Kont::Done => {
                    assert!(env.await_outcomes.is_empty());
                    return JobDone { env, done: Done::Finish, };
                },
            }
        }
    }

    impl std::ops::Deref for JoinedIters {
        type Target = Vec<JoinedIter>;

        fn deref(&self) -> &Self::Target {
            &self.iters
        }
    }

    impl std::ops::DerefMut for JoinedIters {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.iters
        }
    }
}
