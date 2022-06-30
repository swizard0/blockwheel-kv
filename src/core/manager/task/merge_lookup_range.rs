use std::ops::Bound;

use futures::{
    select,
    future,
    pin_mut,
    channel::{
        mpsc,
    },
    SinkExt,
    FutureExt,
};

use alloc_pool::{
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

pub enum MergeError {
    DeprecatedResultsFor {
        key: kv::Key,
        key_values_tx: mpsc::Sender<KeyValueStreamItem>,
    },
    Error(Error),
}

#[derive(Debug)]
pub enum Error {
    Merger(merger::Error),
    WheelsGone,
    WheelNotFound {
        blockwheel_filename: wheels::WheelFilename,
    },
    ReadBlock(blockwheel::ReadBlockError),
    ValueDeserialize(storage::Error),
}

pub async fn run<J>(
    Args { range, mut key_values_tx, butcher_iter_items, mut merger_iters, wheels_pid, thread_pool: _, }: Args<J>,
)
    -> Result<Done, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    let (mut butcher_iter_tx, butcher_iter_rx) = mpsc::channel(0);
    let butcher_forward_task = async move {
        for key_value in butcher_iter_items.iter() {
            let item = KeyValueRef::Item {
                key: key_value.key.clone(),
                value_cell: key_value.value_cell
                    .clone()
                    .into(),
            };
            if let Err(_send_error) = butcher_iter_tx.send(item).await {
                log::warn!("client dropped iterator in butcher forward task");
                return Ok(());
            }
        }
        if let Err(_send_error) = butcher_iter_tx.send(KeyValueRef::NoMore).await {
            log::warn!("client dropped iterator in butcher forward task");
        }
        Ok::<_, MergeError>(())
    };
    merger_iters.push(butcher_iter_rx);
    merger_iters.shrink_to_fit();

    let mut merger = merger::ItersMerger::new(merger_iters);

    let merge_task = async move {
        let maybe_merger_next = merger.next().await
            .map_err(Error::Merger)
            .map_err(MergeError::Error)?;
        if let Some(mut merger_key_value) = maybe_merger_next {
            loop {
                let merger_future = merger.next().fuse();
                pin_mut!(merger_future);
                let retrieve_future = schedule_retrieve(merger_key_value, wheels_pid.clone(), key_values_tx).fuse();
                pin_mut!(retrieve_future);

                enum Event<M, R> {
                    Merger(M),
                    Retrieve(R),
                }

                let event = select! {
                    result = &mut merger_future =>
                        Event::Merger(result.map_err(Error::Merger).map_err(MergeError::Error)?),
                    result = &mut retrieve_future =>
                        Event::Retrieve(result?),
                };

                match event {
                    Event::Merger(None) => {
                        let (maybe_key_value, returned_key_values_tx) = retrieve_future.await?;
                        key_values_tx = returned_key_values_tx;
                        if let Some(key_value) = maybe_key_value {
                            if let Err(_send_error) = key_values_tx.send(KeyValueStreamItem::KeyValue(key_value)).await {
                                log::warn!("client dropped iterator in merger task");
                                return Ok(());
                            }
                        }
                        break;
                    },
                    Event::Merger(Some(next_key_value)) => {
                        let (maybe_key_value, returned_key_values_tx) = retrieve_future.await?;
                        key_values_tx = returned_key_values_tx;
                        if let Some(key_value) = maybe_key_value {
                            if let Err(_send_error) = key_values_tx.send(KeyValueStreamItem::KeyValue(key_value)).await {
                                log::warn!("client dropped iterator in merger task");
                                return Ok(());
                            }
                        }
                        merger_key_value = next_key_value;
                    },
                    Event::Retrieve((maybe_key_value, returned_key_values_tx)) => {
                        key_values_tx = returned_key_values_tx;
                        if let Some(key_value) = maybe_key_value {
                            if let Err(_send_error) = key_values_tx.send(KeyValueStreamItem::KeyValue(key_value)).await {
                                log::warn!("client dropped iterator in merger task");
                                return Ok(());
                            }
                        }
                        let maybe_key_value = merger_future.await
                            .map_err(Error::Merger)
                            .map_err(MergeError::Error)?;
                        match maybe_key_value {
                            None =>
                                break,
                            Some(next_key_value) =>
                                merger_key_value = next_key_value,
                        }
                    },
                }
            }
        }
        if let Err(_send_error) = key_values_tx.send(KeyValueStreamItem::NoMore).await {
            log::warn!("client dropped iterator in merger task");
        }
        Ok::<_, MergeError>(())
    };

    match future::try_join(butcher_forward_task, merge_task).await {
        Ok(((), ())) =>
            Ok(Done::MergeSuccess),
        Err(MergeError::DeprecatedResultsFor { key, key_values_tx, }) =>
            Ok(Done::DeprecatedResults {
                modified_range: SearchRangeBounds {
                    range_from: Bound::Included(key),
                    ..range
                },
                key_values_tx,
            }),
        Err(MergeError::Error(error)) =>
            Err(error),
    }
}

async fn schedule_retrieve(
    key_value: kv::KeyValuePair<storage::OwnedValueBlockRef>,
    mut wheels_pid: wheels::Pid,
    key_values_tx: mpsc::Sender<KeyValueStreamItem>,
)
    -> Result<(Option<kv::KeyValuePair<kv::Value>>, mpsc::Sender<KeyValueStreamItem>), MergeError>
{
    match key_value {
        kv::KeyValuePair {
            key,
            value_cell: kv::ValueCell {
                version,
                cell: kv::Cell::Value(storage::OwnedValueBlockRef::Inline(value)),
            },
        } =>
            Ok((
                Some(kv::KeyValuePair { key, value_cell: kv::ValueCell { version, cell: kv::Cell::Value(value), }, }),
                key_values_tx,
            )),
        kv::KeyValuePair {
            key,
            value_cell: kv::ValueCell {
                version,
                cell: kv::Cell::Value(storage::OwnedValueBlockRef::Ref(block_ref)),
            },
        } => {
            let mut wheel_ref = wheels_pid.get(block_ref.blockwheel_filename.clone()).await
                .map_err(|ero::NoProcError| Error::WheelsGone)
                .map_err(MergeError::Error)?
                .ok_or_else(|| MergeError::Error(Error::WheelNotFound {
                    blockwheel_filename: block_ref.blockwheel_filename.clone(),
                }))?;
            let block_bytes = match wheel_ref.blockwheel_pid.read_block(block_ref.block_id.clone()).await {
                Ok(block_bytes) =>
                    block_bytes,
                Err(blockwheel::ReadBlockError::NotFound) =>
                    return Err(MergeError::DeprecatedResultsFor { key, key_values_tx, }),
                Err(error) =>
                    return Err(MergeError::Error(Error::ReadBlock(error))),
            };
            let value_bytes = storage::value_block_deserialize(&block_bytes)
                .map_err(Error::ValueDeserialize)
                .map_err(MergeError::Error)?;
            Ok((
                Some(kv::KeyValuePair {
                    key,
                    value_cell: kv::ValueCell {
                        version,
                        cell: kv::Cell::Value(value_bytes.into()),
                    },
                }),
                key_values_tx,
            ))
        },
        kv::KeyValuePair { key, value_cell: kv::ValueCell { version, cell: kv::Cell::Tombstone, }, } =>
            Ok((
                Some(kv::KeyValuePair { key, value_cell: kv::ValueCell { version, cell: kv::Cell::Tombstone, }, }),
                key_values_tx,
            )),
    }
}

pub mod merge_job {
    use std::{
        collections::{
            VecDeque,
        },
    };

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
        pub ready_items: VecDeque<kv::KeyValuePair<storage::OwnedValueBlockRef>>,
        pub await_butcher_iters: Vec<ButcherJoinedIter>,
        pub await_search_tree_iters: Vec<mpsc::Receiver<KeyValueRef>>,
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
    }

    pub struct JobDone {
        pub env: Env,
        pub done: Done,
    }

    pub enum Done {
        AwaitIters,
        Finish,
    }

    pub type JobOutput = JobDone;

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

    pub fn run(JobArgs { mut env, kont, }: JobArgs) -> JobOutput {
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
                    if let Some(await_butcher_iter) = env.await_butcher_iters.pop() {
                        let item = if let Some(key_value) = await_butcher_iter.items.get(await_butcher_iter.index) {
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
                        return JobDone { env, done: Done::AwaitIters, };
                    }
                },
                merger::Kont::Deprecated { next, .. } =>
                    next.proceed(),
                merger::Kont::Item { item, next, } => {
                    env.ready_items.push_back(item);
                    next.proceed()
                },
                merger::Kont::Done =>
                    return JobDone { env, done: Done::Finish, },
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
