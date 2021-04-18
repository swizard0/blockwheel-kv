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
    wheels,
    storage,
    core::{
        merger,
        KeyValueRef,
        SearchRangeBounds,
    },
    KeyValueStreamItem,
};

pub struct Args {
    pub range: SearchRangeBounds,
    pub key_values_tx: mpsc::Sender<KeyValueStreamItem>,
    pub butcher_iter_items: Shared<Vec<kv::KeyValuePair<kv::Value>>>,
    pub merger_iters: Unique<Vec<merger::KeyValuesIter>>,
    pub wheels_pid: wheels::Pid,
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

pub async fn run(Args { range, mut key_values_tx, butcher_iter_items, mut merger_iters, wheels_pid, }: Args) -> Result<Done, Error> {
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
    merger_iters.push(merger::KeyValuesIter::new(butcher_iter_rx));
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
