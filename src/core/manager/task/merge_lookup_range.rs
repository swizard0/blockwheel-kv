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
    },
    KeyValueStreamItem,
};

pub struct Args {
    pub key_values_tx: mpsc::Sender<KeyValueStreamItem>,
    pub butcher_iter_items: Shared<Vec<kv::KeyValuePair<kv::Value>>>,
    pub merger_iters: Unique<Vec<merger::KeyValuesIter>>,
    pub wheels_pid: wheels::Pid,
}

pub struct Done;

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

pub async fn run(Args { mut key_values_tx, butcher_iter_items, mut merger_iters, wheels_pid, }: Args) -> Result<Done, Error> {
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
        Ok::<_, Error>(())
    };
    merger_iters.push(merger::KeyValuesIter::new(butcher_iter_rx));

    let mut merger = merger::ItersMerger::new(merger_iters);

    let merge_task = async move {
        if let Some(mut merger_key_value) = merger.next().await.map_err(Error::Merger)? {
            loop {
                let merger_future = merger.next().fuse();
                pin_mut!(merger_future);
                let retrieve_future = schedule_retrieve(merger_key_value, wheels_pid.clone()).fuse();
                pin_mut!(retrieve_future);

                enum Event<M, R> {
                    Merger(M),
                    Retrieve(R),
                }

                let event = select! {
                    result = &mut merger_future =>
                        Event::Merger(result.map_err(Error::Merger)?),
                    result = &mut retrieve_future =>
                        Event::Retrieve(result?),
                };

                match event {
                    Event::Merger(None) => {
                        let key_value = retrieve_future.await?;
                        if let Err(_send_error) = key_values_tx.send(KeyValueStreamItem::KeyValue(key_value)).await {
                            log::warn!("client dropped iterator in merger task");
                            return Ok(());
                        }
                        break;
                    },
                    Event::Merger(Some(next_key_value)) => {
                        let key_value = retrieve_future.await?;
                        if let Err(_send_error) = key_values_tx.send(KeyValueStreamItem::KeyValue(key_value)).await {
                            log::warn!("client dropped iterator in merger task");
                            return Ok(());
                        }
                        merger_key_value = next_key_value;
                    },
                    Event::Retrieve(key_value) => {
                        if let Err(_send_error) = key_values_tx.send(KeyValueStreamItem::KeyValue(key_value)).await {
                            log::warn!("client dropped iterator in merger task");
                            return Ok(());
                        }
                        match merger_future.await.map_err(Error::Merger)? {
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
        Ok::<_, Error>(())
    };

    let ((), ()) = future::try_join(butcher_forward_task, merge_task).await?;
    Ok(Done)
}

async fn schedule_retrieve(
    key_value: kv::KeyValuePair<storage::OwnedValueBlockRef>,
    mut wheels_pid: wheels::Pid,
)
    -> Result<kv::KeyValuePair<kv::Value>, Error>
{
    match key_value {
        kv::KeyValuePair {
            key,
            value_cell: kv::ValueCell {
                version,
                cell: kv::Cell::Value(storage::OwnedValueBlockRef::Inline(value)),
            },
        } =>
            Ok(kv::KeyValuePair { key, value_cell: kv::ValueCell { version, cell: kv::Cell::Value(value), }, }),
        kv::KeyValuePair {
            key,
            value_cell: kv::ValueCell {
                version,
                cell: kv::Cell::Value(storage::OwnedValueBlockRef::Ref(block_ref)),
            },
        } => {
            let mut wheel_ref = wheels_pid.get(block_ref.blockwheel_filename.clone()).await
                .map_err(|ero::NoProcError| Error::WheelsGone)?
                .ok_or_else(|| Error::WheelNotFound {
                    blockwheel_filename: block_ref.blockwheel_filename.clone(),
                })?;
            let block_bytes = wheel_ref.blockwheel_pid.read_block(block_ref.block_id).await
                .map_err(Error::ReadBlock)?;
            let value_bytes = storage::value_block_deserialize(&block_bytes)
                .map_err(Error::ValueDeserialize)?;
            Ok(kv::KeyValuePair {
                key,
                value_cell: kv::ValueCell {
                    version,
                    cell: kv::Cell::Value(value_bytes.into()),
                },
            })
        },
        kv::KeyValuePair { key, value_cell: kv::ValueCell { version, cell: kv::Cell::Tombstone, }, } =>
            Ok(kv::KeyValuePair { key, value_cell: kv::ValueCell { version, cell: kv::Cell::Tombstone, }, }),
    }
}
