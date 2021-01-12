use futures::{
    select,
    future,
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
        enum State<F, I> {
            Init,
            RetrieveOnly {
                retrieve_future: F,
            },
            RetrieveWithNextItem {
                retrieve_future: F,
                next_item: I,
            },
            Finish {
                retrieve_future: F,
            },
        }

        let mut state = State::Init;

        loop {
            enum Event<M, R, F, I> {
                MergerOnInit(M),
                MergerOnRetrieveOnly {
                    result: M,
                    retrieve_future: F,
                },
                RetrieveOnRetrieveOnly(R),
                RetrieveOnRetrieveWithNextItem {
                    result: R,
                    next_item: I,
                },
                RetrieveOnFinish(R),
            }

            let event = match state {
                State::Init =>
                    Event::MergerOnInit(merger.next().await),
                State::RetrieveOnly { mut retrieve_future, } =>
                    select! {
                        result = merger.next() =>
                            Event::MergerOnRetrieveOnly { result, retrieve_future, },
                        result = retrieve_future =>
                            Event::RetrieveOnRetrieveOnly(result),
                    },
                State::RetrieveWithNextItem { mut retrieve_future, next_item, } =>
                    Event::RetrieveOnRetrieveWithNextItem {
                        result: retrieve_future.await,
                        next_item,
                    },
                State::Finish { mut retrieve_future, } =>
                    Event::RetrieveOnFinish(retrieve_future.await),
            };

            match event {
                Event::MergerOnInit(Ok(None)) =>
                    break,
                Event::MergerOnInit(Ok(Some(key_value))) =>
                    state = State::RetrieveOnly {
                        retrieve_future: schedule_retrieve(key_value, wheels_pid.clone())
                            .fuse(),
                    },
                Event::MergerOnRetrieveOnly { result: Ok(None), retrieve_future, } =>
                    state = State::Finish { retrieve_future, },
                Event::MergerOnRetrieveOnly { result: Ok(Some(key_value)), retrieve_future, } =>
                    state = State::RetrieveWithNextItem {
                        retrieve_future,
                        next_item: key_value,
                    },
                Event::RetrieveOnRetrieveOnly(Ok(key_value)) => {
                    if let Err(_send_error) = key_values_tx.send(KeyValueStreamItem::KeyValue(key_value)).await {
                        log::warn!("client dropped iterator in merger task");
                        return Ok(());
                    }
                    state = State::Init;
                },
                Event::RetrieveOnRetrieveWithNextItem { result: Ok(key_value), next_item, } => {
                    if let Err(_send_error) = key_values_tx.send(KeyValueStreamItem::KeyValue(key_value)).await {
                        log::warn!("client dropped iterator in merger task");
                        return Ok(());
                    }
                    state = State::RetrieveOnly {
                        retrieve_future: schedule_retrieve(next_item, wheels_pid.clone())
                            .fuse(),
                    };
                },

                Event::MergerOnInit(Err(error)) |
                Event::MergerOnRetrieveOnly { result: Err(error), .. } =>
                    return Err(Error::Merger(error)),
                Event::RetrieveOnRetrieveOnly(Err(error)) |
                Event::RetrieveOnRetrieveWithNextItem { result: Err(error), .. } =>
                    return Err(error),
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
            Ok(kv::KeyValuePair {
                key,
                value_cell: kv::ValueCell {
                    version,
                    cell: kv::Cell::Value(block_bytes.into()),
                },
            })
        },
    }
}
