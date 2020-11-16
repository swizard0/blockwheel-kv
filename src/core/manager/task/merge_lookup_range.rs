use futures::{
    future,
    channel::{
        mpsc,
    },
    SinkExt,
};

use alloc_pool::{
    Shared,
    Unique,
};

use crate::{
    kv,
    core::{
        merger,
    },
    KeyValueStreamItem,
};

pub struct Args {
    pub key_values_tx: mpsc::Sender<KeyValueStreamItem>,
    pub butcher_iter_items: Shared<Vec<kv::KeyValuePair>>,
    pub merger_iters: Unique<Vec<merger::KeyValuesIter>>,
}

pub struct Done;

#[derive(Debug)]
pub enum Error {
    Merger(merger::Error),
}

pub async fn run(Args { mut key_values_tx, butcher_iter_items, mut merger_iters, }: Args) -> Result<Done, Error> {
    let (mut butcher_iter_tx, butcher_iter_rx) = mpsc::channel(0);
    let butcher_forward_task = async move {
        for key_value in butcher_iter_items.iter() {
            let item = KeyValueStreamItem::KeyValue(key_value.clone());
            if let Err(_send_error) = butcher_iter_tx.send(item).await {
                log::warn!("client dropped iterator in butcher forward task");
                return Ok(());
            }
        }
        if let Err(_send_error) = butcher_iter_tx.send(KeyValueStreamItem::NoMore).await {
            log::warn!("client dropped iterator in butcher forward task");
        }
        Ok::<_, Error>(())
    };
    merger_iters.push(merger::KeyValuesIter::new(butcher_iter_rx));

    let mut merger = merger::ItersMerger::new(merger_iters);

    let merge_task = async move {
        while let Some(key_value) = merger.next().await.map_err(Error::Merger)? {
            if let Err(_send_error) = key_values_tx.send(KeyValueStreamItem::KeyValue(key_value)).await {
                log::warn!("client dropped iterator in merger task");
                return Ok(());
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
