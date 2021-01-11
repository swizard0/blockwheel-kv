use std::sync::Arc;

use futures::{
    channel::{
        mpsc,
        oneshot,
    },
    SinkExt,
};

use crate::{
    core::{
        search_tree::{
            KeyValueRef,
            SearchTreeIterItemsRx,
        },
        MemCache,
        SearchRangeBounds,
    },
};

pub struct Args {
    pub cache: Arc<MemCache>,
    pub range: SearchRangeBounds,
    pub reply_tx: oneshot::Sender<SearchTreeIterItemsRx>,
}

pub struct Done;

#[derive(Debug)]
pub enum Error {
}

pub async fn run(Args { cache, range, reply_tx, }: Args) -> Result<Done, Error> {
    let (mut iter_tx, iter_rx) = mpsc::channel(0);
    let iter = SearchTreeIterItemsRx { items_rx: iter_rx, };
    if let Err(_send_error) = reply_tx.send(iter) {
        log::warn!("client canceled iter request");
    } else {
        for kv_pair in cache.range(range) {
            let iter_item = KeyValueRef::Item {
                key: kv_pair.key,
                value_cell: kv_pair.value_cell.into(),
            };
            if let Err(_send_error) = iter_tx.send(iter_item).await {
                return Ok(Done);
            }
        }
        iter_tx.send(KeyValueRef::NoMore).await.ok();
    }
    Ok(Done)
}
