use std::sync::Arc;

use futures::{
    channel::{
        mpsc,
        oneshot,
    },
    SinkExt,
};

use crate::{
    kv,
    core::{
        search_tree::{
            SearchTreeIterItemsRx,
        },
        OrdKey,
        MemCache,
        SearchRangeBounds,
    },
    KeyValueStreamItem,
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
        let ord_key_range = (ord_key_map(range.range_from), ord_key_map(range.range_to));
        for (ord_key, value_cell) in cache.range(ord_key_range) {
            let kv_pair = kv::KeyValuePair {
                key: ord_key.as_ref().clone(),
                value_cell: value_cell.clone(),
            };
            if let Err(_send_error) = iter_tx.send(KeyValueStreamItem::KeyValue(kv_pair)).await {
                return Ok(Done);
            }
        }
        iter_tx.send(KeyValueStreamItem::NoMore).await.ok();
    }
    Ok(Done)
}

use std::ops::Bound;

fn ord_key_map(bound: Bound<kv::Key>) -> Bound<OrdKey> {
    match bound {
        Bound::Unbounded =>
            Bound::Unbounded,
        Bound::Included(key) =>
            Bound::Included(OrdKey::new(key)),
        Bound::Excluded(key) =>
            Bound::Excluded(OrdKey::new(key)),
    }
}
