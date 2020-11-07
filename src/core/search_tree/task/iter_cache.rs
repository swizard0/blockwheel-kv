use std::sync::Arc;

use futures::{
    channel::{
        mpsc,
    },
    SinkExt,
};

use crate::{
    kv,
    core::{
        MemCache,
    },
};

pub struct Args {
    pub cache: Arc<MemCache>,
    pub iter_tx: mpsc::Sender<kv::KeyValuePair>,
}

pub struct Done;

#[derive(Debug)]
pub enum Error {
}

pub async fn run(Args { cache, mut iter_tx, }: Args) -> Result<Done, Error> {
    for (ord_key, value_cell) in cache.iter() {
        let kv_pair = kv::KeyValuePair {
            key: ord_key.as_ref().clone(),
            value_cell: value_cell.clone(),
        };
        if let Err(_send_error) = iter_tx.send(kv_pair).await {
            break;
        }
    }
    Ok(Done)
}
