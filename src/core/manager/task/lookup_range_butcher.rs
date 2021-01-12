use futures::{
    channel::{
        mpsc,
    },
};

use alloc_pool::{
    pool,
    Shared,
};

use crate::{
    kv,
    core::{
        butcher,
        SearchRangeBounds,
    },
    KeyValueStreamItem,
};

pub struct Args {
    pub range: SearchRangeBounds,
    pub key_values_tx: mpsc::Sender<KeyValueStreamItem>,
    pub iter_items_pool: pool::Pool<Vec<kv::KeyValuePair<kv::Value>>>,
    pub butcher_pid: butcher::Pid,
}

pub struct Done {
    pub range: SearchRangeBounds,
    pub key_values_tx: mpsc::Sender<KeyValueStreamItem>,
    pub iter_items: Shared<Vec<kv::KeyValuePair<kv::Value>>>,
}

#[derive(Debug)]
pub enum Error {
    ButcherLookupRange(ero::NoProcError),
}

pub async fn run(Args { range, key_values_tx, iter_items_pool, mut butcher_pid, }: Args) -> Result<Done, Error> {
    let iter_items = butcher_pid.lookup_range(range.clone(), iter_items_pool).await
        .map_err(Error::ButcherLookupRange)?;
    Ok(Done { range, key_values_tx, iter_items, })
}
