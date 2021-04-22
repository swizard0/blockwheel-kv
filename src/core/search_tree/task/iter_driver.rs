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
            task::{
                IterRecRequest,
                IterRequestData,
            },
            SearchTreeIterItemsTx,
        },
        BlockRef,
        KeyValueRef,
        SearchRangeBounds,
    },
};

pub struct Args {
    pub iter_rec_tx: mpsc::Sender<IterRecRequest>,
    pub maybe_block_ref: Option<BlockRef>,
    pub range: SearchRangeBounds,
    pub iter_items_tx: SearchTreeIterItemsTx,
}

pub struct Done;

#[derive(Debug)]
pub enum Error {
    IterSearchTreeGenServerGone,
}

pub async fn run(
    Args {
        mut iter_rec_tx,
        maybe_block_ref,
        range,
        iter_items_tx,
    }: Args,
)
    -> Result<Done, Error>
{
    let (repay_iter_items_tx, repay_iter_items_rx) = oneshot::channel();

    let iter_request = IterRecRequest {
        maybe_block_ref,
        data: IterRequestData {
            range,
            iter_items_tx,
            repay_iter_items_tx,
        },
    };
    iter_rec_tx.send(iter_request).await
        .map_err(|_send_error| Error::IterSearchTreeGenServerGone)?;

    match repay_iter_items_rx.await {
        Ok(mut iter_items_tx) =>
            if let Err(_send_error) = iter_items_tx.items_tx.send(KeyValueRef::NoMore).await {
                log::warn!("client canceled iter request");
            },
        Err(oneshot::Canceled) =>
            log::debug!("repay_iter_items_rx canceled: assume iter request cancel"),
    }
    Ok(Done)
}
