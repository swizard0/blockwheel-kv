use futures::{
    future,
    channel::{
        mpsc,
        oneshot,
    },
    SinkExt,
    StreamExt,
};

use alloc_pool::{
    bytes::{
        Bytes,
        BytesPool,
    },
};

use crate::{
    kv,
    storage,
    core::{
        BlockRef,
        search_tree::{
            task::{
                ItersTx,
                IterRequest,
                IterRequestsQueue,
            },
            SearchTreeIterTx,
            SearchTreeIterRx,
        },
    },
};

pub struct Args {
    pub block_ref: BlockRef,
    pub blocks_pool: BytesPool,
    pub iters_tx: ItersTx,
    pub block_bytes: Bytes,
    pub iter_rec_tx: mpsc::Sender<IterRequest>,
    pub iter_requests_queue: IterRequestsQueue,
}

pub struct Done {
    pub block_ref: BlockRef,
}

#[derive(Debug)]
pub enum Error {
    ReadBlockStorage { block_ref: BlockRef, error: storage::Error, },
}

pub async fn run(
    Args {
        block_ref,
        blocks_pool,
        mut iters_tx,
        block_bytes,
        mut iter_rec_tx,
        mut iter_requests_queue,
    }: Args,
)
    -> Result<Done, Error>
{
    for IterRequest { block_ref: request_block_ref, reply_tx, } in iter_requests_queue.drain(..) {
        assert_eq!(request_block_ref, block_ref);
        let (items_tx, items_rx) = mpsc::channel(0);
        if let Err(_send_error) = reply_tx.send(SearchTreeIterRx { items_rx, }) {
            log::warn!("client canceled iter request");
        } else {
            iters_tx.push(SearchTreeIterTx { items_tx, });
        }
    }

    let entries_iter = storage::block_deserialize_iter(&block_bytes)
        .map_err(|error| Error::ReadBlockStorage { block_ref: block_ref.clone(), error, })?;
    for maybe_entry in entries_iter {
        let entry = maybe_entry
            .map_err(|error| Error::ReadBlockStorage { block_ref: block_ref.clone(), error, })?;
        let maybe_jump_block_ref = match entry.jump_ref {
            storage::JumpRef::None =>
                None,
            storage::JumpRef::Local(storage::LocalJumpRef { block_id, }) =>
                Some(BlockRef {
                    blockwheel_filename: block_ref.blockwheel_filename.clone(),
                    block_id: block_id.clone(),
                }),
            storage::JumpRef::External(storage::ExternalJumpRef { filename, block_id, }) =>
                Some(BlockRef {
                    blockwheel_filename: filename.into(),
                    block_id: block_id.clone(),
                }),
        };
        if let Some(jump_block_ref) = maybe_jump_block_ref {
            let (reply_tx, reply_rx) = oneshot::channel();
            if let Err(_send_error) = iter_rec_tx.send(IterRequest { block_ref: jump_block_ref, reply_tx, }).await {
                log::warn!("search_tree has gone, terminating iter task");
                break;
            }
            let mut items_rx = match reply_rx.await {
                Ok(SearchTreeIterRx { items_rx }) =>
                    items_rx,
                Err(oneshot::Canceled) => {
                    log::warn!("search_tree has gone, terminating iter task");
                    break;
                },
            };
            while let Some(key_value_pair) = items_rx.next().await {
                let Broadcasted = broadcast_task(key_value_pair, &mut iters_tx).await;
            }
        }

        let mut key_bytes = blocks_pool.lend();
        key_bytes.extend_from_slice(entry.key);
        let key = kv::Key { key_bytes: key_bytes.freeze(), };

        let value_cell = match entry.value_cell {
            storage::ValueCell { version, cell: storage::Cell::Value { value, }, } => {
                let mut value_bytes = blocks_pool.lend();
                value_bytes.extend_from_slice(value);
                kv::ValueCell { version, cell: kv::Cell::Value(kv::Value { value_bytes: value_bytes.freeze(), }), }
            },
            storage::ValueCell { version, cell: storage::Cell::Tombstone, } =>
                kv::ValueCell { version, cell: kv::Cell::Tombstone, },
        };

        let Broadcasted = broadcast_task(kv::KeyValuePair { key, value_cell, }, &mut iters_tx).await;
    }

    Ok(Done { block_ref, })
}

struct Broadcasted;

async fn broadcast_task(key_value_pair: kv::KeyValuePair, iters_tx: &mut ItersTx) -> Broadcasted {
    let task = future::join_all(
        iters_tx.iter_mut()
            .map(|SearchTreeIterTx { items_tx, }| {
                let key_value_pair = key_value_pair.clone();
                async move {
                    items_tx.send(key_value_pair).await.ok();
                }
            }),
    );
    let _vec: Vec<()> = task.await;
    Broadcasted
}
