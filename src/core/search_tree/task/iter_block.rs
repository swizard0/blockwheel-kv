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
                IterRequestKind,
            },
            SearchTreeIterItemsTx,
            SearchTreeIterItemsRx,
            SearchTreeIterBlockRefsTx,
            SearchTreeIterBlockRefsRx,
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
    for IterRequest { block_ref: request_block_ref, kind, } in iter_requests_queue.drain(..) {
        assert_eq!(request_block_ref, block_ref);
        match kind {
            IterRequestKind::Items { reply_tx, } => {
                let (items_tx, items_rx) = mpsc::channel(0);
                if let Err(_send_error) = reply_tx.send(SearchTreeIterItemsRx { items_rx, }) {
                    log::warn!("client canceled iter items request");
                } else {
                    iters_tx.items_txs.push(SearchTreeIterItemsTx { items_tx, });
                }
            },
            IterRequestKind::BlockRefs { reply_tx, } => {
                let (block_refs_tx, block_refs_rx) = mpsc::channel(0);
                if let Err(_send_error) = reply_tx.send(SearchTreeIterBlockRefsRx { block_refs_rx, }) {
                    log::warn!("client canceled iter block refs request");
                } else {
                    iters_tx.block_refs_txs.push(SearchTreeIterBlockRefsTx { block_refs_tx, });
                }
            },
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
            let items_txs = &mut iters_tx.items_txs;
            // let block_refs_txs = &mut iters_tx.block_refs_txs;

            let forward_items_task = async {
                if items_txs.is_empty() {
                    return false;
                }
                let (reply_tx, reply_rx) = oneshot::channel();
                let send_result = iter_rec_tx.send(IterRequest {
                    block_ref: jump_block_ref,
                    kind: IterRequestKind::Items {  reply_tx, },
                }).await;
                if let Err(_send_error) = send_result {
                    log::warn!("search_tree has gone, terminating iter task");
                    return true;
                }
                let mut items_rx = match reply_rx.await {
                    Ok(SearchTreeIterItemsRx { items_rx }) =>
                        items_rx,
                    Err(oneshot::Canceled) => {
                        log::warn!("search_tree has gone, terminating iter task");
                        return true;
                    },
                };
                while let Some(key_value_pair) = items_rx.next().await {
                    let Broadcasted = broadcast_items_task(key_value_pair, items_txs).await;
                }
                false
            };

            let force_break = forward_items_task.await;
            if force_break {
                break;
            }
        }

        if !iters_tx.items_txs.is_empty() {
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

            let Broadcasted = broadcast_items_task(kv::KeyValuePair { key, value_cell, }, &mut iters_tx.items_txs).await;
        }
    }

    iters_tx.clear();
    Ok(Done { block_ref, })
}

struct Broadcasted;

async fn broadcast_items_task(key_value_pair: kv::KeyValuePair, items_txs: &mut [SearchTreeIterItemsTx]) -> Broadcasted {
    let task = future::join_all(
        items_txs.iter_mut()
            .map(|SearchTreeIterItemsTx { items_tx, }| {
                let key_value_pair = key_value_pair.clone();
                async move {
                    items_tx.send(key_value_pair).await.ok();
                }
            }),
    );
    let _vec: Vec<()> = task.await;
    Broadcasted
}
