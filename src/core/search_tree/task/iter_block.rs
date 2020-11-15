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
        BlockRef,
        SearchRangeBounds,
    },
    KeyValueStreamItem,
};

pub struct Args {
    pub block_ref: BlockRef,
    pub blocks_pool: BytesPool,
    pub iters_tx: ItersTx,
    pub block_bytes: Bytes,
    pub iter_rec_tx: mpsc::Sender<IterRequest>,
    pub iter_requests_queue: IterRequestsQueue,
    pub iter_send_buffer: usize,
}

pub struct Done {
    pub block_ref: BlockRef,
}

#[derive(Debug)]
pub enum Error {
    ReadBlockStorage { block_ref: BlockRef, error: storage::Error, },
    IterRecPeerLost,
}

pub async fn run(
    Args {
        block_ref,
        blocks_pool,
        mut iters_tx,
        block_bytes,
        mut iter_rec_tx,
        mut iter_requests_queue,
        iter_send_buffer,
    }: Args,
)
    -> Result<Done, Error>
{
    for IterRequest { block_ref: request_block_ref, kind, } in iter_requests_queue.drain(..) {
        assert_eq!(request_block_ref, block_ref);
        match kind {
            IterRequestKind::Items { range, reply_tx, } => {
                let (items_tx, items_rx) = mpsc::channel(iter_send_buffer);
                if let Err(_send_error) = reply_tx.send(SearchTreeIterItemsRx { items_rx, }) {
                    log::warn!("client canceled iter items request");
                } else {
                    iters_tx.items_txs.push(SearchTreeIterItemsTx { range, items_tx, });
                }
            },
            IterRequestKind::BlockRefs { reply_tx, } => {
                let (block_refs_tx, block_refs_rx) = mpsc::channel(iter_send_buffer);
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
            if !iters_tx.items_txs.is_empty() {
                let (reply_tx, reply_rx) = oneshot::channel();
                let send_result = iter_rec_tx.send(IterRequest {
                    block_ref: jump_block_ref.clone(),
                    kind: IterRequestKind::Items { reply_tx, },
                }).await;
                if let Err(_send_error) = send_result {
                    log::warn!("search_tree has gone, terminating iter task");
                    break;
                }
                let mut items_rx = match reply_rx.await {
                    Ok(SearchTreeIterItemsRx { items_rx }) =>
                        items_rx,
                    Err(oneshot::Canceled) => {
                        log::warn!("search_tree has gone, terminating iter task");
                        break;
                    },
                };
                loop {
                    match items_rx.next().await {
                        None =>
                            return Err(Error::IterRecPeerLost),
                        Some(KeyValueStreamItem::NoMore) =>
                            break,
                        Some(KeyValueStreamItem::KeyValue(key_value_pair)) => {
                            let Broadcasted = broadcast_items_task(
                                KeyValueStreamItem::KeyValue(key_value_pair),
                                &mut iters_tx.items_txs,
                            ).await;
                        },
                    }
                }
            }

            if !iters_tx.block_refs_txs.is_empty() {
                let (reply_tx, reply_rx) = oneshot::channel();
                let send_result = iter_rec_tx.send(IterRequest {
                    block_ref: jump_block_ref.clone(),
                    kind: IterRequestKind::BlockRefs { reply_tx, },
                }).await;
                if let Err(_send_error) = send_result {
                    log::warn!("search_tree has gone, terminating iter task");
                    break;
                }
                let mut block_refs_rx = match reply_rx.await {
                    Ok(SearchTreeIterBlockRefsRx { block_refs_rx }) =>
                        block_refs_rx,
                    Err(oneshot::Canceled) => {
                        log::warn!("search_tree has gone, terminating iter task");
                        break;
                    },
                };
                while let Some(block_ref) = block_refs_rx.next().await {
                    let Broadcasted = broadcast_block_refs_task(block_ref, &mut iters_tx.block_refs_txs).await;
                }
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

            let Broadcasted = broadcast_items_task(
                KeyValueStreamItem::KeyValue(kv::KeyValuePair { key, value_cell, }),
                &mut iters_tx.items_txs,
            ).await;
        }
    }

    if !iters_tx.items_txs.is_empty() {
        let Broadcasted = broadcast_items_task(KeyValueStreamItem::NoMore, &mut iters_tx.items_txs).await;
    }

    if !iters_tx.block_refs_txs.is_empty() {
        let Broadcasted = broadcast_block_refs_task(block_ref.clone(), &mut iters_tx.block_refs_txs).await;
    }

    iters_tx.clear();
    Ok(Done { block_ref, })
}

struct Broadcasted;

async fn broadcast_items_task(stream_item: KeyValueStreamItem, items_txs: &mut [SearchTreeIterItemsTx]) -> Broadcasted {
    let task = future::join_all(
        items_txs.iter_mut()
            .map(|SearchTreeIterItemsTx { items_tx, }| {
                let stream_item = stream_item.clone();
                async move {
                    items_tx.send(stream_item).await.ok();
                }
            }),
    );
    let _vec: Vec<()> = task.await;
    Broadcasted
}

async fn broadcast_block_refs_task(block_ref: BlockRef, block_refs_txs: &mut [SearchTreeIterBlockRefsTx]) -> Broadcasted {
    let task = future::join_all(
        block_refs_txs.iter_mut()
            .map(|SearchTreeIterBlockRefsTx { block_refs_tx, }| {
                let block_ref = block_ref.clone();
                async move {
                    block_refs_tx.send(block_ref).await.ok();
                }
            }),
    );
    let _vec: Vec<()> = task.await;
    Broadcasted
}
