use std::{
    str,
    ops::Bound,
    cmp::Ordering,
};

use futures::{
    channel::{
        mpsc,
        oneshot,
    },
    SinkExt,
    StreamExt,
};

use alloc_pool::{
    pool,
    bytes::{
        Bytes,
    },
};

use crate::{
    storage,
    core::{
        search_tree::{
            task::{
                IterRequest,
                IterRequestKind,
                BlockEntry,
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
    pub iter_request: IterRequest,
    pub iter_block_entries_pool: pool::Pool<Vec<BlockEntry>>,
    pub block_bytes: Bytes,
    pub iter_rec_tx: mpsc::Sender<IterRequest>,
    pub iter_send_buffer: usize,
}

pub struct Done {
    pub block_ref: BlockRef,
}

#[derive(Debug)]
pub enum Error {
    ReadBlockStorage { block_ref: BlockRef, error: storage::Error, },
    FilenameUtf8 { block_ref: BlockRef, error: str::Utf8Error, },
    IterRecPeerLost,
    SearchTreeGone,
    BlockEntriesJoin(tokio::task::JoinError),
}

pub async fn run(
    Args {
        block_ref,
        iter_request,
        iter_block_entries_pool,
        block_bytes,
        mut iter_rec_tx,
        iter_send_buffer,
    }: Args,
)
    -> Result<Done, Error>
{
    assert_eq!(iter_request.block_ref, block_ref);

    enum IterKind {
        Items(SearchTreeIterItemsTx),
        BlockRefs(SearchTreeIterBlockRefsTx),
    }

    let mut iter_kind = match iter_request.kind {
        IterRequestKind::Items { range, reply_tx, } => {
            let (items_tx, items_rx) = mpsc::channel(iter_send_buffer);
            if let Err(_send_error) = reply_tx.send(SearchTreeIterItemsRx { items_rx, }) {
                log::warn!("client canceled iter items request");
                return Ok(Done { block_ref, });
            }
            IterKind::Items(SearchTreeIterItemsTx { range, items_tx, })
        },
        IterRequestKind::BlockRefs { reply_tx, } => {
            let (block_refs_tx, block_refs_rx) = mpsc::channel(iter_send_buffer);
            if let Err(_send_error) = reply_tx.send(SearchTreeIterBlockRefsRx { block_refs_rx, }) {
                log::warn!("client canceled iter block refs request");
                return Ok(Done { block_ref, });
            }
            IterKind::BlockRefs(SearchTreeIterBlockRefsTx { block_refs_tx, })
        },
    };

    let mut block_entries = iter_block_entries_pool.lend(Vec::new);
    block_entries.clear();

    let block_entries_task = tokio::task::spawn_blocking(move || {

        Ok(block_entries)
    });
    let _block_entries = block_entries_task.await
        .map_err(Error::BlockEntriesJoin)??;


    let entries_iter = storage::BlockDeserializeIter::new(block_bytes)
        .map_err(|error| Error::ReadBlockStorage { block_ref: block_ref.clone(), error, })?;
    for maybe_entry in entries_iter {
        let (jump_ref, key_value_pair) = maybe_entry
            .map_err(|error| Error::ReadBlockStorage { block_ref: block_ref.clone(), error, })?;

        match &iter_kind {
            IterKind::Items(SearchTreeIterItemsTx { range: SearchRangeBounds { range_from: Bound::Unbounded, .. }, .. }) =>
                (),
            IterKind::Items(SearchTreeIterItemsTx { range: SearchRangeBounds { range_from: Bound::Excluded(key), .. }, .. }) =>
                match key.key_bytes[..].cmp(&key_value_pair.key.key_bytes) {
                    Ordering::Less =>
                        (),
                    Ordering::Equal | Ordering::Greater =>
                        continue,
                },
            IterKind::Items(SearchTreeIterItemsTx { range: SearchRangeBounds { range_from: Bound::Included(key), .. }, .. }) =>
                match key.key_bytes[..].cmp(&key_value_pair.key.key_bytes) {
                    Ordering::Less | Ordering::Equal =>
                        (),
                    Ordering::Greater =>
                        continue,
                },
            IterKind::BlockRefs(..) =>
                (),
        }

        let maybe_jump_block_ref = match jump_ref {
            storage::JumpRef::None =>
                None,
            storage::JumpRef::Local(storage::LocalJumpRef { block_id, }) =>
                Some(BlockRef {
                    blockwheel_filename: block_ref.blockwheel_filename.clone(),
                    block_id: block_id.clone(),
                }),
            storage::JumpRef::External(storage::ExternalJumpRef { filename, block_id, }) =>
                Some(BlockRef {
                    blockwheel_filename: str::from_utf8(&*filename)
                        .map_err(|error| Error::FilenameUtf8 {
                            block_ref: block_ref.clone(),
                            error,
                        })?
                        .into(),
                    block_id: block_id.clone(),
                }),
        };
        if let Some(jump_block_ref) = maybe_jump_block_ref {
            match &mut iter_kind {

                IterKind::Items(SearchTreeIterItemsTx { range, items_tx, }) => {
                    let (reply_tx, reply_rx) = oneshot::channel();
                    let send_result = iter_rec_tx.send(IterRequest {
                        block_ref: jump_block_ref.clone(),
                        kind: IterRequestKind::Items { range: range.clone(), reply_tx, },
                    }).await;
                    if let Err(_send_error) = send_result {
                        log::warn!("search_tree has gone, terminating iter task");
                        return Err(Error::SearchTreeGone);
                    }
                    let mut items_rec_rx = match reply_rx.await {
                        Ok(SearchTreeIterItemsRx { items_rx }) =>
                            items_rx,
                        Err(oneshot::Canceled) => {
                            log::warn!("search_tree has gone, terminating iter task");
                            return Err(Error::SearchTreeGone);
                        },
                    };
                    loop {
                        match items_rec_rx.next().await {
                            None =>
                                return Err(Error::IterRecPeerLost),
                            Some(KeyValueStreamItem::NoMore) =>
                                break,
                            Some(KeyValueStreamItem::KeyValue(key_value_pair)) =>
                                if let Err(_send_error) = items_tx.send(KeyValueStreamItem::KeyValue(key_value_pair)).await {
                                    log::warn!("client canceled iter items request");
                                    return Ok(Done { block_ref, });
                                },
                        }
                    }
                },

                IterKind::BlockRefs(SearchTreeIterBlockRefsTx { block_refs_tx, }) => {
                    let (reply_tx, reply_rx) = oneshot::channel();
                    let send_result = iter_rec_tx.send(IterRequest {
                        block_ref: jump_block_ref.clone(),
                        kind: IterRequestKind::BlockRefs { reply_tx, },
                    }).await;
                    if let Err(_send_error) = send_result {
                        log::warn!("search_tree has gone, terminating iter task");
                        return Err(Error::SearchTreeGone);
                    }
                    let mut block_refs_rec_rx = match reply_rx.await {
                        Ok(SearchTreeIterBlockRefsRx { block_refs_rx }) =>
                            block_refs_rx,
                        Err(oneshot::Canceled) => {
                            log::warn!("search_tree has gone, terminating iter task");
                            return Err(Error::SearchTreeGone);
                        },
                    };
                    while let Some(block_ref_rec) = block_refs_rec_rx.next().await {
                        if let Err(_send_error) = block_refs_tx.send(block_ref_rec).await {
                            log::warn!("client canceled iter block_refs request");
                            return Ok(Done { block_ref, });
                        }
                    }
                },

            }
        }

        match &mut iter_kind {

            IterKind::Items(SearchTreeIterItemsTx { range, items_tx, }) => {
                match &range.range_to {
                    Bound::Unbounded =>
                        (),
                    Bound::Excluded(key) =>
                        match key.key_bytes[..].cmp(&key_value_pair.key.key_bytes) {
                            Ordering::Less | Ordering::Equal =>
                                break,
                            Ordering::Greater =>
                                (),
                        },
                    Bound::Included(key) =>
                        match key.key_bytes[..].cmp(&key_value_pair.key.key_bytes) {
                            Ordering::Less  =>
                                break,
                            Ordering::Equal | Ordering::Greater =>
                                (),
                        },
                }

                if let Err(_send_error) = items_tx.send(KeyValueStreamItem::KeyValue(key_value_pair)).await {
                    log::warn!("client canceled iter items request");
                    return Ok(Done { block_ref, });
                }
            },

            IterKind::BlockRefs(..) =>
                (),
        }
    }

    match &mut iter_kind {
        IterKind::Items(SearchTreeIterItemsTx { items_tx, .. }) =>
            if let Err(_send_error) = items_tx.send(KeyValueStreamItem::NoMore).await {
                log::warn!("client canceled iter items request");
                return Ok(Done { block_ref, });
            },
        IterKind::BlockRefs(SearchTreeIterBlockRefsTx { block_refs_tx, }) =>
            if let Err(_send_error) = block_refs_tx.send(block_ref.clone()).await {
                log::warn!("client canceled iter block_refs request");
                return Ok(Done { block_ref, });
            },
    }

    Ok(Done { block_ref, })
}
