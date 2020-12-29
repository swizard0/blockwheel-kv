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
    Unique,
    bytes::{
        Bytes,
    },
};

use crate::{
    job,
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

pub struct Args<J> where J: edeltraud::Job {
    pub block_ref: BlockRef,
    pub iter_request: IterRequest,
    pub iter_block_entries_pool: pool::Pool<Vec<BlockEntry>>,
    pub thread_pool: edeltraud::Edeltraud<J>,
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
    ThreadPoolGone,
}

pub type JobOutput = Result<JobDone, Error>;

pub struct JobArgs {
    block_ref: BlockRef,
    block_bytes: Bytes,
    maybe_search_range: Option<SearchRangeBounds>,
    iter_block_entries_pool: pool::Pool<Vec<BlockEntry>>,
}

pub struct JobDone {
    block_entries: Unique<Vec<BlockEntry>>,
}

pub fn job(JobArgs { block_ref, block_bytes, maybe_search_range, iter_block_entries_pool, }: JobArgs) -> JobOutput {
    let mut block_entries = iter_block_entries_pool.lend(Vec::new);
    block_entries.clear();

    let entries_iter = storage::block_deserialize_iter(&block_bytes)
        .map_err(|error| Error::ReadBlockStorage { block_ref: block_ref.clone(), error, })?;
    for maybe_entry in entries_iter {
        let (jump_ref, key_value_pair) = maybe_entry
            .map_err(|error| Error::ReadBlockStorage { block_ref: block_ref.clone(), error, })?;
        match &maybe_search_range {
            Some(SearchRangeBounds { range_from: Bound::Unbounded, .. }) =>
                (),
            Some(SearchRangeBounds { range_from: Bound::Excluded(key), .. }) =>
                match key.key_bytes[..].cmp(&key_value_pair.key.key_bytes) {
                    Ordering::Less =>
                        (),
                    Ordering::Equal | Ordering::Greater =>
                        continue,
                },
            Some(SearchRangeBounds { range_from: Bound::Included(key), .. }) =>
                match key.key_bytes[..].cmp(&key_value_pair.key.key_bytes) {
                    Ordering::Less | Ordering::Equal =>
                        (),
                    Ordering::Greater =>
                        continue,
                },
            None =>
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
                    blockwheel_filename: filename.into(),
                    block_id: block_id.clone(),
                }),
        };

        let force_stop = match &maybe_search_range {
            Some(SearchRangeBounds { range_to: Bound::Unbounded, .. }) =>
                false,
            Some(SearchRangeBounds { range_to: Bound::Excluded(key), .. }) =>
                match key.key_bytes[..].cmp(&key_value_pair.key.key_bytes) {
                    Ordering::Less | Ordering::Equal =>
                        true,
                    Ordering::Greater =>
                        false,
                },
            Some(SearchRangeBounds { range_to: Bound::Included(key), .. }) =>
                match key.key_bytes[..].cmp(&key_value_pair.key.key_bytes) {
                    Ordering::Less  =>
                        true,
                    Ordering::Equal | Ordering::Greater =>
                        false,
                },
            None =>
                false,
        };

        match (maybe_jump_block_ref, force_stop) {
            (None, true) =>
                break,
            (None, false) =>
                block_entries.push(BlockEntry::OnlyEntry(key_value_pair)),
            (Some(jump_block_ref), true) => {
                block_entries.push(BlockEntry::OnlyJump(jump_block_ref));
                break;
            },
            (Some(jump_block_ref), false) =>
                block_entries.push(BlockEntry::JumpAndEntry {
                    jump: jump_block_ref,
                    key_value_pair,
                }),
        }
    }

    Ok(JobDone { block_entries, })
}

pub async fn run<J>(
    Args {
        block_ref,
        iter_request,
        iter_block_entries_pool,
        thread_pool,
        block_bytes,
        mut iter_rec_tx,
        iter_send_buffer,
    }: Args<J>,
)
    -> Result<Done, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
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

    let job_task = thread_pool.spawn(job::Job::SearchTreeIterBlock(JobArgs {
        block_ref: block_ref.clone(),
        block_bytes: block_bytes.clone(),
        maybe_search_range: match &iter_kind {
            IterKind::Items(SearchTreeIterItemsTx { range, .. }) =>
                Some(range.clone()),
            IterKind::BlockRefs(..) =>
                None,
        },
        iter_block_entries_pool,
    }));
    let job_output = job_task.await
        .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
    let job_output: job::JobOutput = job_output.into();
    let job::SearchTreeIterBlockDone(job_result) = job_output.into();
    let JobDone { mut block_entries, } = job_result?;

    for block_entry_action in block_entries.drain(..) {

        match &block_entry_action {
            BlockEntry::OnlyJump(jump_block_ref) | BlockEntry::JumpAndEntry { jump: jump_block_ref, .. } => {
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
            },
            BlockEntry::OnlyEntry(..) =>
                (),
        }

        match &block_entry_action {
            BlockEntry::OnlyEntry(key_value_pair) | BlockEntry::JumpAndEntry { key_value_pair, .. } =>
                match &mut iter_kind {
                    IterKind::Items(SearchTreeIterItemsTx { items_tx, .. }) => {
                        if let Err(_send_error) = items_tx.send(KeyValueStreamItem::KeyValue(key_value_pair.clone())).await {
                            log::warn!("client canceled iter items request");
                            return Ok(Done { block_ref, });
                        }
                    },

                    IterKind::BlockRefs(..) =>
                        (),
                },
            BlockEntry::OnlyJump(..) =>
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
