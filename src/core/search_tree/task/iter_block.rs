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
};

use alloc_pool::{
    pool,
    Unique,
    bytes::{
        Bytes,
    },
};

use crate::{
    kv,
    job,
    storage,
    core::{
        search_tree::{
            task::{
                IterRequest,
                IterRecRequest,
                IterRequestData,
                BlockEntry,
            },
            KeyValueRef,
        },
        BlockRef,
        SearchRangeBounds,
    },
};

pub struct Args<J> where J: edeltraud::Job {
    pub iter_request: IterRequest,
    pub iter_block_entries_pool: pool::Pool<Vec<BlockEntry>>,
    pub thread_pool: edeltraud::Edeltraud<J>,
    pub block_bytes: Bytes,
    pub iter_rec_tx: mpsc::Sender<IterRecRequest>,
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
    search_range: SearchRangeBounds,
    iter_block_entries_pool: pool::Pool<Vec<BlockEntry>>,
}

pub struct JobDone {
    block_entries: Unique<Vec<BlockEntry>>,
}

pub fn job(JobArgs { block_ref, block_bytes, search_range, iter_block_entries_pool, }: JobArgs) -> JobOutput {
    let mut block_entries = iter_block_entries_pool.lend(Vec::new);
    block_entries.clear();

    let entries_iter = storage::block_deserialize_iter(&block_bytes)
        .map_err(|error| Error::ReadBlockStorage { block_ref: block_ref.clone(), error, })?;
    for maybe_entry in entries_iter {
        let iter_entry = maybe_entry
            .map_err(|error| Error::ReadBlockStorage { block_ref: block_ref.clone(), error, })?;
        match &search_range {
            SearchRangeBounds { range_from: Bound::Unbounded, .. } =>
                (),
            SearchRangeBounds { range_from: Bound::Excluded(key), .. } =>
                match key.key_bytes[..].cmp(iter_entry.key) {
                    Ordering::Less =>
                        (),
                    Ordering::Equal | Ordering::Greater =>
                        continue,
                },
            SearchRangeBounds { range_from: Bound::Included(key), .. } =>
                match key.key_bytes[..].cmp(iter_entry.key) {
                    Ordering::Less | Ordering::Equal =>
                        (),
                    Ordering::Greater =>
                        continue,
                },
        }

        let owned_jump_ref = storage::OwnedJumpRef::from_jump_ref(&iter_entry.jump_ref, &block_bytes);
        let maybe_jump_block_ref = match owned_jump_ref {
            storage::OwnedJumpRef::None =>
                None,
            storage::OwnedJumpRef::Local(storage::LocalRef { block_id, }) =>
                Some(BlockRef {
                    blockwheel_filename: block_ref.blockwheel_filename.clone(),
                    block_id: block_id.clone(),
                }),
            storage::OwnedJumpRef::External(block_ref) =>
                Some(block_ref),
        };

        let force_stop = match &search_range {
            SearchRangeBounds { range_to: Bound::Unbounded, .. } =>
                false,
            SearchRangeBounds { range_to: Bound::Excluded(key), .. } =>
                match key.key_bytes[..].cmp(iter_entry.key) {
                    Ordering::Less | Ordering::Equal =>
                        true,
                    Ordering::Greater =>
                        false,
                },
            SearchRangeBounds { range_to: Bound::Included(key), .. } =>
                match key.key_bytes[..].cmp(iter_entry.key) {
                    Ordering::Less  =>
                        true,
                    Ordering::Equal | Ordering::Greater =>
                        false,
                },
        };

        let owned_entry = storage::OwnedEntry::from_entry(&iter_entry, &block_bytes);
        let key = owned_entry.key;
        let value_cell = match owned_entry.value_cell {
            kv::ValueCell { version, cell: kv::Cell::Value(value_ref), } =>
                kv::ValueCell {
                    version,
                    cell: kv::Cell::Value(storage::OwnedValueBlockRef::from_owned_value_ref(
                        value_ref,
                        &block_ref.blockwheel_filename,
                    )),
                },
            kv::ValueCell { version, cell: kv::Cell::Tombstone, } =>
                kv::ValueCell { version, cell: kv::Cell::Tombstone, },
        };

        match (maybe_jump_block_ref, force_stop) {
            (None, true) =>
                break,
            (None, false) =>
                block_entries.push(BlockEntry::OnlyEntry { key, value_cell, }),
            (Some(jump_block_ref), true) => {
                block_entries.push(BlockEntry::OnlyJump(jump_block_ref));
                break;
            },
            (Some(jump_block_ref), false) =>
                block_entries.push(BlockEntry::JumpAndEntry {
                    jump: jump_block_ref,
                    key,
                    value_cell,
                }),
        }
    }

    block_entries.shrink_to_fit();
    Ok(JobDone { block_entries, })
}

pub async fn run<J>(
    Args {
        iter_request: IterRequest {
            block_ref,
            data: IterRequestData {
                range,
                mut iter_items_tx,
                repay_iter_items_tx,
            },
        },
        iter_block_entries_pool,
        thread_pool,
        block_bytes,
        mut iter_rec_tx,
    }: Args<J>,
)
    -> Result<Done, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    let job_task = thread_pool.spawn(job::Job::SearchTreeIterBlock(JobArgs {
        block_ref: block_ref.clone(),
        block_bytes: block_bytes.clone(),
        search_range: range.clone(),
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
                let (repay_iter_items_tx, repay_iter_items_rx) = oneshot::channel();
                let send_result = iter_rec_tx.send(IterRecRequest {
                    maybe_block_ref: Some(jump_block_ref.clone()),
                    data: IterRequestData {
                        range: range.clone(),
                        iter_items_tx,
                        repay_iter_items_tx,
                    },
                }).await;
                if let Err(_send_error) = send_result {
                    log::warn!("search_tree has gone, terminating iter task");
                    return Err(Error::SearchTreeGone);
                }
                match repay_iter_items_rx.await {
                    Ok(repayed_iter_items_tx) =>
                        iter_items_tx = repayed_iter_items_tx,
                    Err(oneshot::Canceled) => {
                        log::warn!("client canceled recursive iter items request");
                        return Ok(Done { block_ref, })
                    },
                }
            },
            BlockEntry::OnlyEntry { .. } =>
                (),
        }

        match block_entry_action {
            BlockEntry::OnlyEntry { key, value_cell, } | BlockEntry::JumpAndEntry { key, value_cell, .. } =>
                if let Err(_send_error) = iter_items_tx.items_tx.send(KeyValueRef::Item { key, value_cell, }).await {
                    log::warn!("client canceled iter items request");
                    return Ok(Done { block_ref, });
                },
            BlockEntry::OnlyJump(..) =>
                (),
        }
    }

    if let Err(_send_error) = iter_items_tx.items_tx.send(KeyValueRef::BlockFinish(block_ref.clone())).await {
        log::warn!("client canceled iter items request on BlockFinish");
    }

    if let Err(_send_error) = repay_iter_items_tx.send(iter_items_tx) {
        log::warn!("client canceled iter items request on repay");
    }

    Ok(Done { block_ref, })
}
