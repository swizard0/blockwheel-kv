use std::sync::Arc;

use futures::{
    channel::{
        oneshot,
    },
};

use alloc_pool::bytes::BytesPool;

use crate::{
    wheels,
    storage,
    blockwheel,
    core::{
        BlockRef,
        MemCache,
    },
};

pub struct Args {
    pub cache: Arc<MemCache>,
    pub thread_pool: Arc<rayon::ThreadPool>,
    pub blocks_pool: BytesPool,
    pub wheels_pid: wheels::Pid,
}

pub struct Done {
    pub block_ref: BlockRef,
}

#[derive(Debug)]
pub enum Error {
    WheelsGone,
    WheelsEmpty,
    SerializeBlockStorage(storage::Error),
    SerializeBlockTaskGone,
    WriteBlock(blockwheel::WriteBlockError),
}

pub async fn run(Args { cache, thread_pool, blocks_pool, mut wheels_pid, }: Args) -> Result<Done, Error> {
    let block_bytes = blocks_pool.lend();
    let serialize_task = move || {
        let mut kont = storage::BlockSerializer::start(
            storage::NodeType::Root { tree_entries_count: cache.len(), },
            cache.len(),
            block_bytes,
        )?;
        let mut cache_iter = cache.iter();
        loop {
            match kont {
                storage::BlockSerializerContinue::Done(block_bytes) =>
                    return Ok(block_bytes),
                storage::BlockSerializerContinue::More(serializer) => {
                    let (key, value_cell) = cache_iter.next().unwrap();
                    kont = serializer.entry::<&'_ [u8]>(&key, &value_cell, storage::JumpRef::None)?;
                },
            }
        }
    };
    let (serialize_task_tx, serialize_task_rx) = oneshot::channel();
    thread_pool.install(move || serialize_task_tx.send(serialize_task()).ok());

    let block_bytes = serialize_task_rx.await
        .map_err(|oneshot::Canceled| Error::SerializeBlockTaskGone)?
        .map_err(Error::SerializeBlockStorage)?;
    let mut wheel_ref = wheels_pid.acquire().await
        .map_err(|ero::NoProcError| Error::WheelsGone)?
        .ok_or(Error::WheelsEmpty)?;
    let block_id = wheel_ref.blockwheel_pid.write_block(block_bytes.freeze()).await
        .map_err(Error::WriteBlock)?;
    Ok(Done {
        block_ref: BlockRef {
            blockwheel_filename: wheel_ref.blockwheel_filename,
            block_id,
        },
    })
}
