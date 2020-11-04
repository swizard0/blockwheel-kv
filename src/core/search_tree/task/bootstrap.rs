use std::sync::Arc;

use alloc_pool::bytes::BytesPool;

use crate::{
    wheels,
    storage,
    blockwheel,
    core::{
        BlockRef,
        MemCache,
        ValueCell,
    },
};

pub struct Args {
    pub cache: Arc<MemCache>,
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
    SerializeBlockJoin(tokio::task::JoinError),
    WriteBlock(blockwheel::WriteBlockError),
}

pub async fn run(Args { cache, blocks_pool, mut wheels_pid, }: Args) -> Result<Done, Error> {
    let block_bytes = blocks_pool.lend();
    let serialize_task = tokio::task::spawn_blocking(move || {
        let mut kont = storage::BlockSerializer::start(
            storage::NodeType::Root,
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
                    let storage_value_cell = match &value_cell {
                        ValueCell::Value(value) =>
                            storage::ValueCell::Value { value: &value.value_bytes, },
                        ValueCell::Tombstone =>
                            storage::ValueCell::Tombstone,
                        ValueCell::Blackmark =>
                            storage::ValueCell::Blackmark,
                    };
                    kont = serializer.entry(&key.key_bytes, storage_value_cell, storage::JumpRef::None)?;
                },
            }
        }
    });
    let block_bytes = serialize_task.await
        .map_err(Error::SerializeBlockJoin)?
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
