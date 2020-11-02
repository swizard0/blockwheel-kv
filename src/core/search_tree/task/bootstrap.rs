use std::sync::Arc;

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
    let mut block_bytes = blocks_pool.lend();
    let serialize_task = tokio::task::spawn_blocking(move || {
        for (key, &()) in cache.iter() {
            storage::serialize_key_value(&key, storage::JumpRef::None, &mut block_bytes)?;
        }
        Ok(block_bytes)
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
