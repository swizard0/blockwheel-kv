use alloc_pool::bytes::Bytes;

use crate::{
    wheels,
    blockwheel,
    core::{
        BlockRef,
    },
};

pub struct Args {
    pub block_ref: BlockRef,
    pub wheels_pid: wheels::Pid,
}

pub struct Done {
    pub block_ref: BlockRef,
    pub block_bytes: Bytes,
}

#[derive(Debug)]
pub enum Error {
    WheelsGone,
    WheelNotFound {
        blockwheel_filename: wheels::WheelFilename,
    },
    ReadBlock(blockwheel::ReadBlockError),
}

pub async fn run(Args { block_ref, mut wheels_pid, }: Args) -> Result<Done, Error> {
    let mut wheel_ref = wheels_pid.get(block_ref.blockwheel_filename.clone()).await
        .map_err(|ero::NoProcError| Error::WheelsGone)?
        .ok_or_else(|| Error::WheelNotFound {
            blockwheel_filename: block_ref.blockwheel_filename.clone(),
        })?;
    let block_bytes = wheel_ref.blockwheel_pid.read_block(block_ref.block_id.clone()).await
        .map_err(Error::ReadBlock)?;
    Ok(Done { block_ref, block_bytes, })
}
