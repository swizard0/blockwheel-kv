use std::sync::Arc;

use alloc_pool::bytes::{
    Bytes,
    BytesPool,
};

use crate::{
    job,
    wheels,
    storage,
    blockwheel,
    core::{
        BlockRef,
        MemCache,
    },
};

pub struct Args<J> where J: edeltraud::Job {
    pub cache: Arc<MemCache>,
    pub thread_pool: edeltraud::Edeltraud<J>,
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
    WriteBlock(blockwheel::WriteBlockError),
    ThreadPoolGone,
}

pub type JobOutput = Result<JobDone, Error>;

pub struct JobArgs {
    cache: Arc<MemCache>,
    blocks_pool: BytesPool,
}

pub struct JobDone {
    block_bytes: Bytes,
}

pub fn job(JobArgs { cache, blocks_pool, }: JobArgs) -> JobOutput {
    let block_bytes = blocks_pool.lend();
    let mut kont = storage::BlockSerializer::start(
        storage::NodeType::Root { tree_entries_count: cache.len(), },
        cache.len(),
        block_bytes,
    ).map_err(Error::SerializeBlockStorage)?;
    let mut cache_iter = cache.iter();
    loop {
        match kont {
            storage::BlockSerializerContinue::Done(block_bytes) =>
                return Ok(JobDone { block_bytes: block_bytes.freeze(), }),
            storage::BlockSerializerContinue::More(serializer) => {
                let (key, value_cell) = cache_iter.next().unwrap();
                kont = serializer.entry(&key, &value_cell, storage::JumpRef::None)
                    .map_err(Error::SerializeBlockStorage)?;
            },
        }
    }
}

pub async fn run<J>(Args { cache, thread_pool, blocks_pool, mut wheels_pid, }: Args<J>) -> Result<Done, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    let job_output = thread_pool.spawn(job::Job::SearchTreeBootstrap(JobArgs { cache, blocks_pool, })).await
        .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
    let job_output: job::JobOutput = job_output.into();
    let job::SearchTreeBootstrapDone(job_result) = job_output.into();
    let JobDone { block_bytes, } = job_result?;

    let mut wheel_ref = wheels_pid.acquire().await
        .map_err(|ero::NoProcError| Error::WheelsGone)?
        .ok_or(Error::WheelsEmpty)?;
    let block_id = wheel_ref.blockwheel_pid.write_block(block_bytes).await
        .map_err(Error::WriteBlock)?;
    Ok(Done {
        block_ref: BlockRef {
            blockwheel_filename: wheel_ref.blockwheel_filename,
            block_id,
        },
    })
}
