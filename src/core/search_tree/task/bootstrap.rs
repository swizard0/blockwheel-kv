use std::sync::Arc;

use futures::{
    stream::{
        FuturesUnordered,
    },
    StreamExt,
};

use alloc_pool::bytes::{
    Bytes,
    BytesPool,
};

use crate::{
    kv,
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
    pub values_inline_size_limit: usize,
}

pub struct Done {
    pub block_ref: BlockRef,
}

#[derive(Debug)]
pub enum Error {
    WheelsGone,
    WheelsEmpty,
    SerializeBlockStorage(storage::Error),
    SerializeValueBlockStorage(storage::Error),
    WriteBlock(blockwheel::WriteBlockError),
    ThreadPoolGone,
}

pub type JobOutput = Result<JobDone, Error>;

pub struct JobArgs {
    entries: Vec<Option<storage::OwnedEntry<wheels::WheelFilename>>>,
    blocks_pool: BytesPool,
}

pub struct JobDone {
    block_bytes: Bytes,
}

pub fn job(JobArgs { entries, blocks_pool, }: JobArgs) -> JobOutput {
    let block_bytes = blocks_pool.lend();
    let mut kont = storage::BlockSerializer::start(
        storage::NodeType::Root { tree_entries_count: entries.len(), },
        entries.len(),
        block_bytes,
    ).map_err(Error::SerializeBlockStorage)?;
    let mut entries_iter = entries.into_iter();
    loop {
        match kont {
            storage::BlockSerializerContinue::Done(block_bytes) =>
                return Ok(JobDone { block_bytes: block_bytes.freeze(), }),
            storage::BlockSerializerContinue::More(serializer) => {
                let storage::OwnedEntry { jump_ref, key, value_cell, } = entries_iter
                    .next()
                    .unwrap()
                    .unwrap();
                kont = serializer.entry(&key, value_cell.as_ref(), jump_ref.as_ref())
                    .map_err(Error::SerializeBlockStorage)?;
            },
        }
    }
}

pub async fn run<J>(
    Args {
        cache,
        thread_pool,
        blocks_pool,
        mut wheels_pid,
        values_inline_size_limit,
    }: Args<J>,
)
    -> Result<Done, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    let mut tasks = FuturesUnordered::new();
    let mut entries = Vec::with_capacity(cache.len());
    for (key, value_cell) in cache.iter() {
        let maybe_entry = match value_cell.cell {
            kv::Cell::Value(kv::Value { ref value_bytes, }) if value_bytes.len() > values_inline_size_limit => {
                let entry_index = entries.len();
                let value_block = value_bytes.clone();
                let key = key.as_ref().clone();
                let version = value_cell.version;
                let mut wheels_pid = wheels_pid.clone();
                let mut block_bytes = blocks_pool.lend();
                tasks.push(async move {
                    let mut wheel_ref = wheels_pid.acquire().await
                        .map_err(|ero::NoProcError| Error::WheelsGone)?
                        .ok_or(Error::WheelsEmpty)?;
                    let blockwheel_filename = wheel_ref.blockwheel_filename.clone();
                    storage::value_block_serialize(&value_block, &mut block_bytes)
                        .map_err(Error::SerializeValueBlockStorage)?;
                    let block_id = wheel_ref.blockwheel_pid.write_block(block_bytes.freeze()).await
                        .map_err(Error::WriteBlock)?;
                    Ok((entry_index, key, version, blockwheel_filename, block_id))
                });
                None
            },
            kv::Cell::Value(..) | kv::Cell::Tombstone =>
                Some(storage::OwnedEntry {
                    jump_ref: storage::OwnedJumpRef::None,
                    key: key.as_ref().clone(),
                    value_cell: value_cell.clone().into(),
                }),
        };
        entries.push(maybe_entry);
    }

    let mut wheel_ref = wheels_pid.acquire().await
        .map_err(|ero::NoProcError| Error::WheelsGone)?
        .ok_or(Error::WheelsEmpty)?;

    while let Some(task_result) = tasks.next().await {
        let (entry_index, key, version, blockwheel_filename, block_id) = task_result?;

        entries[entry_index] = Some(storage::OwnedEntry {
            jump_ref: storage::OwnedJumpRef::None,
            key,
            value_cell: storage::OwnedValueCell {
                version,
                cell: storage::OwnedCell::Value(
                    if blockwheel_filename == wheel_ref.blockwheel_filename {
                        storage::OwnedValueRef::Local { block_id, }
                    } else {
                        storage::OwnedValueRef::External {
                            filename: blockwheel_filename,
                            block_id,
                        }
                    },
                ),
            },
        });
    }

    let job_output = thread_pool.spawn(job::Job::SearchTreeBootstrap(JobArgs { entries, blocks_pool, })).await
        .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
    let job_output: job::JobOutput = job_output.into();
    let job::SearchTreeBootstrapDone(job_result) = job_output.into();
    let JobDone { block_bytes, } = job_result?;

    let block_id = wheel_ref.blockwheel_pid.write_block(block_bytes).await
        .map_err(Error::WriteBlock)?;
    Ok(Done {
        block_ref: BlockRef {
            blockwheel_filename: wheel_ref.blockwheel_filename,
            block_id,
        },
    })
}
