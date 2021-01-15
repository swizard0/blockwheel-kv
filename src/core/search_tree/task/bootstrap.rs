use std::{
    mem,
    sync::Arc,
};

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

pub type LayoutJobOutput = Result<LayoutJobDone, Error>;

pub struct LayoutJobArgs {
    cache: Arc<MemCache>,
    blocks_pool: BytesPool,
    values_inline_size_limit: usize,
}

pub struct LayoutJobDone {
    layout_ops: Vec<LayoutOp>,
}

enum LayoutOp {
    Ready(storage::OwnedEntry),
    WriteExternalValue {
        key: kv::Key,
        value_block_bytes: Bytes,
        value_version: u64,
    },
}

pub fn layout_job(LayoutJobArgs { cache, blocks_pool, values_inline_size_limit, }: LayoutJobArgs) -> LayoutJobOutput {
    let mut layout_ops = Vec::with_capacity(cache.len());
    for (key, value_cell) in cache.iter() {
        let layout_op = match value_cell.cell {
            kv::Cell::Value(kv::Value { ref value_bytes, }) if value_bytes.len() > values_inline_size_limit => {
                let mut block_bytes = blocks_pool.lend();
                storage::value_block_serialize(value_bytes, &mut block_bytes)
                    .map_err(Error::SerializeValueBlockStorage)?;
                LayoutOp::WriteExternalValue {
                    key: key.as_ref().clone(),
                    value_block_bytes: block_bytes.freeze(),
                    value_version: value_cell.version,
                }
            },
            kv::Cell::Value(..) | kv::Cell::Tombstone =>
                LayoutOp::Ready(storage::OwnedEntry {
                    jump_ref: storage::OwnedJumpRef::None,
                    key: key.as_ref().clone(),
                    value_cell: value_cell.clone().into(),
                }),
        };
        layout_ops.push(layout_op);
    }

    Ok(LayoutJobDone { layout_ops, })
}


pub type BlockJobOutput = Result<BlockJobDone, Error>;

pub struct BlockJobArgs {
    layout_ops: Vec<LayoutOp>,
    blocks_pool: BytesPool,
}

pub struct BlockJobDone {
    block_bytes: Bytes,
}

pub fn block_job(BlockJobArgs { layout_ops, blocks_pool, }: BlockJobArgs) -> BlockJobOutput {
    let block_bytes = blocks_pool.lend();
    let mut kont = storage::BlockSerializer::start(
        storage::NodeType::Root { tree_entries_count: layout_ops.len(), },
        layout_ops.len(),
        block_bytes,
    ).map_err(Error::SerializeBlockStorage)?;
    let mut layout_ops_iter = layout_ops.into_iter();
    loop {
        match kont {
            storage::BlockSerializerContinue::Done(block_bytes) =>
                return Ok(BlockJobDone { block_bytes: block_bytes.freeze(), }),
            storage::BlockSerializerContinue::More(serializer) =>
                match layout_ops_iter.next() {
                    Some(LayoutOp::Ready(ref owned_entry)) => {
                        kont = serializer.entry(owned_entry.into())
                            .map_err(Error::SerializeBlockStorage)?;
                    },
                    _ =>
                        unreachable!(),
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
    let layout_job_args = LayoutJobArgs {
        cache,
        blocks_pool: blocks_pool.clone(),
        values_inline_size_limit,
    };
    let layout_job_output = thread_pool.spawn(job::Job::SearchTreeBootstrapLayout(layout_job_args)).await
        .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
    let layout_job_output: job::JobOutput = layout_job_output.into();
    let job::SearchTreeBootstrapLayoutDone(layout_job_result) = layout_job_output.into();
    let LayoutJobDone { mut layout_ops, } = layout_job_result?;

    let mut tasks = FuturesUnordered::new();
    for (layout_op_index, layout_op) in layout_ops.iter().enumerate() {
        match layout_op {
            LayoutOp::Ready(..) =>
                (),
            &LayoutOp::WriteExternalValue { ref key, ref value_block_bytes, value_version, } => {
                let mut wheels_pid = wheels_pid.clone();
                let block_bytes = value_block_bytes.clone();
                let key = key.clone();
                tasks.push(async move {
                    let mut wheel_ref = wheels_pid.acquire().await
                        .map_err(|ero::NoProcError| Error::WheelsGone)?
                        .ok_or(Error::WheelsEmpty)?;
                    let blockwheel_filename = wheel_ref.blockwheel_filename.clone();
                    let block_id = wheel_ref.blockwheel_pid.write_block(block_bytes).await
                        .map_err(Error::WriteBlock)?;
                    Ok::<_, Error>((layout_op_index, key, value_version, blockwheel_filename, block_id))
                });
            },
        }
    }

    let mut wheel_ref = wheels_pid.acquire().await
        .map_err(|ero::NoProcError| Error::WheelsGone)?
        .ok_or(Error::WheelsEmpty)?;

    while let Some(task_result) = tasks.next().await {
        let (layout_op_index, key, version, blockwheel_filename, block_id) = task_result?;
        let owned_entry = storage::OwnedEntry {
            jump_ref: storage::OwnedJumpRef::None,
            key: key.clone(),
            value_cell: kv::ValueCell {
                version,
                cell: kv::Cell::Value(
                    if blockwheel_filename == wheel_ref.blockwheel_filename {
                        storage::OwnedValueRef::Local(storage::LocalRef { block_id, })
                    } else {
                        storage::OwnedValueRef::External(BlockRef {
                            blockwheel_filename,
                            block_id,
                        })
                    },
                ),
            },
        };
        let prev_layout_op = mem::replace(&mut layout_ops[layout_op_index], LayoutOp::Ready(owned_entry));
        assert!(matches!(prev_layout_op, LayoutOp::WriteExternalValue { .. }));
    }

    let block_job_output = thread_pool.spawn(job::Job::SearchTreeBootstrapBlock(BlockJobArgs { layout_ops, blocks_pool, })).await
        .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
    let block_job_output: job::JobOutput = block_job_output.into();
    let job::SearchTreeBootstrapBlockDone(block_job_result) = block_job_output.into();
    let BlockJobDone { block_bytes, } = block_job_result?;

    let block_id = wheel_ref.blockwheel_pid.write_block(block_bytes).await
        .map_err(Error::WriteBlock)?;
    Ok(Done {
        block_ref: BlockRef {
            blockwheel_filename: wheel_ref.blockwheel_filename,
            block_id,
        },
    })
}
