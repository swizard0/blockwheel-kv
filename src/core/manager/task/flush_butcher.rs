use std::{
    sync::{
        Arc,
    },
};

use alloc_pool::{
    pool,
    Unique,
    bytes::{
        Bytes,
    },
};

use o1::{
    set::{
        Ref,
    },
};

use crate::{
    kv,
    job,
    storage,
    core::{
        search_tree_builder,
        MemCache,
        BlockRef,
        SearchTreeBuilderCps,
        SearchTreeBuilderBlockEntry,
    },
};

pub struct Args<J> where J: edeltraud::Job {
    pub search_tree_ref: Ref,
    pub frozen_memcache: Arc<MemCache>,
    pub block_entries_pool: pool::Pool<Vec<SearchTreeBuilderBlockEntry>>,
    pub search_tree_builder_params: search_tree_builder::Params,
    pub thread_pool: edeltraud::Edeltraud<J>,
}

pub struct Done {
    pub search_tree_ref: Ref,
    pub root_block: BlockRef,
}

#[derive(Debug)]
pub enum Error {
    ThreadPoolGone,
    SearchTreeBuilder(search_tree_builder::Error),
}

pub async fn run<J>(
    Args {
        search_tree_ref,
        frozen_memcache,
        block_entries_pool,
        search_tree_builder_params,
        thread_pool,
    }: Args<J>,
)
    -> Result<Done, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    let mut job_args = JobArgs {
        env: Env::default(),
        kont: Kont::Start {
            frozen_memcache,
            builder: search_tree_builder::BuilderCps::new(
                block_entries_pool,
                search_tree_builder_params,
            ),
        },
    };
    loop {
        let job = job::Job::ManagerTaskFlushButcher(job_args);
        let job_output = thread_pool.spawn(job).await
            .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
        let job_output: job::JobOutput = job_output.into();
        let job::ManagerTaskFlushButcherDone(job_done) = job_output.into();

        todo!()
    }
}

pub struct JobArgs {
    env: Env,
    kont: Kont,
}

#[derive(Default)]
pub struct Env {
    write_block_tasks: Vec<WriteBlockTask>,
}

pub enum Kont {
    Start {
        frozen_memcache: Arc<MemCache>,
        builder: SearchTreeBuilderCps,
    },
}

pub enum JobDone {
    AwaitWriteBlockTasks {
        next: search_tree_builder::KontPollProcessedBlockNext<kv::KeyValuePair<storage::OwnedValueBlockRef>, BlockRef>,
    },
    Finished {
        root_block: BlockRef,
    },
}

struct WriteBlockTask {
    node_type: storage::NodeType,
    async_ref: Ref,
    data: WriteBlockTaskData,
}

enum WriteBlockTaskData {
    Entries {
        block_entries: Unique<Vec<SearchTreeBuilderBlockEntry>>,
    },
    Bytes {
        block_bytes: Bytes,
    },
}

pub type Output = Result<JobDone, Error>;

pub fn job(JobArgs { env, kont, }: JobArgs) -> Output {
    match kont {
        Kont::Start { frozen_memcache, builder, } =>
            job_build(env, frozen_memcache, builder),
    }
}

fn job_build(mut env: Env, frozen_memcache: Arc<MemCache>, builder: SearchTreeBuilderCps) -> Output {
    assert!(env.write_block_tasks.is_empty());

    let mut memcache_iter = frozen_memcache.iter();

    let mut builder_kont = builder.step()
        .map_err(Error::SearchTreeBuilder)?;
    loop {
        match builder_kont {
            search_tree_builder::Kont::PollNextItemOrProcessedBlock(
                search_tree_builder::KontPollNextItemOrProcessedBlock { next, },
            ) => {
                let (ord_key, value_cell) = memcache_iter.next().unwrap();
                let item = kv::KeyValuePair {
                    key: ord_key.as_ref().clone(),
                    value_cell: value_cell.clone().into(),
                };
                builder_kont = next.item_arrived(item)
                    .map_err(Error::SearchTreeBuilder)?;
            },
            search_tree_builder::Kont::PollProcessedBlock(
                search_tree_builder::KontPollProcessedBlock { next, },
            ) => {
                assert!(memcache_iter.next().is_none());
                return Ok(JobDone::AwaitWriteBlockTasks { next, });
            }
            search_tree_builder::Kont::ProcessBlockAsync(
                search_tree_builder::KontProcessBlockAsync { node_type, block_entries, async_ref, next, },
            ) => {
                env.write_block_tasks.push(WriteBlockTask {
                    node_type,
                    async_ref,
                    data: WriteBlockTaskData::Entries { block_entries, },
                });
                builder_kont = next.process_scheduled()
                    .map_err(Error::SearchTreeBuilder)?;
            },
            search_tree_builder::Kont::Finished { .. } =>
                unreachable!("totally unexpected Kont::Finished during search tree building in butcher flush"),
        }
    }
}
