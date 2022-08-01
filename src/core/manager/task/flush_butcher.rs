use std::{
    sync::{
        Arc,
    },
};

use alloc_pool::{
    pool,
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
}

pub async fn run<J>(
    Args {
        search_tree_ref,
        frozen_memcache,
        block_entries_pool,
        search_tree_builder_params,
        mut thread_pool,
    }: Args<J>,
)
    -> Result<Done, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    let mut job_args = JobArgs {
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
    kont: Kont,
}

pub enum Kont {
    Start {
        frozen_memcache: Arc<MemCache>,
        builder: SearchTreeBuilderCps,
    },
}

pub enum JobDone {
}

pub type Output = Result<JobDone, Error>;

pub fn job(JobArgs { kont, }: JobArgs) -> Output {

    todo!()
}
