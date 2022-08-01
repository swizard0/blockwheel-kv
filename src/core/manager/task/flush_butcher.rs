use std::{
    sync::{
        Arc,
    },
};

use o1::{
    set::{
        Ref,
    },
};

use crate::{
    job,
    core::{
        search_tree_builder,
        MemCache,
        BlockRef,
    },
};

pub struct Args<J> where J: edeltraud::Job {
    pub search_tree_ref: Ref,
    pub frozen_memcache: Arc<MemCache>,
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
    Args { search_tree_ref, frozen_memcache, thread_pool, }: Args<J>,
)
    -> Result<Done, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{

    todo!()
}
