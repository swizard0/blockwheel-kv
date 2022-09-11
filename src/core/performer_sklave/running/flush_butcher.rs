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

use alloc_pool::{
    pool,
    bytes::{
        BytesPool,
    },
};

use arbeitssklave::{
    komm,
};

use crate::{
    kv,
    job,
    wheels,
    core::{
        performer_sklave,
        search_tree_builder,
        MemCache,
        BlockRef,
        SearchTreeBuilderBlockEntry,
    },
    AccessPolicy,
};

pub enum Order {
    Terminate,
    WriteValue(OrderWriteValue),
    WriteBlock(OrderWriteBlock),
}

pub struct OrderWriteValue {
    pub block_ref: BlockRef,
    pub async_ref: Ref,
    pub block_entry_index: usize,
}

pub struct OrderWriteBlock {
    pub block_ref: BlockRef,
    pub async_ref: Ref,
}

pub struct Welt<A> where A: AccessPolicy {
    pub kont: Option<Kont>,
    pub search_tree_id: u64,
    pub meister_ref: o1::set::Ref,
    pub sendegeraet: komm::Sendegeraet<performer_sklave::Order<A>>,
    pub wheels: wheels::Wheels<A>,
    pub blocks_pool: BytesPool,
    pub block_entries_pool: pool::Pool<Vec<SearchTreeBuilderBlockEntry>>,
    pub drop_bomb: komm::Rueckkopplung<performer_sklave::Order<A>, performer_sklave::FlushButcherDrop>,
    pub incoming_orders: Vec<Order>,
    pub delayed_orders: Vec<Order>,
    pub search_tree_builder_params: search_tree_builder::Params,
    pub values_inline_size_limit: usize,
}

pub type Meister<A> = arbeitssklave::Meister<Welt<A>, Order>;
pub type SklaveJob<A> = arbeitssklave::SklaveJob<Welt<A>, Order>;

pub enum Kont {
    Start {
        frozen_memcache: Arc<MemCache>,
    },
}

#[derive(Debug)]
enum Error {
}

pub fn run_job<A, P>(sklave_job: SklaveJob<A>, thread_pool: &P)
where A: AccessPolicy,
      P: edeltraud::ThreadPool<job::Job<A>>,
{
    if let Err(error) = job(sklave_job, thread_pool) {
        log::error!("terminated with an error: {error:?}");
    }
}

fn job<A, P>(mut sklave_job: SklaveJob<A>, thread_pool: &P) -> Result<(), Error>
where A: AccessPolicy,
      P: edeltraud::ThreadPool<job::Job<A>>,
{

    todo!()
}
