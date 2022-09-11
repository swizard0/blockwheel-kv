use alloc_pool::{
    pool,
    bytes::{
        Bytes,
    },
    Unique,
};

use o1::{
    set::{
        Ref,
    },
};

use arbeitssklave::{
    komm,
};

use crate::{
    kv,
    job,
    wheels,
    storage,
    core::{
        performer,
        performer_sklave,
        search_ranges_merge,
        SearchTreeBuilderBlockEntry,
    },
    HideDebug,
    AccessPolicy,
};

pub enum Order {
    ReadBlock(OrderReadBlock),
}

pub struct OrderReadBlock {
    pub read_block_result: Result<Bytes, blockwheel_fs::RequestReadBlockError>,
    pub target: ReadBlockTarget,
}

#[derive(Debug)]
pub enum ReadBlockTarget {
    LoadBlock(ReadBlockTargetLoadBlock),
}

#[derive(Debug)]
pub struct ReadBlockTargetLoadBlock {
    async_token: HideDebug<search_ranges_merge::AsyncToken<performer::LookupRangeSource>>,
}

pub struct Welt<A> where A: AccessPolicy {
    kont: Option<Kont<A>>,
    meister_ref: Ref,
    sendegeraet: komm::Sendegeraet<performer_sklave::Order<A>>,
    wheels: wheels::Wheels<A>,
    block_entries_pool: pool::Pool<Vec<SearchTreeBuilderBlockEntry>>,
    tree_block_size: usize,
    feedback: komm::Rueckkopplung<performer_sklave::Order<A>, performer_sklave::MergeSearchTreesDrop>,
    incoming_orders: Vec<Order>,
    delayed_orders: Vec<Order>,
}

impl<A> Welt<A> where A: AccessPolicy {
    pub fn new(
        source_count_items: SearchRangesMergeCps,
        source_build: SearchRangesMergeCps,
        meister_ref: Ref,
        sendegeraet: komm::Sendegeraet<performer_sklave::Order<A>>,
        wheels: wheels::Wheels<A>,
        block_entries_pool: pool::Pool<Vec<SearchTreeBuilderBlockEntry>>,
        tree_block_size: usize,
        feedback: komm::Rueckkopplung<performer_sklave::Order<A>, performer_sklave::MergeSearchTreesDrop>,
    )
        -> Self
    {
        Welt {
            kont: Some(Kont::Start { source_count_items, source_build, }),
            meister_ref,
            sendegeraet,
            wheels,
            block_entries_pool,
            tree_block_size,
            feedback,
            incoming_orders: Vec::new(),
            delayed_orders: Vec::new(),
        }
    }
}

pub type Meister<A> = arbeitssklave::Meister<Welt<A>, Order>;
pub type SklaveJob<A> = arbeitssklave::SklaveJob<Welt<A>, Order>;

type SearchRangesMergeCps =
    search_ranges_merge::RangesMergeCps<Unique<Vec<performer::LookupRangeSource>>, performer::LookupRangeSource>;
// type SearchRangesMergeBlockNext =
//     search_ranges_merge::KontAwaitBlocksNext<Unique<Vec<performer::LookupRangeSource>>, performer::LookupRangeSource>;
// type SearchRangesMergeItemNext =
//     search_ranges_merge::KontEmitItemNext<Unique<Vec<performer::LookupRangeSource>>, performer::LookupRangeSource>;

enum Kont<A> where A: AccessPolicy {
    Start {
        source_count_items: SearchRangesMergeCps,
        source_build: SearchRangesMergeCps,
    },
    Dummy(A),
}

#[derive(Debug)]
enum Error {
    OrphanSklave(arbeitssklave::Error),
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
