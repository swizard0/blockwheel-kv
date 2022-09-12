use alloc_pool::{
    bytes::{
        Bytes,
    },
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
        search_tree_walker,
    },
    HideDebug,
    AccessPolicy,
};

pub enum Order {
    ReadBlock(OrderReadBlock),
    DeleteBlock(OrderDeleteBlock),
}

pub struct OrderReadBlock {
    pub read_block_result: Result<Bytes, blockwheel_fs::RequestReadBlockError>,
}

pub struct OrderDeleteBlock {
    pub delete_block_result: Result<blockwheel_fs::Deleted, blockwheel_fs::RequestDeleteBlockError>,
}

pub struct Welt<A> where A: AccessPolicy {
    kont: Option<Kont>,
    meister_ref: Ref,
    sendegeraet: komm::Sendegeraet<performer_sklave::Order<A>>,
    wheels: wheels::Wheels<A>,
    maybe_feedback: Option<komm::Rueckkopplung<performer_sklave::Order<A>, performer_sklave::DemolishSearchTreeDrop>>,
    received_block_tasks: Vec<ReceivedBlockTask>,
    pending_delete_tasks: usize,
}

impl<A> Welt<A> where A: AccessPolicy {
    pub fn new(
        walker: search_tree_walker::WalkerCps,
        meister_ref: Ref,
        sendegeraet: komm::Sendegeraet<performer_sklave::Order<A>>,
        wheels: wheels::Wheels<A>,
        feedback: komm::Rueckkopplung<performer_sklave::Order<A>, performer_sklave::DemolishSearchTreeDrop>,
    )
        -> Self
    {
        Welt {
            kont: Some(Kont::Start { walker, }),
            meister_ref,
            sendegeraet,
            wheels,
            maybe_feedback: Some(feedback),
            received_block_tasks: Vec::new(),
            pending_delete_tasks: 0,
        }
    }
}

pub type Meister<A> = arbeitssklave::Meister<Welt<A>, Order>;
pub type SklaveJob<A> = arbeitssklave::SklaveJob<Welt<A>, Order>;

enum Kont {
    Start { walker: search_tree_walker::WalkerCps, },
}

#[derive(Debug)]
pub enum Error {
    OrphanSklave(arbeitssklave::Error),
    SearchTreeWalker(search_tree_walker::Error),
    ReadBlock(blockwheel_fs::RequestReadBlockError),
    DeleteBlock(blockwheel_fs::RequestDeleteBlockError),
    FeedbackCommit(komm::Error),
    WheelNotFound { blockwheel_filename: wheels::WheelFilename, },
    BlockLoadReadBlockRequest(arbeitssklave::Error),
    DeleteBlockRequest(arbeitssklave::Error),
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

struct ReceivedBlockTask {
    block_bytes: Bytes,
}
