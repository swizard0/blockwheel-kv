use alloc_pool::{
    bytes::{
        Bytes,
        BytesPool,
    },
};

use arbeitssklave::{
    komm,
};

use blockwheel_fs::{
    block,
};

use crate::{
    kv,
    job,
    wheels,
    version,
    core::{
        SearchRangeBounds,
    },
    Params,
    AccessPolicy,
};

pub enum Order<A> where A: AccessPolicy {
    Request(OrderRequest<A>),
    LookupRangeStreamCancel(komm::UmschlagAbbrechen<LookupRangeRoute>),
    LookupRangeStreamNext(komm::Umschlag<LookupRangeStreamNext<A>, LookupRangeRoute>),
    Wheel(OrderWheel),
}

pub enum OrderRequest<A> where A: AccessPolicy {
    Info(OrderRequestInfo<A>),
    Insert(OrderRequestInsert<A>),
    LookupRange(OrderRequestLookupRange<A>),
    Remove(OrderRequestRemove<A>),
    Flush(OrderRequestFlush<A>),
}

pub enum OrderWheel {
    InfoCancel(komm::UmschlagAbbrechen<WheelRouteInfo>),
    Info(komm::Umschlag<blockwheel_fs::Info, WheelRouteInfo>),
    FlushCancel(komm::UmschlagAbbrechen<WheelRouteFlush>),
    Flush(komm::Umschlag<blockwheel_fs::Flushed, WheelRouteFlush>),
    WriteBlockCancel(komm::UmschlagAbbrechen<WheelRouteWriteBlock>),
    WriteBlock(komm::Umschlag<Result<block::Id, blockwheel_fs::RequestWriteBlockError>, WheelRouteWriteBlock>),
    ReadBlockCancel(komm::UmschlagAbbrechen<WheelRouteReadBlock>),
    ReadBlock(komm::Umschlag<Result<Bytes, blockwheel_fs::RequestReadBlockError>, WheelRouteReadBlock>),
    DeleteBlockCancel(komm::UmschlagAbbrechen<WheelRouteDeleteBlock>),
    DeleteBlock(komm::Umschlag<Result<blockwheel_fs::Deleted, blockwheel_fs::RequestDeleteBlockError>, WheelRouteDeleteBlock>),
    IterBlocksInitCancel(komm::UmschlagAbbrechen<WheelRouteIterBlocksInit>),
    IterBlocksInit(komm::Umschlag<blockwheel_fs::IterBlocks, WheelRouteIterBlocksInit>),
    IterBlocksNextCancel(komm::UmschlagAbbrechen<WheelRouteIterBlocksNext>),
    IterBlocksNext(komm::Umschlag<blockwheel_fs::IterBlocksItem, WheelRouteIterBlocksNext>),
}

pub struct Welt<A> where A: AccessPolicy {
    pub env: Env<A>,
    pub kont: Kont,
}

pub type SklaveJob<A> = arbeitssklave::SklaveJob<Welt<A>, Order<A>>;

pub struct Env<A> where A: AccessPolicy {
    pub params: Params,
    pub blocks_pool: BytesPool,
    pub version_provider: version::Provider,
    pub wheels: wheels::Wheels<A>,
    pub incoming_orders: Vec<Order<A>>,
    pub delayed_orders: Vec<Order<A>>,
}

pub enum Kont {
    Initialize,
}

pub struct LookupRangeRoute {
    meister_ref: o1::set::Ref,
}

pub struct LookupRangeStreamNext<A> where A: AccessPolicy {
    pub rueckkopplung: komm::Rueckkopplung<A::Order, A::LookupRange>,
}

pub struct OrderRequestInfo<A> where A: AccessPolicy {
    pub rueckkopplung: komm::Rueckkopplung<A::Order, A::Info>,
}

pub struct OrderRequestInsert<A> where A: AccessPolicy {
    pub key: kv::Key,
    pub value: kv::Value,
    pub rueckkopplung: komm::Rueckkopplung<A::Order, A::Insert>,
}

pub struct OrderRequestLookupRange<A> where A: AccessPolicy {
    pub search_range: SearchRangeBounds,
    pub rueckkopplung: komm::Rueckkopplung<A::Order, A::LookupRange>,
}

pub struct OrderRequestRemove<A> where A: AccessPolicy {
    pub key: kv::Key,
    pub rueckkopplung: komm::Rueckkopplung<A::Order, A::Remove>,
}

pub struct OrderRequestFlush<A> where A: AccessPolicy {
    pub rueckkopplung: komm::Rueckkopplung<A::Order, A::Flush>,
}

pub struct WheelRouteInfo;
pub struct WheelRouteFlush;
pub struct WheelRouteWriteBlock;
pub struct WheelRouteReadBlock;
pub struct WheelRouteDeleteBlock;
pub struct WheelRouteIterBlocksInit;
pub struct WheelRouteIterBlocksNext;

pub fn run_job<A, P>(sklave_job: SklaveJob<A>, thread_pool: &P) where A: AccessPolicy, P: edeltraud::ThreadPool<job::Job<A>> {

    todo!()
}

impl<A> From<komm::UmschlagAbbrechen<LookupRangeRoute>> for Order<A> where A: AccessPolicy {
    fn from(v: komm::UmschlagAbbrechen<LookupRangeRoute>) -> Order<A> {
        Order::LookupRangeStreamCancel(v)
    }
}

impl<A> From<komm::Umschlag<LookupRangeStreamNext<A>, LookupRangeRoute>> for Order<A> where A: AccessPolicy {
    fn from(v: komm::Umschlag<LookupRangeStreamNext<A>, LookupRangeRoute>) -> Order<A> {
        Order::LookupRangeStreamNext(v)
    }
}

impl<A> From<komm::UmschlagAbbrechen<WheelRouteInfo>> for Order<A> where A: AccessPolicy {
    fn from(v: komm::UmschlagAbbrechen<WheelRouteInfo>) -> Order<A> {
        Order::Wheel(OrderWheel::InfoCancel(v))
    }
}

impl<A> From<komm::Umschlag<blockwheel_fs::Info, WheelRouteInfo>> for Order<A> where A: AccessPolicy {
    fn from(v: komm::Umschlag<blockwheel_fs::Info, WheelRouteInfo>) -> Order<A> {
        Order::Wheel(OrderWheel::Info(v))
    }
}

impl<A> From<komm::UmschlagAbbrechen<WheelRouteFlush>> for Order<A> where A: AccessPolicy {
    fn from(v: komm::UmschlagAbbrechen<WheelRouteFlush>) -> Order<A> {
        Order::Wheel(OrderWheel::FlushCancel(v))
    }
}

impl<A> From<komm::Umschlag<blockwheel_fs::Flushed, WheelRouteFlush>> for Order<A> where A: AccessPolicy {
    fn from(v: komm::Umschlag<blockwheel_fs::Flushed, WheelRouteFlush>) -> Order<A> {
        Order::Wheel(OrderWheel::Flush(v))
    }
}

impl<A> From<komm::UmschlagAbbrechen<WheelRouteWriteBlock>> for Order<A> where A: AccessPolicy {
    fn from(v: komm::UmschlagAbbrechen<WheelRouteWriteBlock>) -> Order<A> {
        Order::Wheel(OrderWheel::WriteBlockCancel(v))
    }
}

impl<A> From<komm::Umschlag<Result<block::Id, blockwheel_fs::RequestWriteBlockError>, WheelRouteWriteBlock>> for Order<A> where A: AccessPolicy {
    fn from(v: komm::Umschlag<Result<block::Id, blockwheel_fs::RequestWriteBlockError>, WheelRouteWriteBlock>) -> Order<A> {
        Order::Wheel(OrderWheel::WriteBlock(v))
    }
}

impl<A> From<komm::UmschlagAbbrechen<WheelRouteReadBlock>> for Order<A> where A: AccessPolicy {
    fn from(v: komm::UmschlagAbbrechen<WheelRouteReadBlock>) -> Order<A> {
        Order::Wheel(OrderWheel::ReadBlockCancel(v))
    }
}

impl<A> From<komm::Umschlag<Result<Bytes, blockwheel_fs::RequestReadBlockError>, WheelRouteReadBlock>> for Order<A> where A: AccessPolicy {
    fn from(v: komm::Umschlag<Result<Bytes, blockwheel_fs::RequestReadBlockError>, WheelRouteReadBlock>) -> Order<A> {
        Order::Wheel(OrderWheel::ReadBlock(v))
    }
}

impl<A> From<komm::UmschlagAbbrechen<WheelRouteDeleteBlock>> for Order<A> where A: AccessPolicy {
    fn from(v: komm::UmschlagAbbrechen<WheelRouteDeleteBlock>) -> Order<A> {
        Order::Wheel(OrderWheel::DeleteBlockCancel(v))
    }
}

impl<A> From<komm::Umschlag<Result<blockwheel_fs::Deleted, blockwheel_fs::RequestDeleteBlockError>, WheelRouteDeleteBlock>> for Order<A>
where A: AccessPolicy
{
    fn from(v: komm::Umschlag<Result<blockwheel_fs::Deleted, blockwheel_fs::RequestDeleteBlockError>, WheelRouteDeleteBlock>) -> Order<A> {
        Order::Wheel(OrderWheel::DeleteBlock(v))
    }
}

impl<A> From<komm::UmschlagAbbrechen<WheelRouteIterBlocksInit>> for Order<A> where A: AccessPolicy {
    fn from(v: komm::UmschlagAbbrechen<WheelRouteIterBlocksInit>) -> Order<A> {
        Order::Wheel(OrderWheel::IterBlocksInitCancel(v))
    }
}

impl<A> From<komm::Umschlag<blockwheel_fs::IterBlocks, WheelRouteIterBlocksInit>> for Order<A> where A: AccessPolicy {
    fn from(v: komm::Umschlag<blockwheel_fs::IterBlocks, WheelRouteIterBlocksInit>) -> Order<A> {
        Order::Wheel(OrderWheel::IterBlocksInit(v))
    }
}

impl<A> From<komm::UmschlagAbbrechen<WheelRouteIterBlocksNext>> for Order<A> where A: AccessPolicy {
    fn from(v: komm::UmschlagAbbrechen<WheelRouteIterBlocksNext>) -> Order<A> {
        Order::Wheel(OrderWheel::IterBlocksNextCancel(v))
    }
}

impl<A> From<komm::Umschlag<blockwheel_fs::IterBlocksItem, WheelRouteIterBlocksNext>> for Order<A> where A: AccessPolicy {
    fn from(v: komm::Umschlag<blockwheel_fs::IterBlocksItem, WheelRouteIterBlocksNext>) -> Order<A> {
        Order::Wheel(OrderWheel::IterBlocksNext(v))
    }
}
