use std::{
    mem,
    marker::{
        PhantomData,
    },
};

use o1::{
    set::{
        Ref,
        Set,
    },
};

use alloc_pool::{
    pool,
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
        context,
        performer,
        search_tree_walker,
        SearchRangeBounds,
        SearchTreeBuilderBlockEntry,
    },
    Params,
    AccessPolicy,
};

pub mod loading;
pub mod running;

pub enum Order<A> where A: AccessPolicy {
    Request(OrderRequest<A>),
    LookupRangeStreamCancel(komm::UmschlagAbbrechen<LookupRangeRoute>),
    LookupRangeStreamNext(komm::Umschlag<LookupRangeStreamNext<A>, LookupRangeRoute>),
    UnregisterLookupRangeMerge(komm::UmschlagAbbrechen<LookupRangeMergeDrop>),
    UnregisterFlushButcher(komm::UmschlagAbbrechen<FlushButcherDrop>),
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
    env: Env<A>,
    state: WeltState<A>,
}

impl<A> Welt<A> where A: AccessPolicy {
    pub fn new(env: Env<A>) -> Self {
        Welt {
            env,
            state: WeltState::Init,
        }
    }
}

pub type SklaveJob<A> = arbeitssklave::SklaveJob<Welt<A>, Order<A>>;

pub struct Env<A> where A: AccessPolicy {
    params: Params,
    blocks_pool: BytesPool,
    version_provider: version::Provider,
    wheels: wheels::Wheels<A>,
    sendegeraet: komm::Sendegeraet<Order<A>>,
    incoming_orders: Vec<Order<A>>,
    delayed_orders: Vec<Order<A>>,
    lookup_range_merge_sklaven: Set<running::lookup_range_merge::Meister<A>>,
    flush_butcher_sklaven: Set<running::flush_butcher::Meister<A>>,
}

impl<A> Env<A> where A: AccessPolicy {
    pub fn new(
        params: Params,
        blocks_pool: BytesPool,
        version_provider: version::Provider,
        wheels: wheels::Wheels<A>,
        sendegeraet: komm::Sendegeraet<Order<A>>,
    )
        -> Self
    {
        Env {
            params,
            blocks_pool,
            version_provider,
            wheels,
            sendegeraet,
            incoming_orders: Vec::new(),
            delayed_orders: Vec::new(),
            lookup_range_merge_sklaven: Set::new(),
            flush_butcher_sklaven: Set::new(),
        }
    }
}

enum WeltState<A> where A: AccessPolicy {
    Init,
    Loading(loading::WeltState),
    Running(running::WeltState<A>),
}

#[derive(Debug)]
pub struct LookupRangeRoute {
    meister_ref: Ref,
}

pub struct LookupRangeMergeDrop {
    access_token: performer::AccessToken,
    route: LookupRangeRoute,
}

#[derive(Debug)]
pub struct FlushButcherRoute {
    meister_ref: Ref,
}

pub struct FlushButcherDrop {
    route: FlushButcherRoute,
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

pub enum WheelRouteReadBlock {
    LookupRangeMerge {
        route: LookupRangeRoute,
        target: running::lookup_range_merge::ReadBlockTarget,
    },
}

pub struct WheelRouteDeleteBlock;

pub struct WheelRouteIterBlocksInit {
    blockwheel_filename: wheels::WheelFilename,
}

pub struct WheelRouteIterBlocksNext {
    blockwheel_filename: wheels::WheelFilename,
}

#[derive(Debug)]
pub enum Error {
    Loading(loading::Error),
    Running(running::Error),
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
    loop {
        // first retrieve all orders available
        if let WeltState::Init = sklave_job.sklavenwelt().state {
            // skip it on initialize
        } else {
            let gehorsam = sklave_job.zu_ihren_diensten()
                .map_err(Error::OrphanSklave)?;
            match gehorsam {
                arbeitssklave::Gehorsam::Rasten =>
                    return Ok(()),
                arbeitssklave::Gehorsam::Machen { mut befehle, } =>
                    loop {
                        match befehle.befehl() {
                            arbeitssklave::SklavenBefehl::Mehr { befehl, mut mehr_befehle, } => {
                                mehr_befehle
                                    .sklavenwelt_mut()
                                    .env
                                    .incoming_orders.push(befehl);
                                befehle = mehr_befehle;
                            },
                            arbeitssklave::SklavenBefehl::Ende { sklave_job: next_sklave_job, } => {
                                sklave_job = next_sklave_job;
                                break;
                            },
                        }
                    },
            }
        }

        // then process the orders retrieved
        let sklavenwelt = sklave_job.sklavenwelt_mut();
        loop {
            let state = mem::replace(&mut sklavenwelt.state, WeltState::Init);
            match state {
                WeltState::Init => {
                    log::debug!("WeltState::Init: loading forest");
                    sklavenwelt.state =
                        WeltState::Loading(loading::WeltState::new());
                },
                WeltState::Loading(loading) =>
                    match loading::job(loading, sklavenwelt, thread_pool).map_err(Error::Loading)? {
                        loading::Outcome::Rasten { loading, } => {
                            sklavenwelt.state = WeltState::Loading(loading);
                            break;
                        },
                        loading::Outcome::Done { performer, pools, } => {
                            sklavenwelt.state =
                                WeltState::Running(running::WeltState::new(performer, pools));
                            sklavenwelt
                                .env
                                .incoming_orders
                                .extend(sklavenwelt.env.delayed_orders.drain(..));
                        },
                    },
                WeltState::Running(running) =>
                    match running::job(running, sklavenwelt, thread_pool).map_err(Error::Running)? {
                        running::Outcome::Rasten { running, } => {
                            sklavenwelt.state = WeltState::Running(running);
                            break;
                        },
                    },
            }
        }
    }
}

pub struct Pools {
    kv_pool: pool::Pool<Vec<kv::KeyValuePair<kv::Value>>>,
    block_entries_pool: pool::Pool<Vec<SearchTreeBuilderBlockEntry>>,
    sources_pool: pool::Pool<Vec<performer::LookupRangeSource>>,
    block_entry_steps_pool: pool::Pool<Vec<search_tree_walker::BlockEntryStep>>,
}

impl Pools {
    fn new() -> Self {
        Self {
            kv_pool: pool::Pool::new(),
            block_entries_pool: pool::Pool::new(),
            sources_pool: pool::Pool::new(),
            block_entry_steps_pool: pool::Pool::new(),
        }
    }
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

impl<A> From<komm::UmschlagAbbrechen<LookupRangeMergeDrop>> for Order<A> where A: AccessPolicy {
    fn from(v: komm::UmschlagAbbrechen<LookupRangeMergeDrop>) -> Order<A> {
        Order::UnregisterLookupRangeMerge(v)
    }
}

impl<A> From<komm::UmschlagAbbrechen<FlushButcherDrop>> for Order<A> where A: AccessPolicy {
    fn from(v: komm::UmschlagAbbrechen<FlushButcherDrop>) -> Order<A> {
        Order::UnregisterFlushButcher(v)
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

pub struct Context<A>(PhantomData<A>);

impl<A> context::Context for Context<A> where A: AccessPolicy {
    type Info = komm::Rueckkopplung<A::Order, A::Info>;
    type Insert = komm::Rueckkopplung<A::Order, A::Insert>;
    type Lookup = komm::Rueckkopplung<A::Order, A::LookupRange>;
    type Remove = komm::Rueckkopplung<A::Order, A::Remove>;
    type Flush = komm::Rueckkopplung<A::Order, A::Flush>;
}
