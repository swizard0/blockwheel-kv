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
    version,
    core::{
        context,
        performer,
        BlockRef,
        SearchRangeBounds,
    },
    Params,
    EchoPolicy,
};

pub mod loading;
pub mod running;

pub enum Order<E> where E: EchoPolicy {
    Request(OrderRequest<E>),
    FlushButcherDone(komm::Umschlag<FlushButcherDone, FlushButcherDrop>),
    LookupRangeMergeDone(komm::Umschlag<LookupRangeMergeDone, LookupRangeMergeDrop>),
    MergeSearchTreesDone(komm::Umschlag<MergeSearchTreesDone, MergeSearchTreesDrop>),
    DemolishSearchTreeDone(komm::Umschlag<DemolishSearchTreeDone, DemolishSearchTreeDrop>),
    UnregisterFlushButcher(komm::UmschlagAbbrechen<FlushButcherDrop>),
    UnregisterLookupRangeMerge(komm::UmschlagAbbrechen<LookupRangeMergeDrop>),
    UnregisterMergeSearchTrees(komm::UmschlagAbbrechen<MergeSearchTreesDrop>),
    UnregisterDemolishSearchTree(komm::UmschlagAbbrechen<DemolishSearchTreeDrop>),
    Wheel(OrderWheel),
}

pub enum OrderRequest<E> where E: EchoPolicy {
    Info(OrderRequestInfo<E>),
    Insert(OrderRequestInsert<E>),
    LookupRange(OrderRequestLookupRange<E>),
    Remove(OrderRequestRemove<E>),
    Flush(OrderRequestFlush<E>),
}

pub enum OrderWheel {
    InfoCancel(komm::UmschlagAbbrechen<WheelRouteInfo>),
    Info(komm::Umschlag<blockwheel_fs::Info, WheelRouteInfo>),
    FlushCancel(komm::UmschlagAbbrechen<WheelRouteFlush>),
    Flush(komm::Umschlag<blockwheel_fs::Flushed, WheelRouteFlush>),
    IterBlocksInitCancel(komm::UmschlagAbbrechen<WheelRouteIterBlocksInit>),
    IterBlocksInit(komm::Umschlag<blockwheel_fs::IterBlocks, WheelRouteIterBlocksInit>),
    IterBlocksNextCancel(komm::UmschlagAbbrechen<WheelRouteIterBlocksNext>),
    IterBlocksNext(komm::Umschlag<blockwheel_fs::IterBlocksItem, WheelRouteIterBlocksNext>),
}

pub struct Welt<E> where E: EchoPolicy {
    env: Env<E>,
    state: WeltState<E>,
}

impl<E> Welt<E> where E: EchoPolicy {
    pub fn new(env: Env<E>) -> Self {
        Welt {
            env,
            state: WeltState::Init,
        }
    }
}

pub type SklaveJob<E> = arbeitssklave::SklaveJob<Welt<E>, Order<E>>;

pub struct Env<E> where E: EchoPolicy {
    params: Params,
    blocks_pool: BytesPool,
    version_provider: version::Provider,
    sendegeraet: komm::Sendegeraet<Order<E>>,
    wheels: wheels::Wheels<E>,
    incoming_orders: Vec<Order<E>>,
    delayed_orders: Vec<Order<E>>,
    pending_info_requests: Set<running::PendingInfo<E>>,
    flush_butcher_sklaven: Set<running::flush_butcher::Meister<E>>,
    lookup_ranges_merge_sklaven: Set<running::lookup_range_merge::Meister<E>>,
    merge_search_trees_sklaven: Set<running::merge_search_trees::Meister<E>>,
    demolish_search_tree_sklaven: Set<running::demolish_search_tree::Meister<E>>,
}

impl<E> Env<E> where E: EchoPolicy {
    pub fn new(
        params: Params,
        blocks_pool: BytesPool,
        version_provider: version::Provider,
        sendegeraet: komm::Sendegeraet<Order<E>>,
        wheels: wheels::Wheels<E>,
    )
        -> Self
    {
        Env {
            params,
            blocks_pool,
            version_provider,
            sendegeraet,
            wheels,
            incoming_orders: Vec::new(),
            delayed_orders: Vec::new(),
            pending_info_requests: Set::new(),
            flush_butcher_sklaven: Set::new(),
            lookup_ranges_merge_sklaven: Set::new(),
            merge_search_trees_sklaven: Set::new(),
            demolish_search_tree_sklaven: Set::new(),
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum WeltState<E> where E: EchoPolicy {
    Init,
    Loading(loading::WeltState),
    Running(running::WeltState<E>),
}

pub struct FlushButcherDrop {
    search_tree_id: u64,
    meister_ref: Ref,
}

pub struct FlushButcherDone {
    root_block: BlockRef,
}

pub struct LookupRangeMergeDrop {
    access_token: performer::AccessToken,
    meister_ref: Ref,
}

pub struct LookupRangeMergeDone;

pub struct MergeSearchTreesDrop {
    access_token: performer::AccessToken,
    meister_ref: Ref,
}

pub struct MergeSearchTreesDone {
    merged_search_tree_ref: BlockRef,
    merged_search_tree_items_count: usize,
}

pub struct DemolishSearchTreeDrop {
    demolish_group_ref: Ref,
    meister_ref: Ref,
}

pub struct DemolishSearchTreeDone;

#[derive(Debug)]
pub struct InfoRoute {
    info_ref: Ref,
}

pub struct OrderRequestInfo<E> where E: EchoPolicy {
    pub echo: E::Info,
}

pub struct OrderRequestInsert<E> where E: EchoPolicy {
    pub key: kv::Key,
    pub value: kv::Value,
    pub echo: E::Insert,
}

pub struct OrderRequestLookupRange<E> where E: EchoPolicy {
    pub search_range: SearchRangeBounds,
    pub lookup_ranges_merge_freie: arbeitssklave::Freie<running::lookup_range_merge::Welt<E>, running::lookup_range_merge::Order<E>>,
    pub lookup_ranges_merge_sendegeraet: komm::Sendegeraet<running::lookup_range_merge::Order<E>>,
}

pub struct OrderRequestRemove<E> where E: EchoPolicy {
    pub key: kv::Key,
    pub echo: E::Remove,
}

pub struct OrderRequestFlush<E> where E: EchoPolicy {
    pub echo: E::Flush,
}

#[derive(Debug)]
pub struct WheelRouteInfo {
    pub route: InfoRoute,
    pub blockwheel_filename: wheels::WheelFilename,
}

pub struct WheelRouteFlush;

pub struct WheelRouteIterBlocksInit {
    blockwheel_filename: wheels::WheelFilename,
}

pub struct WheelRouteIterBlocksNext {
    blockwheel_filename: wheels::WheelFilename,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Error {
    Loading(loading::Error),
    Running(running::Error),
    OrphanSklave(arbeitssklave::Error),
}

pub fn run_job<E, J>(sklave_job: SklaveJob<E>, thread_pool: &edeltraud::Handle<J>)
where E: EchoPolicy,
      J: From<blockwheel_fs::job::SklaveJob<wheels::WheelEchoPolicy<E>>>,
      J: From<job::FlushButcherSklaveJob<E>>,
      J: From<job::LookupRangeMergeSklaveJob<E>>,
      J: From<job::MergeSearchTreesSklaveJob<E>>,
      J: From<job::DemolishSearchTreeSklaveJob<E>>,
      J: Send + 'static,
{
    if let Err(error) = job(sklave_job, thread_pool) {
        log::error!("terminated with an error: {error:?}");
    }
}

fn job<E, J>(mut sklave_job: SklaveJob<E>, thread_pool: &edeltraud::Handle<J>) -> Result<(), Error>
where E: EchoPolicy,
      J: From<blockwheel_fs::job::SklaveJob<wheels::WheelEchoPolicy<E>>>,
      J: From<job::FlushButcherSklaveJob<E>>,
      J: From<job::LookupRangeMergeSklaveJob<E>>,
      J: From<job::MergeSearchTreesSklaveJob<E>>,
      J: From<job::DemolishSearchTreeSklaveJob<E>>,
      J: Send + 'static,
{
    loop {
        // first retrieve all orders available
        if let WeltState::Init = sklave_job.state {
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
                            arbeitssklave::SklavenBefehl::Mehr {
                                befehl,
                                mut mehr_befehle,
                            } => {
                                mehr_befehle.env.incoming_orders.push(befehl);
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
        loop {
            let state = mem::replace(&mut sklave_job.state, WeltState::Init);
            match state {
                WeltState::Init => {
                    log::debug!("WeltState::Init: loading forest");
                    sklave_job.state =
                        WeltState::Loading(loading::WeltState::new());
                },
                WeltState::Loading(loading) =>
                    match loading::job(loading, &mut sklave_job, thread_pool).map_err(Error::Loading)? {
                        loading::Outcome::Rasten { loading, } => {
                            sklave_job.state = WeltState::Loading(loading);
                            break;
                        },
                        loading::Outcome::Done { performer, } => {
                            sklave_job.state =
                                WeltState::Running(running::WeltState::new(performer));
                            let sklavenwelt = &mut *sklave_job;
                            sklavenwelt.env.incoming_orders.append(&mut sklavenwelt.env.delayed_orders);
                        },
                    },
                WeltState::Running(running) =>
                    match running::job(running, &mut sklave_job, thread_pool).map_err(Error::Running)? {
                        running::Outcome::Rasten { running, } => {
                            sklave_job.state = WeltState::Running(running);
                            break;
                        },
                    },
            }
        }
    }
}

impl<E> From<OrderRequestLookupRange<E>> for Order<E> where E: EchoPolicy {
    fn from(v: OrderRequestLookupRange<E>) -> Order<E> {
        Order::Request(OrderRequest::LookupRange(v))
    }
}

impl<E> From<komm::UmschlagAbbrechen<LookupRangeMergeDrop>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::UmschlagAbbrechen<LookupRangeMergeDrop>) -> Order<E> {
        Order::UnregisterLookupRangeMerge(v)
    }
}

impl<E> From<komm::UmschlagAbbrechen<FlushButcherDrop>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::UmschlagAbbrechen<FlushButcherDrop>) -> Order<E> {
        Order::UnregisterFlushButcher(v)
    }
}

impl<E> From<komm::UmschlagAbbrechen<MergeSearchTreesDrop>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::UmschlagAbbrechen<MergeSearchTreesDrop>) -> Order<E> {
        Order::UnregisterMergeSearchTrees(v)
    }
}

impl<E> From<komm::UmschlagAbbrechen<DemolishSearchTreeDrop>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::UmschlagAbbrechen<DemolishSearchTreeDrop>) -> Order<E> {
        Order::UnregisterDemolishSearchTree(v)
    }
}

impl<E> From<komm::UmschlagAbbrechen<WheelRouteInfo>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::UmschlagAbbrechen<WheelRouteInfo>) -> Order<E> {
        Order::Wheel(OrderWheel::InfoCancel(v))
    }
}

impl<E> From<komm::Umschlag<blockwheel_fs::Info, WheelRouteInfo>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::Umschlag<blockwheel_fs::Info, WheelRouteInfo>) -> Order<E> {
        Order::Wheel(OrderWheel::Info(v))
    }
}

impl<E> From<komm::UmschlagAbbrechen<WheelRouteFlush>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::UmschlagAbbrechen<WheelRouteFlush>) -> Order<E> {
        Order::Wheel(OrderWheel::FlushCancel(v))
    }
}

impl<E> From<komm::Umschlag<blockwheel_fs::Flushed, WheelRouteFlush>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::Umschlag<blockwheel_fs::Flushed, WheelRouteFlush>) -> Order<E> {
        Order::Wheel(OrderWheel::Flush(v))
    }
}

impl<E> From<komm::UmschlagAbbrechen<WheelRouteIterBlocksInit>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::UmschlagAbbrechen<WheelRouteIterBlocksInit>) -> Order<E> {
        Order::Wheel(OrderWheel::IterBlocksInitCancel(v))
    }
}

impl<E> From<komm::Umschlag<blockwheel_fs::IterBlocks, WheelRouteIterBlocksInit>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::Umschlag<blockwheel_fs::IterBlocks, WheelRouteIterBlocksInit>) -> Order<E> {
        Order::Wheel(OrderWheel::IterBlocksInit(v))
    }
}

impl<E> From<komm::UmschlagAbbrechen<WheelRouteIterBlocksNext>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::UmschlagAbbrechen<WheelRouteIterBlocksNext>) -> Order<E> {
        Order::Wheel(OrderWheel::IterBlocksNextCancel(v))
    }
}

impl<E> From<komm::Umschlag<blockwheel_fs::IterBlocksItem, WheelRouteIterBlocksNext>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::Umschlag<blockwheel_fs::IterBlocksItem, WheelRouteIterBlocksNext>) -> Order<E> {
        Order::Wheel(OrderWheel::IterBlocksNext(v))
    }
}

impl<E> From<komm::Umschlag<FlushButcherDone, FlushButcherDrop>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::Umschlag<FlushButcherDone, FlushButcherDrop>) -> Order<E> {
        Order::FlushButcherDone(v)
    }
}

impl<E> From<komm::Umschlag<LookupRangeMergeDone, LookupRangeMergeDrop>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::Umschlag<LookupRangeMergeDone, LookupRangeMergeDrop>) -> Order<E> {
        Order::LookupRangeMergeDone(v)
    }
}

impl<E> From<komm::Umschlag<MergeSearchTreesDone, MergeSearchTreesDrop>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::Umschlag<MergeSearchTreesDone, MergeSearchTreesDrop>) -> Order<E> {
        Order::MergeSearchTreesDone(v)
    }
}

impl<E> From<komm::Umschlag<DemolishSearchTreeDone, DemolishSearchTreeDrop>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::Umschlag<DemolishSearchTreeDone, DemolishSearchTreeDrop>) -> Order<E> {
        Order::DemolishSearchTreeDone(v)
    }
}

pub struct Context<E>(PhantomData<E>);

pub struct LookupRangeStream<E> where E: EchoPolicy {
    pub lookup_ranges_merge_freie: arbeitssklave::Freie<running::lookup_range_merge::Welt<E>, running::lookup_range_merge::Order<E>>,
    pub lookup_ranges_merge_sendegeraet: komm::Sendegeraet<running::lookup_range_merge::Order<E>>,
}

impl<E> context::Context for Context<E> where E: EchoPolicy {
    type Info = E::Info;
    type Insert = E::Insert;
    type Lookup = LookupRangeStream<E>;
    type Remove = E::Remove;
    type Flush = E::Flush;
}
