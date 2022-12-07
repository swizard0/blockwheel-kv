use std::{
    mem,
    marker::{
        PhantomData,
    },
    collections::{
        HashMap,
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
    wheels,
    version,
    core::{
        context,
        performer,
        search_tree_walker,
        BlockRef,
        SearchRangeBounds,
        SearchTreeBuilderBlockEntry,
    },
    Params,
    EchoPolicy,
};

pub mod loading;
pub mod running;

pub enum Order<E> where E: EchoPolicy {
    Request(OrderRequest<E>),
    LookupRangeNext(komm::StreamMehr<OrderRequestLookupRangeNext<E>>),
    LookupRangeCancel(komm::StreamAbbrechen),
    FlushButcherDone(komm::Umschlag<FlushButcherDone, FlushButcherDrop>),
    MergeSearchTreesDone(komm::Umschlag<MergeSearchTreesDone, MergeSearchTreesDrop>),
    DemolishSearchTreeDone(komm::Umschlag<DemolishSearchTreeDone, DemolishSearchTreeDrop>),
    UnregisterLookupRangeMerge(komm::UmschlagAbbrechen<LookupRangeMergeDrop>),
    UnregisterFlushButcher(komm::UmschlagAbbrechen<FlushButcherDrop>),
    UnregisterMergeSearchTrees(komm::UmschlagAbbrechen<MergeSearchTreesDrop>),
    UnregisterDemolishSearchTree(komm::UmschlagAbbrechen<DemolishSearchTreeDrop>),
    Wheel(OrderWheel),
}

pub enum OrderRequest<E> where E: EchoPolicy {
    Info(OrderRequestInfo<E>),
    Insert(OrderRequestInsert<E>),
    LookupRange(komm::StreamStarten<OrderRequestLookupRange<E>>),
    Remove(OrderRequestRemove<E>),
    Flush(OrderRequestFlush<E>),
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

pub struct Welt<E> where E: EchoPolicy {
    env: Env<E>,
    state: WeltState<E>,
    created_at: Instant,
    idle_started_at: Option<Instant>,
}

impl<E> Welt<E> where E: EchoPolicy {
    pub fn new(env: Env<E>) -> Self {
        Welt {
            env,
            state: WeltState::Init,
            created_at: Instant::now(),
            idle_started_at: None,
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
    lookup_range_merge_sklaven: HashMap<komm::StreamId, running::lookup_range_merge::Meister<E>>,
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
            lookup_range_merge_sklaven: HashMap::new(),
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

#[derive(Debug)]
pub struct LookupRangeRoute {
    stream_id: komm::StreamId,
}

pub struct LookupRangeMergeDrop {
    access_token: performer::AccessToken,
    route: LookupRangeRoute,
}

pub struct FlushButcherDrop {
    search_tree_id: u64,
}

pub struct FlushButcherDone {
    root_block: BlockRef,
}

#[derive(Debug)]
pub struct MergeSearchTreesRoute {
    meister_ref: Ref,
}

pub struct MergeSearchTreesDrop {
    access_token: performer::AccessToken,
    route: MergeSearchTreesRoute,
}

pub struct MergeSearchTreesDone {
    merged_search_tree_ref: BlockRef,
    merged_search_tree_items_count: usize,
}

#[derive(Debug)]
pub struct DemolishSearchTreeRoute {
    meister_ref: Ref,
}

pub struct DemolishSearchTreeDrop {
    demolish_group_ref: Ref,
    route: DemolishSearchTreeRoute,
}

#[derive(Debug)]
pub struct InfoRoute {
    info_ref: Ref,
}

pub struct DemolishSearchTreeDone;

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
    pub stream_echo: E::LookupRange,
}

pub struct OrderRequestLookupRangeNext<E> where E: EchoPolicy {
    pub stream_echo: E::LookupRange,
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

#[derive(Debug)]
pub enum WheelRouteWriteBlock {
    MergeSearchTrees {
        route: MergeSearchTreesRoute,
        target: running::merge_search_trees::WriteBlockTarget,
    },
}

#[derive(Debug)]
pub enum WheelRouteReadBlock {
    LookupRangeMerge {
        route: LookupRangeRoute,
        target: running::lookup_range_merge::ReadBlockTarget,
    },
    MergeSearchTrees {
        route: MergeSearchTreesRoute,
        target: running::merge_search_trees::ReadBlockTarget,
    },
    DemolishSearchTree {
        route: DemolishSearchTreeRoute,
        target: running::demolish_search_tree::ReadBlockTarget,
    },
}

#[derive(Debug)]
pub enum WheelRouteDeleteBlock {
    DemolishSearchTree {
        route: DemolishSearchTreeRoute,
    },
}

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

use std::{sync::atomic::{AtomicUsize, Ordering}, time::{Instant, Duration}};
pub static PERFORMER_INVOKED_TOTAL: AtomicUsize = AtomicUsize::new(0);
pub static PERFORMER_GOT_RASTEN_TOTAL: AtomicUsize = AtomicUsize::new(0);
pub static PERFORMER_GOT_MACHEN_TOTAL: AtomicUsize = AtomicUsize::new(0);
pub static PERFORMER_GOT_MACHEN_BEFEHL_TOTAL: AtomicUsize = AtomicUsize::new(0);
pub static PERFORMER_RUN_MS: AtomicUsize = AtomicUsize::new(0);
pub static PERFORMER_RUN_MAX_MS: AtomicUsize = AtomicUsize::new(0);
pub static PERFORMER_IDLE_MS: AtomicUsize = AtomicUsize::new(0);

impl<E> Drop for Welt<E> where E: EchoPolicy {
    fn drop(&mut self) {

        println!(" ;; PERFORMER_INVOKED_TOTAL: {:?}", PERFORMER_INVOKED_TOTAL.load(Ordering::Relaxed));
        println!(" ;; PERFORMER_GOT_RASTEN_TOTAL: {:?}", PERFORMER_GOT_RASTEN_TOTAL.load(Ordering::Relaxed));
        println!(" ;; PERFORMER_GOT_MACHEN_TOTAL: {:?}", PERFORMER_GOT_MACHEN_TOTAL.load(Ordering::Relaxed));
        println!(" ;; PERFORMER_GOT_MACHEN_BEFEHL_TOTAL: {:?}", PERFORMER_GOT_MACHEN_BEFEHL_TOTAL.load(Ordering::Relaxed));
        println!(" ;; PERFORMER_RUN_MS: {:?}", Duration::from_micros(PERFORMER_RUN_MS.load(Ordering::Relaxed) as u64));
        println!(" ;; PERFORMER_RUN_MAX_MS: {:?}", Duration::from_micros(PERFORMER_RUN_MAX_MS.load(Ordering::Relaxed) as u64));
        println!(" ;; PERFORMER_IDLE_MS: {:?}", Duration::from_micros(PERFORMER_IDLE_MS.load(Ordering::Relaxed) as u64));
        println!(" ;; PERFORMER_WELT_ALIVE_MS: {:?}", self.created_at.elapsed());

    }
}

pub fn run_job<E, J>(sklave_job: SklaveJob<E>, thread_pool: &edeltraud::Handle<J>)
where E: EchoPolicy,
      J: From<blockwheel_fs::job::SklaveJob<wheels::WheelEchoPolicy<E>>>,
      J: From<running::flush_butcher::SklaveJob<E>>,
      J: From<running::lookup_range_merge::SklaveJob<E>>,
      J: From<running::merge_search_trees::SklaveJob<E>>,
      J: From<running::demolish_search_tree::SklaveJob<E>>,
      J: Send + 'static,
{
    let now = Instant::now();
    if let Some(started_at) = sklave_job.idle_started_at {
        PERFORMER_IDLE_MS.fetch_add(started_at.elapsed().as_micros() as usize, Ordering::Relaxed);
    }

    if let Err(error) = job(sklave_job, thread_pool) {
        log::error!("terminated with an error: {error:?}");
    }

    let elapsed_ms = now.elapsed().as_micros() as usize;
    PERFORMER_RUN_MS.fetch_add(elapsed_ms, Ordering::Relaxed);
    let current_max = PERFORMER_RUN_MAX_MS.load(Ordering::Relaxed);
    if current_max < elapsed_ms {
        PERFORMER_RUN_MAX_MS.store(elapsed_ms, Ordering::Relaxed);
    }
}

fn job<E, J>(mut sklave_job: SklaveJob<E>, thread_pool: &edeltraud::Handle<J>) -> Result<(), Error>
where E: EchoPolicy,
      J: From<blockwheel_fs::job::SklaveJob<wheels::WheelEchoPolicy<E>>>,
      J: From<running::flush_butcher::SklaveJob<E>>,
      J: From<running::lookup_range_merge::SklaveJob<E>>,
      J: From<running::merge_search_trees::SklaveJob<E>>,
      J: From<running::demolish_search_tree::SklaveJob<E>>,
      J: Send + 'static,
{
    PERFORMER_INVOKED_TOTAL.fetch_add(1, Ordering::Relaxed);

    loop {
        // first retrieve all orders available
        if let WeltState::Init = sklave_job.state {
            // skip it on initialize
        } else {
            sklave_job.idle_started_at = Some(Instant::now());
            let gehorsam = sklave_job.zu_ihren_diensten()
                .map_err(Error::OrphanSklave)?;
            match gehorsam {
                arbeitssklave::Gehorsam::Rasten => {
                    PERFORMER_GOT_RASTEN_TOTAL.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                },
                arbeitssklave::Gehorsam::Machen { mut befehle, } => {
                    PERFORMER_GOT_MACHEN_TOTAL.fetch_add(1, Ordering::Relaxed);
                    loop {
                        match befehle.befehl() {
                            arbeitssklave::SklavenBefehl::Mehr {
                                befehl,
                                mut mehr_befehle,
                            } => {
                                PERFORMER_GOT_MACHEN_BEFEHL_TOTAL.fetch_add(1, Ordering::Relaxed);
                                mehr_befehle
                                    .env
                                    .incoming_orders.push(befehl);
                                befehle = mehr_befehle;
                            },
                            arbeitssklave::SklavenBefehl::Ende { sklave_job: next_sklave_job, } => {
                                sklave_job = next_sklave_job;
                                break;
                            },
                        }
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
                        loading::Outcome::Done { performer, pools, } => {
                            sklave_job.state =
                                WeltState::Running(running::WeltState::new(performer, pools));
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

impl<E> From<komm::StreamStarten<OrderRequestLookupRange<E>>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::StreamStarten<OrderRequestLookupRange<E>>) -> Order<E> {
        Order::Request(OrderRequest::LookupRange(v))
    }
}

impl<E> From<komm::StreamMehr<OrderRequestLookupRangeNext<E>>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::StreamMehr<OrderRequestLookupRangeNext<E>>) -> Order<E> {
        Order::LookupRangeNext(v)
    }
}

impl<E> From<komm::StreamAbbrechen> for Order<E> where E: EchoPolicy {
    fn from(v: komm::StreamAbbrechen) -> Order<E> {
        Order::LookupRangeCancel(v)
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

impl<E> From<komm::UmschlagAbbrechen<WheelRouteWriteBlock>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::UmschlagAbbrechen<WheelRouteWriteBlock>) -> Order<E> {
        Order::Wheel(OrderWheel::WriteBlockCancel(v))
    }
}

impl<E> From<komm::Umschlag<Result<block::Id, blockwheel_fs::RequestWriteBlockError>, WheelRouteWriteBlock>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::Umschlag<Result<block::Id, blockwheel_fs::RequestWriteBlockError>, WheelRouteWriteBlock>) -> Order<E> {
        Order::Wheel(OrderWheel::WriteBlock(v))
    }
}

impl<E> From<komm::UmschlagAbbrechen<WheelRouteReadBlock>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::UmschlagAbbrechen<WheelRouteReadBlock>) -> Order<E> {
        Order::Wheel(OrderWheel::ReadBlockCancel(v))
    }
}

impl<E> From<komm::Umschlag<Result<Bytes, blockwheel_fs::RequestReadBlockError>, WheelRouteReadBlock>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::Umschlag<Result<Bytes, blockwheel_fs::RequestReadBlockError>, WheelRouteReadBlock>) -> Order<E> {
        Order::Wheel(OrderWheel::ReadBlock(v))
    }
}

impl<E> From<komm::UmschlagAbbrechen<WheelRouteDeleteBlock>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::UmschlagAbbrechen<WheelRouteDeleteBlock>) -> Order<E> {
        Order::Wheel(OrderWheel::DeleteBlockCancel(v))
    }
}

impl<E> From<komm::Umschlag<Result<blockwheel_fs::Deleted, blockwheel_fs::RequestDeleteBlockError>, WheelRouteDeleteBlock>> for Order<E>
where E: EchoPolicy
{
    fn from(v: komm::Umschlag<Result<blockwheel_fs::Deleted, blockwheel_fs::RequestDeleteBlockError>, WheelRouteDeleteBlock>) -> Order<E> {
        Order::Wheel(OrderWheel::DeleteBlock(v))
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

pub struct LookupRangeStream<E> {
    pub stream_echo: E,
    pub stream_token: komm::StreamToken,
}

impl<E> context::Context for Context<E> where E: EchoPolicy {
    type Info = E::Info;
    type Insert = E::Insert;
    type Lookup = LookupRangeStream<E::LookupRange>;
    type Remove = E::Remove;
    type Flush = E::Flush;
}
