use std::{
    mem,
    marker::{
        PhantomData,
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
    pub params: Params,
    pub blocks_pool: BytesPool,
    pub version_provider: version::Provider,
    pub wheels: wheels::Wheels<A>,
    pub sendegeraet: komm::Sendegeraet<Order<A>>,
    pub incoming_orders: Vec<Order<A>>,
    pub delayed_orders: Vec<Order<A>>,
}

enum WeltState<A> where A: AccessPolicy {
    Init,
    Loading(WeltStateLoading),
    Running(WeltStateRunning<A>),
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

#[derive(Debug)]
pub enum Error {
    WheelIterBlocksInit {
        wheel_filename: wheels::WheelFilename,
        error: arbeitssklave::Error,
    },
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
        let state = mem::replace(&mut sklave_job.sklavenwelt_mut().state, WeltState::Init);
        match state {
            WeltState::Init => {
                sklave_job.sklavenwelt_mut().state =
                    WeltState::Loading(WeltStateLoading {
                        mode: WeltStateLoadingMode::NeedIterBlocksRequest,
                        forest: performer::SearchForest::new(),
                        blocks_total: 0,
                    });
            },
            WeltState::Loading(loading) =>
                match job_loading(loading, sklave_job, thread_pool)? {
                    LoadOutcome::Rasten { loading, } =>
                        return Ok(()),
                    LoadOutcome::Done { sklave_job: next_sklave_job, } => {
                        sklave_job = next_sklave_job;
                    },
                },
            WeltState::Running(running) =>
                match job_running(running, sklave_job, thread_pool)? {
                    RunOutcome::Rasten { running, } =>
                        return Ok(()),
                },
        }
    }
}

enum LoadOutcome<A> where A: AccessPolicy {
    Rasten {
        loading: WeltStateLoading,
    },
    Done {
        sklave_job: SklaveJob<A>,
    },
}

struct WeltStateLoading {
    mode: WeltStateLoadingMode,
    forest: performer::SearchForest,
    blocks_total: usize,
}

enum WeltStateLoadingMode {
    NeedIterBlocksRequest,
    Loading { wheels_left: usize, },
}

fn job_loading<A, P>(
    mut welt_state_loading: WeltStateLoading,
    mut sklave_job: SklaveJob<A>,
    thread_pool: &P,
)
    -> Result<LoadOutcome<A>, Error>
where A: AccessPolicy,
      P: edeltraud::ThreadPool<job::Job<A>>,
{
    loop {
        match mem::replace(&mut welt_state_loading.mode, WeltStateLoadingMode::NeedIterBlocksRequest) {

            WeltStateLoadingMode::NeedIterBlocksRequest => {
                let env = &sklave_job.sklavenwelt().env;
                let wheels_iter = env.wheels.iter();
                let mut wheels_left = 0;
                for wheel_ref in wheels_iter {
                    wheel_ref.meister
                        .iter_blocks_init(
                            env.sendegeraet.rueckkopplung(WheelRouteIterBlocksInit),
                            &edeltraud::ThreadPoolMap::new(thread_pool),
                        )
                        .map_err(|error| Error::WheelIterBlocksInit {
                            wheel_filename: wheel_ref.blockwheel_filename.clone(),
                            error,
                        })?;
                    wheels_left += 1;
                }
                welt_state_loading.mode =
                    WeltStateLoadingMode::Loading { wheels_left, };
            },

            WeltStateLoadingMode::Loading { wheels_left, } if wheels_left == 0 => {
                log::info!(
                    "loading done, {} search_trees restored within {} blocks",
                    welt_state_loading.forest.len(),
                    welt_state_loading.blocks_total,
                );

                let env = &sklave_job.sklavenwelt().env;
                let pools = Pools::new();
                let performer = performer::Performer::new(
                    env.params.clone(),
                    env.version_provider.clone(),
                    pools.kv_pool.clone(),
                    pools.sources_pool.clone(),
                    pools.block_entry_steps_pool.clone(),
                    welt_state_loading.forest,
                );

                sklave_job.sklavenwelt_mut().state =
                    WeltState::Running(WeltStateRunning {
                        kont: Kont::Initialize { performer, },
                    });
                return Ok(LoadOutcome::Done { sklave_job, });
            },

            WeltStateLoadingMode::Loading { mut wheels_left, } => {

                // sklave_job.sklavenwelt_mut().state = WeltState::Loading(loading);

                todo!();
            },

        }
    }
}

struct Pools {
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

enum RunOutcome<A> where A: AccessPolicy {
    Rasten { running: WeltStateRunning<A>, },
}

struct WeltStateRunning<A> where A: AccessPolicy {
    kont: Kont<A>,
}

enum Kont<A> where A: AccessPolicy {
    Initialize { performer: performer::Performer<Context<A>>, },
}

fn job_running<A, P>(
    mut welt_state_running: WeltStateRunning<A>,
    mut sklave_job: SklaveJob<A>,
    thread_pool: &P,
)
    -> Result<RunOutcome<A>, Error>
where A: AccessPolicy,
      P: edeltraud::ThreadPool<job::Job<A>>,
{

    // sklave_job.sklavenwelt_mut().state = WeltState::Running(running);

    todo!();
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

struct Context<A>(PhantomData<A>);

impl<A> context::Context for Context<A> where A: AccessPolicy {
    type Info = A::Info;
    type Insert = A::Insert;
    type Lookup = A::LookupRange;
    type Remove = A::Remove;
    type Flush = A::Flush;
}
