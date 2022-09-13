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
        search_ranges_merge,
        BlockRef,
        SearchRangesMergeCps,
        SearchRangesMergeBlockNext,
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
        merger: SearchRangesMergeCps,
        meister_ref: Ref,
        sendegeraet: komm::Sendegeraet<performer_sklave::Order<A>>,
        wheels: wheels::Wheels<A>,
        feedback: komm::Rueckkopplung<performer_sklave::Order<A>, performer_sklave::DemolishSearchTreeDrop>,
    )
        -> Self
    {
        Welt {
            kont: Some(Kont::Start { merger, }),
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
    Start {
        merger: SearchRangesMergeCps,
    },
    AwaitBlocks {
        next: SearchRangesMergeBlockNext,
    },
    Finished,
}

#[derive(Debug)]
pub enum Error {
    OrphanSklave(arbeitssklave::Error),
    SearchRangesMerge(search_ranges_merge::Error),
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
    'outer: loop {
        // first retrieve all orders available
        if let Some(Kont::Start { .. }) = sklave_job.sklavenwelt().kont {
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
                            arbeitssklave::SklavenBefehl::Mehr { befehl, mehr_befehle, } => {
                                befehle = mehr_befehle;
                                let sklavenwelt = befehle.sklavenwelt_mut();

                                match befehl {
                                    Order::ReadBlock(OrderReadBlock {
                                        read_block_result: Ok(block_bytes),
                                        target: ReadBlockTarget::LoadBlock(ReadBlockTargetLoadBlock {
                                            async_token: HideDebug(async_token),
                                        }),
                                    }) =>
                                        sklavenwelt.received_block_tasks
                                            .push(ReceivedBlockTask { async_token, block_bytes, }),
                                    Order::ReadBlock(OrderReadBlock { read_block_result: Err(error), .. }) =>
                                        return Err(Error::ReadBlock(error)),
                                    Order::DeleteBlock(OrderDeleteBlock { delete_block_result: Ok(blockwheel_fs::Deleted), }) => {
                                        assert!(sklavenwelt.pending_delete_tasks > 0);
                                        sklavenwelt.pending_delete_tasks -= 1;
                                    },
                                    Order::DeleteBlock(OrderDeleteBlock { delete_block_result: Err(error), }) =>
                                        return Err(Error::DeleteBlock(error)),
                                }
                            },
                            arbeitssklave::SklavenBefehl::Ende { sklave_job: next_sklave_job, } => {
                                sklave_job = next_sklave_job;
                                break;
                            },
                        }
                    },
            }
        }

        let sklavenwelt = sklave_job.sklavenwelt_mut();

        loop {
            let kont = sklavenwelt.kont.take().unwrap();
            let mut merger_kont = match kont {
                Kont::Start { merger, } => {
                    merger.step()
                        .map_err(Error::SearchRangesMerge)?
                },
                Kont::AwaitBlocks { next, } =>
                    if let Some(ReceivedBlockTask { async_token, block_bytes, }) = sklavenwelt.received_block_tasks.pop() {
                        next.block_arrived(async_token, block_bytes)
                            .map_err(Error::SearchRangesMerge)?
                    } else {
                        sklavenwelt.kont = Some(Kont::AwaitBlocks { next, });
                        continue 'outer;
                    },
                Kont::Finished if sklavenwelt.pending_delete_tasks == 0 => {
                    if let Some(feedback) = sklavenwelt.maybe_feedback.take() {
                        feedback
                            .commit(performer_sklave::DemolishSearchTreeDone)
                            .map_err(Error::FeedbackCommit)?;
                    }
                    return Ok(());
                },
                Kont::Finished => {
                    sklavenwelt.kont = Some(Kont::Finished);
                    continue 'outer;
                },
            };

            loop {
                match merger_kont {
                    search_ranges_merge::Kont::RequireBlockAsync(
                        search_ranges_merge::KontRequireBlockAsync { block_ref, async_token, next, },
                    ) => {
                        let wheel_ref = sklavenwelt.wheels.get(&block_ref.blockwheel_filename)
                            .ok_or_else(|| Error::WheelNotFound {
                                blockwheel_filename: block_ref.blockwheel_filename.clone(),
                            })?;
                        let rueckkopplung = sklavenwelt
                            .sendegeraet
                            .rueckkopplung(
                                performer_sklave::WheelRouteReadBlock::DemolishSearchTree {
                                    route: performer_sklave::DemolishSearchTreeRoute {
                                        meister_ref: sklavenwelt.meister_ref,
                                    },
                                    target: ReadBlockTarget::LoadBlock(ReadBlockTargetLoadBlock {
                                        async_token: HideDebug(async_token),
                                    }),
                                },
                            );
                        wheel_ref.meister
                            .read_block(
                                block_ref.block_id,
                                rueckkopplung,
                                &edeltraud::ThreadPoolMap::new(thread_pool),
                            )
                            .map_err(Error::BlockLoadReadBlockRequest)?;
                        merger_kont = next.scheduled()
                            .map_err(Error::SearchRangesMerge)?;
                    },
                    search_ranges_merge::Kont::AwaitBlocks(search_ranges_merge::KontAwaitBlocks { next, }) => {
                        sklavenwelt.kont = Some(Kont::AwaitBlocks { next, });
                        break;
                    },
                    search_ranges_merge::Kont::BlockFinished(search_ranges_merge::KontBlockFinished { block_ref, next, }) => {
                        schedule_delete_block(sklavenwelt, block_ref, thread_pool)?;
                        merger_kont = next.proceed()
                            .map_err(Error::SearchRangesMerge)?;
                    },
                    search_ranges_merge::Kont::EmitDeprecated(search_ranges_merge::KontEmitDeprecated { item, next, }) => {
                        match item {
                            kv::KeyValuePair {
                                value_cell: kv::ValueCell {
                                    cell: kv::Cell::Value(storage::OwnedValueBlockRef::Ref(block_ref)),
                                    ..
                                },
                                ..
                            } =>
                                schedule_delete_block(sklavenwelt, block_ref, thread_pool)?,
                            _ =>
                                (),
                        }
                        merger_kont = next.proceed()
                            .map_err(Error::SearchRangesMerge)?;
                    },
                    search_ranges_merge::Kont::EmitItem(search_ranges_merge::KontEmitItem { next, .. }) => {
                        merger_kont = next.proceed()
                            .map_err(Error::SearchRangesMerge)?;
                    },
                    search_ranges_merge::Kont::Finished => {
                        sklavenwelt.kont = Some(Kont::Finished);
                        break;
                    },
                }
            }
        }
    }
}

struct ReceivedBlockTask {
    block_bytes: Bytes,
    async_token: search_ranges_merge::AsyncToken<performer::LookupRangeSource>,
}

fn schedule_delete_block<A, P>(
    sklavenwelt: &mut Welt<A>,
    block_ref: BlockRef,
    thread_pool: &P,
)
    -> Result<(), Error>
where A: AccessPolicy,
      P: edeltraud::ThreadPool<job::Job<A>>,
{
    let wheel_ref = sklavenwelt.wheels.get(&block_ref.blockwheel_filename)
        .ok_or_else(|| Error::WheelNotFound {
            blockwheel_filename: block_ref.blockwheel_filename.clone(),
        })?;
    let rueckkopplung = sklavenwelt
        .sendegeraet
        .rueckkopplung(
            performer_sklave::WheelRouteDeleteBlock::DemolishSearchTree {
                route: performer_sklave::DemolishSearchTreeRoute {
                    meister_ref: sklavenwelt.meister_ref,
                },
            },
        );
    wheel_ref.meister
        .delete_block(
            block_ref.block_id,
            rueckkopplung,
            &edeltraud::ThreadPoolMap::new(thread_pool),
        )
        .map_err(Error::DeleteBlockRequest)?;
    sklavenwelt.pending_delete_tasks += 1;

    Ok(())
}
