use alloc_pool::{
    bytes::{
        Bytes,
    },
};

use arbeitssklave::{
    komm,
};

use crate::{
    kv,
    wheels,
    storage,
    core::{
        performer,
        performer_sklave,
        search_ranges_merge,
        BlockRef,
        FsReadBlock,
        FsDeleteBlock,
        SearchRangesMergeCps,
        SearchRangesMergeBlockNext,
    },
    HideDebug,
    EchoPolicy,
};

#[allow(clippy::large_enum_variant)]
pub enum Order {
    ReadBlock(OrderReadBlock),
    ReadBlockCancel(komm::UmschlagAbbrechen<ReadBlockTarget>),
    DeleteBlock(OrderDeleteBlock),
    DeleteBlockCancel(komm::UmschlagAbbrechen<DeleteBlockTarget>),
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
    pub target: DeleteBlockTarget,
}

pub struct DeleteBlockTarget;

pub struct Welt<E> where E: EchoPolicy {
    kont: Option<Kont>,
    sendegeraet: komm::Sendegeraet<Order>,
    wheels: wheels::Wheels<E>,
    maybe_feedback: Option<komm::Rueckkopplung<performer_sklave::Order<E>, performer_sklave::DemolishSearchTreeDrop>>,
    received_block_tasks: Vec<ReceivedBlockTask>,
    pending_delete_tasks: usize,
    total_search_tree_blocks_removed: usize,
    total_external_value_blocks_removed: usize,
}

impl<E> Welt<E> where E: EchoPolicy {
    pub fn new(
        merger: SearchRangesMergeCps,
        sendegeraet: komm::Sendegeraet<Order>,
        wheels: wheels::Wheels<E>,
        feedback: komm::Rueckkopplung<performer_sklave::Order<E>, performer_sklave::DemolishSearchTreeDrop>,
    )
        -> Self
    {
        Welt {
            kont: Some(Kont::Start { merger, }),
            sendegeraet,
            wheels,
            maybe_feedback: Some(feedback),
            received_block_tasks: Vec::new(),
            pending_delete_tasks: 0,
            total_search_tree_blocks_removed: 0,
            total_external_value_blocks_removed: 0,
        }
    }
}

pub type Meister<E> = arbeitssklave::Meister<Welt<E>, Order>;
pub type SklaveJob<E> = arbeitssklave::SklaveJob<Welt<E>, Order>;

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
    FeedbackCommit(arbeitssklave::Error),
    WheelNotFound { blockwheel_filename: wheels::WheelFilename, },
    BlockLoadReadBlockRequest(blockwheel_fs::Error),
    DeleteBlockRequest(blockwheel_fs::Error),
    WheelIsGoneDuringReadBlock,
    WheelIsGoneDuringDeleteBlock,
}

pub fn run_job<E, J>(sklave_job: SklaveJob<E>, thread_pool: &edeltraud::Handle<J>)
where E: EchoPolicy,
      J: From<blockwheel_fs::job::SklaveJob<wheels::WheelEchoPolicy<E>>>,
{
    if let Err(error) = job(sklave_job, thread_pool) {
        log::error!("terminated with an error: {error:?}");
    }
}

fn job<E, J>(mut sklave_job: SklaveJob<E>, thread_pool: &edeltraud::Handle<J>) -> Result<(), Error>
where E: EchoPolicy,
      J: From<blockwheel_fs::job::SklaveJob<wheels::WheelEchoPolicy<E>>>,
{
    'outer: loop {
        // first retrieve all orders available
        if let Some(Kont::Start { .. }) = sklave_job.kont {
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
                                let sklavenwelt = &mut *befehle;

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
                                    Order::ReadBlockCancel(..) =>
                                        return Err(Error::WheelIsGoneDuringReadBlock),
                                    Order::DeleteBlock(OrderDeleteBlock {
                                        delete_block_result: Ok(blockwheel_fs::Deleted),
                                        target: DeleteBlockTarget,
                                    }) => {
                                        assert!(sklavenwelt.pending_delete_tasks > 0);
                                        sklavenwelt.pending_delete_tasks -= 1;
                                    },
                                    Order::DeleteBlock(OrderDeleteBlock { delete_block_result: Err(error), .. }) =>
                                        return Err(Error::DeleteBlock(error)),
                                    Order::DeleteBlockCancel(..) =>
                                        return Err(Error::WheelIsGoneDuringDeleteBlock),
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

        let sklavenwelt = &mut *sklave_job;

        loop {
            let kont = sklavenwelt.kont.take().unwrap();
            let mut merger_kont = match kont {
                Kont::Start { merger, } => {
                    merger.step()
                        .map_err(Error::SearchRangesMerge)?
                },
                Kont::AwaitBlocks { next, } =>
                    if let Some(ReceivedBlockTask {
                        async_token,
                        block_bytes,
                    }) = sklavenwelt.received_block_tasks.pop() {
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
                        log::debug!(
                            "finished, total_search_tree_blocks_removed: {}, total_external_value_blocks_removed: {}",
                            sklavenwelt.total_search_tree_blocks_removed,
                            sklavenwelt.total_external_value_blocks_removed,
                        );
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
                        search_ranges_merge::KontRequireBlockAsync {
                            block_ref,
                            async_token,
                            next,
                        },
                    ) => {
                        let wheel_ref = sklavenwelt.wheels.get(&block_ref.blockwheel_filename)
                            .ok_or_else(|| Error::WheelNotFound {
                                blockwheel_filename: block_ref.blockwheel_filename.clone(),
                            })?;
                        let rueckkopplung = sklavenwelt
                            .sendegeraet
                            .rueckkopplung(ReadBlockTarget::LoadBlock(ReadBlockTargetLoadBlock {
                                async_token: HideDebug(async_token),
                            }));
                        wheel_ref.meister
                            .read_block(
                                block_ref.block_id,
                                FsReadBlock::DemolishSearchTree { rueckkopplung, },
                                thread_pool,
                            )
                            .map_err(Error::BlockLoadReadBlockRequest)?;
                        merger_kont = next.scheduled()
                            .map_err(Error::SearchRangesMerge)?;
                    },
                    search_ranges_merge::Kont::AwaitBlocks(search_ranges_merge::KontAwaitBlocks {
                        next,
                    }) => {
                        sklavenwelt.kont = Some(Kont::AwaitBlocks { next, });
                        break;
                    },
                    search_ranges_merge::Kont::BlockFinished(search_ranges_merge::KontBlockFinished {
                        block_ref,
                        next,
                    }) => {
                        sklavenwelt.total_search_tree_blocks_removed += 1;
                        schedule_delete_block(sklavenwelt, block_ref, thread_pool)?;
                        merger_kont = next.proceed()
                            .map_err(Error::SearchRangesMerge)?;
                    },
                    search_ranges_merge::Kont::EmitDeprecated(search_ranges_merge::KontEmitDeprecated {
                        item,
                        next,
                    }) => {
                        #[allow(clippy::single_match)]
                        match item {
                            kv::KeyValuePair {
                                value_cell: kv::ValueCell {
                                    cell: kv::Cell::Value(storage::ValueRef::External(block_ref)),
                                    ..
                                },
                                ..
                            } => {
                                sklavenwelt.total_external_value_blocks_removed += 1;
                                schedule_delete_block(sklavenwelt, block_ref, thread_pool)?;
                            },
                            kv::KeyValuePair { value_cell: kv::ValueCell { cell: kv::Cell::Value(storage::ValueRef::Local(..)), .. }, .. } =>
                                unreachable!("totally unexpected ValueRef::Local value from `search_range_merge`"),
                            _ =>
                                (),
                        }
                        merger_kont = next.proceed()
                            .map_err(Error::SearchRangesMerge)?;
                    },
                    search_ranges_merge::Kont::EmitItem(search_ranges_merge::KontEmitItem {
                        next,
                        ..
                    }) => {
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

fn schedule_delete_block<E, J>(
    sklavenwelt: &mut Welt<E>,
    block_ref: BlockRef,
    thread_pool: &edeltraud::Handle<J>,
)
    -> Result<(), Error>
where E: EchoPolicy,
      J: From<blockwheel_fs::job::SklaveJob<wheels::WheelEchoPolicy<E>>>,
{
    let wheel_ref = sklavenwelt.wheels.get(&block_ref.blockwheel_filename)
        .ok_or_else(|| Error::WheelNotFound {
            blockwheel_filename: block_ref.blockwheel_filename.clone(),
        })?;
    let rueckkopplung = sklavenwelt
        .sendegeraet
        .rueckkopplung(DeleteBlockTarget);
    wheel_ref.meister
        .delete_block(
            block_ref.block_id,
            FsDeleteBlock::DemolishSearchTree { rueckkopplung, },
            thread_pool,
        )
        .map_err(Error::DeleteBlockRequest)?;
    sklavenwelt.pending_delete_tasks += 1;

    Ok(())
}

impl From<komm::UmschlagAbbrechen<ReadBlockTarget>> for Order {
    fn from(v: komm::UmschlagAbbrechen<ReadBlockTarget>) -> Self {
        Self::ReadBlockCancel(v)
    }
}

impl From<komm::Umschlag<Result<Bytes, blockwheel_fs::RequestReadBlockError>, ReadBlockTarget>> for Order {
    fn from(v: komm::Umschlag<Result<Bytes, blockwheel_fs::RequestReadBlockError>, ReadBlockTarget>) -> Self {
        Self::ReadBlock(OrderReadBlock {
            read_block_result: v.inhalt,
            target: v.stamp,
        })
    }
}

impl From<komm::UmschlagAbbrechen<DeleteBlockTarget>> for Order {
    fn from(v: komm::UmschlagAbbrechen<DeleteBlockTarget>) -> Self {
        Self::DeleteBlockCancel(v)
    }
}

impl From<komm::Umschlag<Result<blockwheel_fs::Deleted, blockwheel_fs::RequestDeleteBlockError>, DeleteBlockTarget>> for Order {
    fn from(v: komm::Umschlag<Result<blockwheel_fs::Deleted, blockwheel_fs::RequestDeleteBlockError>, DeleteBlockTarget>) -> Self {
        Self::DeleteBlock(OrderDeleteBlock {
            delete_block_result: v.inhalt,
            target: v.stamp,
        })
    }
}
