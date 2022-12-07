use alloc_pool::{
    pool,
    bytes::{
        Bytes,
        BytesPool,
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
    wheels,
    storage,
    core::{
        performer,
        performer_sklave,
        search_tree_builder,
        search_ranges_merge,
        BlockRef,
        FsWriteBlock,
        SearchTreeBuilderCps,
        SearchTreeBuilderKont,
        SearchTreeBuilderBlockNext,
        SearchTreeBuilderBlockEntry,
        SearchTreeBuilderItemOrBlockNext,
        SearchRangesMergeCps,
        SearchRangesMergeKont,
        SearchRangesMergeBlockNext,
        SearchRangesMergeItemNext,
    },
    HideDebug,
    EchoPolicy,
};

#[allow(clippy::large_enum_variant)]
pub enum Order {
    ReadBlock(OrderReadBlock),
    WriteBlock(OrderWriteBlock),
    ReadBlockCancel(komm::UmschlagAbbrechen<ReadBlockTarget>),
    WriteBlockCancel(komm::UmschlagAbbrechen<WriteBlockTarget>),
}

pub struct OrderReadBlock {
    pub read_block_result: Result<Bytes, blockwheel_fs::RequestReadBlockError>,
    pub target: ReadBlockTarget,
}

pub struct OrderWriteBlock {
    pub write_block_result: Result<blockwheel_fs::block::Id, blockwheel_fs::RequestWriteBlockError>,
    pub target: WriteBlockTarget,
}

#[derive(Debug)]
pub enum ReadBlockTarget {
    LoadBlock(ReadBlockTargetLoadBlock),
}

#[derive(Debug)]
pub struct ReadBlockTargetLoadBlock {
    async_token: HideDebug<search_ranges_merge::AsyncToken<performer::LookupRangeSource>>,
}

#[derive(Debug)]
pub enum WriteBlockTarget {
    StoreBlock(WriteBlockTargetStoreBlock),
}

#[derive(Debug)]
pub struct WriteBlockTargetStoreBlock {
    async_ref: Ref,
    blockwheel_filename: wheels::WheelFilename,
}

pub struct Welt<E> where E: EchoPolicy {
    kont: Option<Kont>,
    meister_ref: Ref,
    parent_sendegeraet: komm::Sendegeraet<performer_sklave::Order<E>>,
    sendegeraet: komm::Sendegeraet<Order>,
    wheels: wheels::Wheels<E>,
    blocks_pool: BytesPool,
    block_entries_pool: pool::Pool<Vec<SearchTreeBuilderBlockEntry>>,
    tree_block_size: usize,
    maybe_feedback: Option<komm::Rueckkopplung<performer_sklave::Order<E>, performer_sklave::MergeSearchTreesDrop>>,
    received_block_tasks: Vec<ReceivedBlockTask>,
    written_block_tasks: Vec<WrittenBlockTask>,
}

impl<E> Welt<E> where E: EchoPolicy {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source_count_items: SearchRangesMergeCps,
        source_build: SearchRangesMergeCps,
        meister_ref: Ref,
        parent_sendegeraet: komm::Sendegeraet<performer_sklave::Order<E>>,
        sendegeraet: komm::Sendegeraet<Order>,
        wheels: wheels::Wheels<E>,
        blocks_pool: BytesPool,
        block_entries_pool: pool::Pool<Vec<SearchTreeBuilderBlockEntry>>,
        tree_block_size: usize,
        feedback: komm::Rueckkopplung<performer_sklave::Order<E>, performer_sklave::MergeSearchTreesDrop>,
    )
        -> Self
    {
        Welt {
            kont: Some(Kont::Busy {
                merge_kont: MergeKont::Start {
                    merger: source_count_items,
                },
                build_kont: BuildKont::CountItems {
                    items_count: 0,
                    merger_source_build: source_build,
                },
            }),
            meister_ref,
            parent_sendegeraet,
            sendegeraet,
            wheels,
            blocks_pool,
            block_entries_pool,
            tree_block_size,
            maybe_feedback: Some(feedback),
            received_block_tasks: Vec::new(),
            written_block_tasks: Vec::new(),
        }
    }
}

pub type Meister<E> = arbeitssklave::Meister<Welt<E>, Order>;
pub type SklaveJob<E> = arbeitssklave::SklaveJob<Welt<E>, Order>;

#[allow(clippy::large_enum_variant)]
enum Kont {
    Busy {
        merge_kont: MergeKont,
        build_kont: BuildKont,
    },
    Finished {
        items_count: usize,
        root_block: BlockRef,
    },
}

#[derive(Debug)]
pub enum Error {
    OrphanSklave(arbeitssklave::Error),
    SearchRangesMerge(search_ranges_merge::Error),
    SearchTreeBuilder(search_tree_builder::Error),
    ReadBlock(blockwheel_fs::RequestReadBlockError),
    WriteBlock(blockwheel_fs::RequestWriteBlockError),
    FeedbackCommit(arbeitssklave::Error),
    WheelNotFound { blockwheel_filename: wheels::WheelFilename, },
    BlockLoadReadBlockRequest(blockwheel_fs::Error),
    BlockStoreWriteBlockRequest(blockwheel_fs::Error),
    WheelIsGoneDuringReadBlock,
    WheelIsGoneDuringWriteBlock,
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
        if let Some(Kont::Busy { merge_kont: MergeKont::Start { .. }, .. }) = sklave_job.kont {
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
                                            .push(ReceivedBlockTask {
                                                async_token,
                                                block_bytes,
                                            }),
                                    Order::ReadBlock(OrderReadBlock {
                                        read_block_result: Err(error),
                                        target: ReadBlockTarget::LoadBlock { .. },
                                    }) =>
                                        return Err(Error::ReadBlock(error)),
                                    Order::WriteBlock(OrderWriteBlock {
                                        write_block_result: Ok(block_id),
                                        target: WriteBlockTarget::StoreBlock(WriteBlockTargetStoreBlock {
                                            async_ref,
                                            blockwheel_filename,
                                        }),
                                    }) =>
                                        sklavenwelt.written_block_tasks
                                            .push(WrittenBlockTask {
                                                async_ref,
                                                block_ref: BlockRef { blockwheel_filename, block_id, },
                                            }),
                                    Order::WriteBlock(OrderWriteBlock {
                                        write_block_result: Err(error),
                                        target: WriteBlockTarget::StoreBlock { .. },
                                    }) =>
                                        return Err(Error::WriteBlock(error)),
                                    Order::ReadBlockCancel(..) =>
                                        return Err(Error::WheelIsGoneDuringReadBlock),
                                    Order::WriteBlockCancel(..) =>
                                        return Err(Error::WheelIsGoneDuringWriteBlock),
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
            let (mut merge_kont, mut build_kont) = match sklavenwelt.kont.take().unwrap() {
                Kont::Busy { merge_kont, build_kont } =>
                    (merge_kont, build_kont),
                Kont::Finished { items_count, root_block, } => {
                    assert!(sklavenwelt.received_block_tasks.is_empty());
                    assert!(sklavenwelt.written_block_tasks.is_empty());
                    if let Some(feedback) = sklavenwelt.maybe_feedback.take() {
                        feedback
                            .commit(performer_sklave::MergeSearchTreesDone {
                                merged_search_tree_ref: root_block,
                                merged_search_tree_items_count: items_count,
                            })
                            .map_err(Error::FeedbackCommit)?;
                    }
                    return Ok(());
                },
            };

            loop {
                enum MergeKontState<K, A, I> {
                    Ready(K),
                    Await(A),
                    Idle(I),
                    Finished,
                }

                let merge_kont_state = match merge_kont {
                    MergeKont::Start { merger, } =>
                        MergeKontState::Ready(
                            merger.step()
                                .map_err(Error::SearchRangesMerge)?,
                        ),
                    MergeKont::ProceedAwaitBlocks { next, } =>
                        if let Some(ReceivedBlockTask { async_token, block_bytes, }) = sklavenwelt.received_block_tasks.pop() {
                            MergeKontState::Ready(
                                next.block_arrived(async_token, block_bytes)
                                    .map_err(Error::SearchRangesMerge)?,
                            )
                        } else {
                            MergeKontState::Await(MergeKont::ProceedAwaitBlocks { next, })
                        },
                    MergeKont::ProceedItem { item, next, } =>
                        match &mut build_kont {
                            BuildKont::CountItems { items_count, .. } => {
                                *items_count += 1;
                                MergeKontState::Ready(next.proceed().map_err(Error::SearchRangesMerge)?)
                            },
                            BuildKont::Active { item_arrived: item_arrived @ None, .. } => {
                                *item_arrived = Some(item);
                                MergeKontState::Ready(next.proceed().map_err(Error::SearchRangesMerge)?)
                            },
                            BuildKont::Active { item_arrived: Some(..), .. } =>
                                MergeKontState::Idle(MergeKont::ProceedItem { item, next, }),
                        },
                    MergeKont::Finished =>
                        MergeKontState::Finished,
                };

                enum BuildKontState<K, A, I> {
                    CountItems {
                        items_count: usize,
                        merger_source_build: SearchRangesMergeCps,
                    },
                    Active {
                        item_arrived: I,
                        kont: Active<K, A>,
                    },
                }

                enum Active<K, A> {
                    Ready(K),
                    Await(A),
                    Finished {
                        items_count: usize,
                        root_block: BlockRef,
                    },
                }

                let build_kont_state = match build_kont {
                    BuildKont::CountItems { items_count, merger_source_build, } =>
                        BuildKontState::CountItems { items_count, merger_source_build, },
                    BuildKont::Active { item_arrived, kont: BuildKontActive::Start { builder, }, } =>
                        BuildKontState::Active {
                            item_arrived,
                            kont: Active::Ready(
                                builder.step()
                                    .map_err(Error::SearchTreeBuilder)?,
                            ),
                        },
                    BuildKont::Active { item_arrived, kont: BuildKontActive::ProceedWrittenBlock { next, }, } =>
                        if let Some(WrittenBlockTask { async_ref, block_ref, }) = sklavenwelt.written_block_tasks.pop() {
                            BuildKontState::Active {
                                item_arrived,
                                kont: Active::Ready(
                                    next.block_processed(async_ref, block_ref)
                                        .map_err(Error::SearchTreeBuilder)?,
                                ),
                            }
                        } else {
                            BuildKontState::Active {
                                item_arrived,
                                kont: Active::Await(BuildKontActive::ProceedWrittenBlock { next, }),
                            }
                        },
                    BuildKont::Active { mut item_arrived, kont: BuildKontActive::ProceedItemOrWrittenBlock { next, }, } =>
                        if let Some(WrittenBlockTask { async_ref, block_ref, }) = sklavenwelt.written_block_tasks.pop() {
                            BuildKontState::Active {
                                item_arrived,
                                kont: Active::Ready(
                                    next.block_processed(async_ref, block_ref)
                                        .map_err(Error::SearchTreeBuilder)?,
                                ),
                            }
                        } else if let Some(item) = item_arrived.take() {
                            BuildKontState::Active {
                                item_arrived,
                                kont: Active::Ready(
                                    next.item_arrived(item)
                                        .map_err(Error::SearchTreeBuilder)?,
                                ),
                            }
                        } else {
                            BuildKontState::Active {
                                item_arrived,
                                kont: Active::Await(BuildKontActive::ProceedItemOrWrittenBlock { next, }),
                            }
                        },
                    BuildKont::Active { item_arrived, kont: BuildKontActive::Finished { items_count, root_block, }, .. } =>
                        BuildKontState::Active { item_arrived, kont: Active::Finished { items_count, root_block, }, },
                };

                match (merge_kont_state, build_kont_state) {
                    (MergeKontState::Ready(merger_kont), BuildKontState::CountItems { items_count, merger_source_build, }) => {
                        merge_kont = job_step_merger(sklavenwelt, merger_kont, thread_pool)?;
                        build_kont = BuildKont::CountItems { items_count, merger_source_build, };
                    },
                    (MergeKontState::Await(await_merge_kont), BuildKontState::CountItems { items_count, merger_source_build, }) => {
                        sklavenwelt.kont = Some(Kont::Busy {
                            merge_kont: await_merge_kont,
                            build_kont: BuildKont::CountItems { items_count, merger_source_build, },
                        });
                        continue 'outer;
                    },
                    (MergeKontState::Idle(..), BuildKontState::CountItems { .. }) =>
                        unreachable!(),
                    (MergeKontState::Finished, BuildKontState::CountItems { items_count, merger_source_build, }) => {
                        sklavenwelt.kont = Some(Kont::Busy {
                            merge_kont: MergeKont::Start {
                                merger: merger_source_build,
                            },
                            build_kont: BuildKont::Active {
                                item_arrived: None,
                                kont: BuildKontActive::Start {
                                    builder: search_tree_builder::BuilderCps::new(
                                        sklavenwelt.block_entries_pool.clone(),
                                        search_tree_builder::Params {
                                            tree_items_count: items_count,
                                            tree_block_size: sklavenwelt.tree_block_size,
                                        },
                                    ),
                                },
                            },
                        });
                        continue 'outer;
                    },

                    (
                        MergeKontState::Ready(merger_kont),
                        BuildKontState::Active { item_arrived, kont: Active::Ready(builder_kont), },
                    ) => {
                        merge_kont = job_step_merger(sklavenwelt, merger_kont, thread_pool)?;
                        build_kont = job_step_builder(sklavenwelt, item_arrived, builder_kont, thread_pool)?;
                    },
                    (
                        MergeKontState::Ready(merger_kont),
                        BuildKontState::Active { item_arrived, kont: Active::Await(await_build_kont), },
                    ) => {
                        merge_kont = job_step_merger(sklavenwelt, merger_kont, thread_pool)?;
                        build_kont = BuildKont::Active { item_arrived, kont: await_build_kont, };
                    },
                    (
                        MergeKontState::Await(await_merge_kont),
                        BuildKontState::Active { item_arrived, kont: Active::Ready(builder_kont), },
                    ) => {
                        merge_kont = await_merge_kont;
                        build_kont = job_step_builder(sklavenwelt, item_arrived, builder_kont, thread_pool)?;
                    },
                    (
                        MergeKontState::Await(await_merge_kont),
                        BuildKontState::Active { item_arrived, kont: Active::Await(await_build_kont), },
                    ) => {
                        sklavenwelt.kont = Some(Kont::Busy {
                            merge_kont: await_merge_kont,
                            build_kont: BuildKont::Active { item_arrived, kont: await_build_kont, },
                        });
                        continue 'outer;
                    },
                    (
                        MergeKontState::Idle(idle_merge_kont),
                        BuildKontState::Active { item_arrived, kont: Active::Ready(builder_kont), },
                    ) => {
                        merge_kont = idle_merge_kont;
                        build_kont = job_step_builder(sklavenwelt, item_arrived, builder_kont, thread_pool)?;
                    },
                    (
                        MergeKontState::Idle(idle_merge_kont),
                        BuildKontState::Active { item_arrived, kont: Active::Await(await_build_kont), },
                    ) => {
                        sklavenwelt.kont = Some(Kont::Busy {
                            merge_kont: idle_merge_kont,
                            build_kont: BuildKont::Active { item_arrived, kont: await_build_kont, },
                        });
                        continue 'outer;
                    },
                    (
                        MergeKontState::Finished,
                        BuildKontState::Active { item_arrived, kont: Active::Ready(builder_kont), },
                    ) => {
                        merge_kont = MergeKont::Finished;
                        build_kont = job_step_builder(sklavenwelt, item_arrived, builder_kont, thread_pool)?;
                    },
                    (
                        MergeKontState::Finished,
                        BuildKontState::Active { item_arrived, kont: Active::Await(await_build_kont), },
                    ) => {
                        sklavenwelt.kont = Some(Kont::Busy {
                            merge_kont: MergeKont::Finished,
                            build_kont: BuildKont::Active { item_arrived, kont: await_build_kont, },
                        });
                        continue 'outer;
                    },
                    (
                        MergeKontState::Ready(merger_kont),
                        BuildKontState::Active { item_arrived, kont: Active::Finished { items_count, root_block, }, },
                    ) => {
                        merge_kont = job_step_merger(sklavenwelt, merger_kont, thread_pool)?;
                        build_kont = BuildKont::Active { item_arrived, kont: BuildKontActive::Finished { items_count, root_block, }, };
                    },
                    (
                        MergeKontState::Await(await_merge_kont),
                        BuildKontState::Active { item_arrived, kont: Active::Finished { items_count, root_block, }, },
                    ) => {
                        sklavenwelt.kont = Some(Kont::Busy {
                            merge_kont: await_merge_kont,
                            build_kont: BuildKont::Active { item_arrived, kont: BuildKontActive::Finished { items_count, root_block, }, },
                        });
                        continue 'outer;
                    },
                    (MergeKontState::Idle(..), BuildKontState::Active { kont: Active::Finished { .. }, .. }) =>
                        unreachable!(),
                    (MergeKontState::Finished, BuildKontState::Active { kont: Active::Finished { items_count, root_block, }, .. }) => {
                        sklavenwelt.kont = Some(Kont::Finished { items_count, root_block, });
                        break;
                    },
                }
            }
        }
    }
}

fn job_step_merger<E, J>(
    sklavenwelt: &mut Welt<E>,
    mut merger_kont: SearchRangesMergeKont,
    thread_pool: &edeltraud::Handle<J>,
)
    -> Result<MergeKont, Error>
where E: EchoPolicy,
      J: From<blockwheel_fs::job::SklaveJob<wheels::WheelEchoPolicy<E>>>,
{
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
                    .parent_sendegeraet
                    .rueckkopplung(
                        performer_sklave::WheelRouteReadBlock::MergeSearchTrees {
                            route: performer_sklave::MergeSearchTreesRoute {
                                meister_ref: sklavenwelt.meister_ref,
                            },
                            target: ReadBlockTarget::LoadBlock(ReadBlockTargetLoadBlock { async_token: HideDebug(async_token), }),
                        },
                    );
                wheel_ref.meister
                    .read_block(
                        block_ref.block_id,
                        rueckkopplung,
                        thread_pool,
                    )
                    .map_err(Error::BlockLoadReadBlockRequest)?;
                merger_kont = next.scheduled()
                    .map_err(Error::SearchRangesMerge)?;
            },
            search_ranges_merge::Kont::AwaitBlocks(search_ranges_merge::KontAwaitBlocks { next, }) =>
                return Ok(MergeKont::ProceedAwaitBlocks { next, }),
            search_ranges_merge::Kont::BlockFinished(search_ranges_merge::KontBlockFinished { next, .. }) => {
                merger_kont = next.proceed()
                    .map_err(Error::SearchRangesMerge)?;
            },
            search_ranges_merge::Kont::EmitDeprecated(search_ranges_merge::KontEmitDeprecated { next, .. }) => {
                merger_kont = next.proceed()
                    .map_err(Error::SearchRangesMerge)?;
            },
            search_ranges_merge::Kont::EmitItem(
                search_ranges_merge::KontEmitItem { item, next, },
            ) =>
                return Ok(MergeKont::ProceedItem { item, next, }),
            search_ranges_merge::Kont::Finished =>
                return Ok(MergeKont::Finished),
        }
    }
}

fn job_step_builder<E, J>(
    sklavenwelt: &mut Welt<E>,
    item_arrived: Option<kv::KeyValuePair<storage::ValueRef>>,
    mut builder_kont: SearchTreeBuilderKont,
    thread_pool: &edeltraud::Handle<J>,
)
    -> Result<BuildKont, Error>
where E: EchoPolicy,
      J: From<blockwheel_fs::job::SklaveJob<wheels::WheelEchoPolicy<E>>>,
{
    loop {
        match builder_kont {
            search_tree_builder::Kont::PollNextItemOrProcessedBlock(
                search_tree_builder::KontPollNextItemOrProcessedBlock {
                    next,
                },
            ) =>
                return Ok(BuildKont::Active { item_arrived, kont: BuildKontActive::ProceedItemOrWrittenBlock { next, }, }),
            search_tree_builder::Kont::PollProcessedBlock(
                search_tree_builder::KontPollProcessedBlock { next, },
            ) =>
                return Ok(BuildKont::Active { item_arrived, kont: BuildKontActive::ProceedWrittenBlock { next, }, }),
            search_tree_builder::Kont::ProcessBlockAsync(
                search_tree_builder::KontProcessBlockAsync {
                    node_type,
                    mut block_entries,
                    async_ref,
                    next,
                },
            ) => {
                // acquire target wheel
                let wheel_ref = sklavenwelt.wheels.acquire();

                // serialize block
                let block_bytes = sklavenwelt.blocks_pool.lend();
                let mut serialize_kont =
                    storage::BlockSerializer::start(node_type, block_entries.len(), block_bytes);
                let mut block_entries_iter = block_entries.drain(..);
                let block_bytes = loop {
                    match serialize_kont {
                        storage::BlockSerializerContinue::Done(block_bytes) =>
                            break block_bytes.freeze(),
                        storage::BlockSerializerContinue::More(serializer) => {
                            let block_entry = block_entries_iter.next().unwrap();
                            let key = block_entry.item.key;
                            let mut value_cell = block_entry.item.value_cell;
                            value_cell.maybe_collapse(&wheel_ref.blockwheel_filename);

                            let entry = storage::Entry {
                                jump_ref: storage::JumpRef::from_maybe_block_ref(
                                    &block_entry.child_block_ref,
                                    &wheel_ref.blockwheel_filename,
                                ),
                                key,
                                value_cell,
                            };
                            serialize_kont = serializer.entry(entry);
                        },
                    }
                };

                let rueckkopplung = sklavenwelt
                    .sendegeraet
                    .rueckkopplung(
                        WriteBlockTarget::StoreBlock(WriteBlockTargetStoreBlock {
                            async_ref,
                            blockwheel_filename: wheel_ref.blockwheel_filename.clone(),
                        }),
                    );
                wheel_ref.meister
                    .write_block(
                        block_bytes,
                        FsWriteBlock::MergeSearchTrees { rueckkopplung, },
                        thread_pool,
                    )
                    .map_err(Error::BlockStoreWriteBlockRequest)?;
                builder_kont = next.process_scheduled()
                    .map_err(Error::SearchTreeBuilder)?;
            },
            search_tree_builder::Kont::Finished { items_count, root_block_ref: root_block, } =>
                return Ok(BuildKont::Active { item_arrived, kont: BuildKontActive::Finished { items_count, root_block, }, }),
        }
    }
}

struct ReceivedBlockTask {
    async_token: search_ranges_merge::AsyncToken<performer::LookupRangeSource>,
    block_bytes: Bytes,
}

struct WrittenBlockTask {
    async_ref: Ref,
    block_ref: BlockRef,
}

enum MergeKont {
    Start {
        merger: SearchRangesMergeCps,
    },
    ProceedAwaitBlocks {
        next: SearchRangesMergeBlockNext,
    },
    ProceedItem {
        item: kv::KeyValuePair<storage::ValueRef>,
        next: SearchRangesMergeItemNext,
    },
    Finished,
}

#[allow(clippy::large_enum_variant)]
enum BuildKont {
    CountItems {
        items_count: usize,
        merger_source_build: SearchRangesMergeCps,
    },
    Active {
        item_arrived: Option<kv::KeyValuePair<storage::ValueRef>>,
        kont: BuildKontActive,
    },
}

enum BuildKontActive {
    Start {
        builder: SearchTreeBuilderCps,
    },
    ProceedItemOrWrittenBlock {
        next: SearchTreeBuilderItemOrBlockNext,
    },
    ProceedWrittenBlock {
        next: SearchTreeBuilderBlockNext,
    },
    Finished {
        items_count: usize,
        root_block: BlockRef,
    },
}

impl From<komm::UmschlagAbbrechen<ReadBlockTarget>> for Order {
    fn from(v: komm::UmschlagAbbrechen<ReadBlockTarget>) -> Order {
        Order::ReadBlockCancel(v)
    }
}

impl From<komm::UmschlagAbbrechen<WriteBlockTarget>> for Order {
    fn from(v: komm::UmschlagAbbrechen<WriteBlockTarget>) -> Order {
        Order::WriteBlockCancel(v)
    }
}

impl From<komm::Umschlag<Result<blockwheel_fs::block::Id, blockwheel_fs::RequestWriteBlockError>, WriteBlockTarget>> for Order {
    fn from(v: komm::Umschlag<Result<blockwheel_fs::block::Id, blockwheel_fs::RequestWriteBlockError>, WriteBlockTarget>) -> Order {
        Order::WriteBlock(OrderWriteBlock {
            write_block_result: v.inhalt,
            target: v.stamp,
        })
    }
}
