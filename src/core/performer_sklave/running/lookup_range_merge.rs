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
    komm::{
        self,
        Echo,
    },
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
        SearchRangesMergeCps,
        SearchRangesMergeBlockNext,
        SearchRangesMergeItemNext,
    },
    HideDebug,
    EchoPolicy,
    LookupRangeStream,
};

pub enum Order<E> where E: EchoPolicy {
    Terminate,
    ReadBlock(OrderReadBlock),
    ItemNext(OrderItemNext<E>),
}

pub struct OrderReadBlock {
    pub read_block_result: Result<Bytes, blockwheel_fs::RequestReadBlockError>,
    pub target: ReadBlockTarget,
}

pub struct OrderItemNext<E> where E: EchoPolicy {
    pub lookup_context: E::LookupRange,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum ReadBlockTarget {
    LoadValue,
    LoadBlock(ReadBlockTargetLoadBlock),
}

#[derive(Debug)]
pub struct ReadBlockTargetLoadBlock {
    async_token: HideDebug<search_ranges_merge::AsyncToken<performer::LookupRangeSource>>,
}

pub struct Welt<E> where E: EchoPolicy {
    kont: Option<Kont<E>>,
    meister_ref: Ref,
    sendegeraet: komm::Sendegeraet<performer_sklave::Order<E>>,
    wheels: wheels::Wheels<E>,
    _drop_bomb: komm::Rueckkopplung<performer_sklave::Order<E>, performer_sklave::LookupRangeMergeDrop>,
    received_block_tasks: Vec<ReceivedBlockTask>,
    received_order_item_next: Option<OrderItemNext<E>>,
    received_value_bytes: Option<Bytes>,
}

impl<E> Welt<E> where E: EchoPolicy {
    pub fn new(
        merger: SearchRangesMergeCps,
        lookup_context: E::LookupRange,
        meister_ref: Ref,
        sendegeraet: komm::Sendegeraet<performer_sklave::Order<E>>,
        wheels: wheels::Wheels<E>,
        drop_bomb: komm::Rueckkopplung<performer_sklave::Order<E>, performer_sklave::LookupRangeMergeDrop>,
    )
        -> Self
    {
        Welt {
            kont: Some(Kont::Start { merger, lookup_context, }),
            meister_ref,
            sendegeraet,
            wheels,
            _drop_bomb: drop_bomb,
            received_block_tasks: Vec::new(),
            received_order_item_next: None,
            received_value_bytes: None,
        }
    }
}

pub type Meister<E> = arbeitssklave::Meister<Welt<E>, Order<E>>;
pub type SklaveJob<E> = arbeitssklave::SklaveJob<Welt<E>, Order<E>>;

enum Kont<E> where E: EchoPolicy {
    Start {
        merger: SearchRangesMergeCps,
        lookup_context: E::LookupRange,
    },
    AwaitBlocks {
        lookup_context: E::LookupRange,
        next: SearchRangesMergeBlockNext,
    },
    ReadyItem {
        key_value_pair: kv::KeyValuePair<kv::Value>,
        lookup_context: E::LookupRange,
        next: SearchRangesMergeItemNext,
    },
    AwaitItemNext {
        next: SearchRangesMergeItemNext,
    },
    AwaitItemValue {
        key: kv::Key,
        version: u64,
        lookup_context: E::LookupRange,
        next: SearchRangesMergeItemNext,
    },
}

#[derive(Debug)]
pub enum Error {
    OrphanSklave(arbeitssklave::Error),
    SearchRangesMerge(search_ranges_merge::Error),
    SendegeraetGone(komm::EchoError),
    WheelNotFound {
        blockwheel_filename: wheels::WheelFilename,
    },
    ValueLoadReadBlockRequest(arbeitssklave::Error),
    BlockLoadReadBlockRequest(arbeitssklave::Error),
    LoadBlock(blockwheel_fs::RequestReadBlockError),
    LoadValue(blockwheel_fs::RequestReadBlockError),
    ValueDeserialize(storage::Error),
}

pub fn run_job<E, P>(sklave_job: SklaveJob<E>, thread_pool: &P)
where E: EchoPolicy,
      P: edeltraud::ThreadPool<job::Job<E>>,
{
    if let Err(error) = job(sklave_job, thread_pool) {
        log::error!("terminated with an error: {error:?}");
    }
}

fn job<E, P>(mut sklave_job: SklaveJob<E>, thread_pool: &P) -> Result<(), Error>
where E: EchoPolicy,
      P: edeltraud::ThreadPool<job::Job<E>>,
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
                                    Order::Terminate =>
                                        return Ok(()),
                                    Order::ItemNext(order_item_next) => {
                                        assert!(sklavenwelt.received_order_item_next.is_none());
                                        sklavenwelt.received_order_item_next =
                                            Some(order_item_next);
                                    },
                                    Order::ReadBlock(OrderReadBlock {
                                        read_block_result: Ok(block_bytes),
                                        target: ReadBlockTarget::LoadBlock(ReadBlockTargetLoadBlock { async_token: HideDebug(async_token), }),
                                    }) =>
                                        sklavenwelt.received_block_tasks
                                            .push(ReceivedBlockTask { async_token, block_bytes, }),
                                    Order::ReadBlock(OrderReadBlock {
                                        read_block_result: Ok(block_bytes),
                                        target: ReadBlockTarget::LoadValue,
                                    }) => {
                                        let value_bytes = storage::value_block_deserialize(&block_bytes)
                                            .map_err(Error::ValueDeserialize)?;
                                        assert!(sklavenwelt.received_value_bytes.is_none());
                                        sklavenwelt.received_value_bytes =
                                            Some(value_bytes);
                                    },
                                    Order::ReadBlock(OrderReadBlock {
                                        read_block_result: Err(error),
                                        target: ReadBlockTarget::LoadBlock { .. },
                                    }) =>
                                        return Err(Error::LoadBlock(error)),
                                    Order::ReadBlock(OrderReadBlock {
                                        read_block_result: Err(error),
                                        target: ReadBlockTarget::LoadValue,
                                    }) =>
                                        return Err(Error::LoadValue(error)),
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

        'kont: loop {
            let (mut merger_kont, lookup_context) = match sklavenwelt.kont.take().unwrap() {
                Kont::Start { merger, lookup_context, } => (
                    merger.step()
                        .map_err(Error::SearchRangesMerge)?,
                    lookup_context,
                ),
                Kont::AwaitBlocks { lookup_context, next, } =>
                    if let Some(ReceivedBlockTask { async_token, block_bytes, }) = sklavenwelt.received_block_tasks.pop() {
                        (
                            next.block_arrived(async_token, block_bytes)
                                .map_err(Error::SearchRangesMerge)?,
                            lookup_context,
                        )
                    } else {
                        sklavenwelt.kont =
                            Some(Kont::AwaitBlocks { lookup_context, next, });
                        continue 'outer;
                    },
                Kont::ReadyItem { key_value_pair, lookup_context, next, } => {
                    let next_rueckkopplung = sklavenwelt
                        .sendegeraet
                        .rueckkopplung(performer_sklave::LookupRangeRoute {
                            meister_ref: sklavenwelt.meister_ref,
                        });
                    lookup_context
                        .commit_echo(komm::Streamzeug::Zeug {
                            zeug: key_value_pair,
                            mehr_stream: LookupRangeStream {
                                next: HideDebug(next_rueckkopplung),
                            },
                        })
                        .map_err(Error::SendegeraetGone)?;
                    sklavenwelt.kont =
                        Some(Kont::AwaitItemNext { next, });
                    continue 'kont;
                },
                Kont::AwaitItemNext { next, } =>
                    if let Some(OrderItemNext { lookup_context, }) = sklavenwelt.received_order_item_next.take() {
                        let merger_kont = next.proceed()
                            .map_err(Error::SearchRangesMerge)?;
                        (merger_kont, lookup_context)
                    } else {
                        sklavenwelt.kont = Some(Kont::AwaitItemNext { next, });
                        continue 'outer;
                    },
                Kont::AwaitItemValue { key, version, lookup_context, next, } =>
                    if let Some(value_bytes) = sklavenwelt.received_value_bytes.take() {
                        sklavenwelt.kont =
                            Some(Kont::ReadyItem {
                                key_value_pair: kv::KeyValuePair {
                                    key,
                                    value_cell: kv::ValueCell {
                                        version,
                                        cell: kv::Cell::Value(kv::Value { value_bytes, }),
                                    },
                                },
                                lookup_context,
                                next,
                            });
                        continue 'kont;
                    } else {
                        sklavenwelt.kont = Some(Kont::AwaitItemValue { key, version, lookup_context, next, });
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
                                performer_sklave::WheelRouteReadBlock::LookupRangeMerge {
                                    route: performer_sklave::LookupRangeRoute {
                                        meister_ref: sklavenwelt.meister_ref,
                                    },
                                    target: ReadBlockTarget::LoadBlock(ReadBlockTargetLoadBlock { async_token: HideDebug(async_token), }),
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
                        sklavenwelt.kont =
                            Some(Kont::AwaitBlocks { lookup_context, next, });
                        break;
                    },
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
                    ) => {
                        match item {
                            kv::KeyValuePair {
                                key,
                                value_cell: kv::ValueCell {
                                    version,
                                    cell: kv::Cell::Value(storage::OwnedValueBlockRef::Inline(value)),
                                },
                            } => {
                                sklavenwelt.kont = Some(Kont::ReadyItem {
                                    key_value_pair: kv::KeyValuePair {
                                        key,
                                        value_cell: kv::ValueCell {
                                            version,
                                            cell: kv::Cell::Value(value),
                                        },
                                    },
                                    lookup_context,
                                    next,
                                });
                            },
                            kv::KeyValuePair { key, value_cell: kv::ValueCell { version, cell: kv::Cell::Tombstone, }, } => {
                                sklavenwelt.kont = Some(Kont::ReadyItem {
                                    key_value_pair: kv::KeyValuePair {
                                        key,
                                        value_cell: kv::ValueCell {
                                            version,
                                            cell: kv::Cell::Tombstone,
                                        },
                                    },
                                    lookup_context,
                                    next,
                                });
                            },
                            kv::KeyValuePair {
                                key,
                                value_cell: kv::ValueCell {
                                    version,
                                    cell: kv::Cell::Value(storage::OwnedValueBlockRef::Ref(block_ref)),
                                },
                            } => {
                                let wheel_ref = sklavenwelt.wheels.get(&block_ref.blockwheel_filename)
                                    .ok_or_else(|| Error::WheelNotFound {
                                        blockwheel_filename: block_ref.blockwheel_filename.clone(),
                                    })?;
                                let rueckkopplung = sklavenwelt
                                    .sendegeraet
                                    .rueckkopplung(
                                        performer_sklave::WheelRouteReadBlock::LookupRangeMerge {
                                            route: performer_sklave::LookupRangeRoute {
                                                meister_ref: sklavenwelt.meister_ref,
                                            },
                                            target: ReadBlockTarget::LoadValue,
                                        },
                                    );
                                wheel_ref.meister
                                    .read_block(
                                        block_ref.block_id,
                                        rueckkopplung,
                                        &edeltraud::ThreadPoolMap::new(thread_pool),
                                    )
                                    .map_err(Error::ValueLoadReadBlockRequest)?;
                                sklavenwelt.kont =
                                    Some(Kont::AwaitItemValue { key, version, lookup_context, next, });
                            },
                        }
                        break;
                    },
                    search_ranges_merge::Kont::Finished => {
                        lookup_context
                            .commit_echo(komm::Streamzeug::NichtMehr)
                            .map_err(Error::SendegeraetGone)?;
                        return Ok(());
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
