use alloc_pool::{
    Unique,
    bytes::{
        Bytes,
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
    },
    AccessPolicy,
    LookupRangeStream,
    KeyValueStreamItem,
};

pub enum Order<A> where A: AccessPolicy {
    Terminate,
    ReadBlock(OrderReadBlock),
    ItemNext(OrderItemNext<A>),
}

pub struct OrderReadBlock {
    pub read_block_result: Result<Bytes, blockwheel_fs::RequestReadBlockError>,
    pub target: ReadBlockTarget,
}

pub struct OrderItemNext<A> where A: AccessPolicy {
    pub lookup_context: komm::Rueckkopplung<A::Order, A::LookupRange>,
}

pub enum ReadBlockTarget {
    LoadValue,
    LoadBlock {
        async_token: search_ranges_merge::AsyncToken<performer::LookupRangeSource>,
    },
}

pub struct Welt<A> where A: AccessPolicy {
    pub kont: Option<Kont<A>>,
    pub meister_ref: o1::set::Ref,
    pub sendegeraet: komm::Sendegeraet<performer_sklave::Order<A>>,
    pub wheels: wheels::Wheels<A>,
    pub drop_bomb: komm::Rueckkopplung<performer_sklave::Order<A>, performer_sklave::LookupRangeMergeDrop>,
    pub incoming_orders: Vec<Order<A>>,
    pub delayed_orders: Vec<Order<A>>,
}

pub type Meister<A> = arbeitssklave::Meister<Welt<A>, Order<A>>;
pub type SklaveJob<A> = arbeitssklave::SklaveJob<Welt<A>, Order<A>>;

type SearchRangesMergeCps =
    search_ranges_merge::RangesMergeCps<Unique<Vec<performer::LookupRangeSource>>, performer::LookupRangeSource>;
type SearchRangesMergeBlockNext =
    search_ranges_merge::KontAwaitBlocksNext<Unique<Vec<performer::LookupRangeSource>>, performer::LookupRangeSource>;
type SearchRangesMergeItemNext =
    search_ranges_merge::KontEmitItemNext<Unique<Vec<performer::LookupRangeSource>>, performer::LookupRangeSource>;

pub enum Kont<A> where A: AccessPolicy {
    Start {
        merger: SearchRangesMergeCps,
        lookup_context: komm::Rueckkopplung<A::Order, A::LookupRange>,
    },
    AwaitBlocks {
        lookup_context: komm::Rueckkopplung<A::Order, A::LookupRange>,
        next: SearchRangesMergeBlockNext,
    },
    ReadyItem {
        key_value_pair: kv::KeyValuePair<kv::Value>,
        lookup_context: komm::Rueckkopplung<A::Order, A::LookupRange>,
        next: SearchRangesMergeItemNext,
    },
    AwaitItemNext {
        next: SearchRangesMergeItemNext,
    },
    AwaitItemValue {
        key: kv::Key,
        version: u64,
        lookup_context: komm::Rueckkopplung<A::Order, A::LookupRange>,
        next: SearchRangesMergeItemNext,
    },
}

#[derive(Debug)]
enum Error {
    OrphanSklave(arbeitssklave::Error),
    SearchRangesMerge(search_ranges_merge::Error),
    SendegeraetGone(komm::Error),
    WheelNotFound {
        blockwheel_filename: wheels::WheelFilename,
    },
    ValueLoadReadBlockRequest(arbeitssklave::Error),
    BlockLoadReadBlockRequest(arbeitssklave::Error),
    LoadBlock(blockwheel_fs::RequestReadBlockError),
    LoadValue(blockwheel_fs::RequestReadBlockError),
    ValueDeserialize(storage::Error),
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
                            arbeitssklave::SklavenBefehl::Mehr { befehl, mut mehr_befehle, } => {
                                mehr_befehle
                                    .sklavenwelt_mut()
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

        'kont: loop {
            let sklavenwelt = sklave_job.sklavenwelt_mut();
            sklavenwelt.incoming_orders.extend(sklavenwelt.delayed_orders.drain(..));

            let (mut merger_kont, lookup_context) = match sklavenwelt.kont.take().unwrap() {
                Kont::Start { merger, lookup_context, } => (
                    merger.step()
                        .map_err(Error::SearchRangesMerge)?,
                    lookup_context,
                ),
                Kont::AwaitBlocks { lookup_context, next, } =>
                    loop {
                        match sklavenwelt.incoming_orders.pop() {
                            None => {
                                sklavenwelt.kont = Some(Kont::AwaitBlocks { lookup_context, next, });
                                continue 'outer;
                            },
                            Some(Order::Terminate) =>
                                return Ok(()),
                            Some(Order::ReadBlock(OrderReadBlock {
                                read_block_result: Ok(block_bytes),
                                target: ReadBlockTarget::LoadBlock { async_token, },
                            })) => {
                                let merger_kont = next.block_arrived(async_token, block_bytes)
                                    .map_err(Error::SearchRangesMerge)?;
                                break (merger_kont, lookup_context);
                            },
                            Some(Order::ReadBlock(OrderReadBlock {
                                read_block_result: Err(error),
                                target: ReadBlockTarget::LoadBlock { .. },
                            })) =>
                                return Err(Error::LoadBlock(error)),
                            Some(other_order) =>
                                sklavenwelt.delayed_orders.push(other_order),
                        }
                    },
                Kont::ReadyItem { key_value_pair, lookup_context, next, } => {
                    let next_rueckkopplung = sklavenwelt
                        .sendegeraet
                        .rueckkopplung(performer_sklave::LookupRangeRoute {
                            meister_ref: sklavenwelt.meister_ref,
                        });
                    lookup_context
                        .commit(KeyValueStreamItem::KeyValue {
                            key_value_pair,
                            next: LookupRangeStream {
                                next: next_rueckkopplung,
                            },
                        })
                        .map_err(Error::SendegeraetGone)?;
                    sklavenwelt.kont =
                        Some(Kont::AwaitItemNext { next, });
                    continue 'kont;
                },
                Kont::AwaitItemNext { next, } =>
                    loop {
                        match sklavenwelt.incoming_orders.pop() {
                            None => {
                                sklavenwelt.kont = Some(Kont::AwaitItemNext { next, });
                                continue 'outer;
                            },
                            Some(Order::Terminate) =>
                                return Ok(()),
                            Some(Order::ItemNext(OrderItemNext { lookup_context, })) => {
                                let merger_kont = next.proceed()
                                    .map_err(Error::SearchRangesMerge)?;
                                break (merger_kont, lookup_context);
                            },
                            Some(other_order) =>
                                sklavenwelt.delayed_orders.push(other_order),
                        }
                    },
                Kont::AwaitItemValue { key, version, lookup_context, next, } =>
                    loop {
                        match sklavenwelt.incoming_orders.pop() {
                            None => {
                                sklavenwelt.kont = Some(Kont::AwaitItemValue { key, version, lookup_context, next, });
                                continue 'outer;
                            },
                            Some(Order::Terminate) =>
                                return Ok(()),
                            Some(Order::ReadBlock(OrderReadBlock {
                                read_block_result: Ok(block_bytes),
                                target: ReadBlockTarget::LoadValue,
                            })) => {
                                let value_bytes = storage::value_block_deserialize(&block_bytes)
                                    .map_err(Error::ValueDeserialize)?;
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
                            },
                            Some(Order::ReadBlock(OrderReadBlock {
                                read_block_result: Err(error),
                                target: ReadBlockTarget::LoadValue,
                            })) =>
                                return Err(Error::LoadValue(error)),
                            Some(other_order) =>
                                sklavenwelt.delayed_orders.push(other_order),
                        }
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
                                    target: ReadBlockTarget::LoadBlock { async_token, },
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
                        sklave_job.sklavenwelt_mut().kont =
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
                        let sklavenwelt = sklave_job.sklavenwelt_mut();
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
                            .commit(KeyValueStreamItem::NoMore)
                            .map_err(Error::SendegeraetGone)?;
                        return Ok(());
                    },
                }
            }
        }
    }
}
