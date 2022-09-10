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
    Dummy(A),
}

pub struct OrderReadBlock {
    pub read_block_result: Result<Bytes, blockwheel_fs::RequestReadBlockError>,
    pub target: ReadBlockTarget,
}

pub enum ReadBlockTarget {
    LoadValue,
}

pub struct Welt<A> where A: AccessPolicy {
    pub kont: Option<Kont<A>>,
    pub meister_ref: o1::set::Ref,
    pub sendegeraet: komm::Sendegeraet<performer_sklave::Order<A>>,
    pub wheels: wheels::Wheels<A>,
    pub drop_bomb: komm::Rueckkopplung<performer_sklave::Order<A>, performer_sklave::LookupRangeMergeDrop>,
}

pub type Meister<A> = arbeitssklave::Meister<Welt<A>, Order<A>>;
pub type SklaveJob<A> = arbeitssklave::SklaveJob<Welt<A>, Order<A>>;

type SearchRangesMergeCps =
    search_ranges_merge::RangesMergeCps<Unique<Vec<performer::LookupRangeSource>>, performer::LookupRangeSource>;
type SearchRangesMergeKont =
    search_ranges_merge::Kont<Unique<Vec<performer::LookupRangeSource>>, performer::LookupRangeSource>;
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
pub enum Error {
    SearchRangesMerge(search_ranges_merge::Error),
    SendegeraetGone(komm::Error),
    WheelNotFound {
        blockwheel_filename: wheels::WheelFilename,
    },
    ValueLoadReadBlockRequest(arbeitssklave::Error),
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
        let (mut merger_kont, lookup_context) = match sklave_job.sklavenwelt_mut().kont.take().unwrap() {
            Kont::Start { merger, lookup_context, } => (
                merger.step()
                    .map_err(Error::SearchRangesMerge)?,
                lookup_context,
            ),
            Kont::AwaitBlocks { lookup_context, next, } => {

                todo!();
                // if let Some(ReceivedBlockTask { async_token, block_bytes, }) = env.incoming.received_block_tasks.pop() {
                //     next.block_arrived(async_token, block_bytes)
                //         .map_err(Error::SearchRangesMerge)?
                // } else {
                //     return Ok(JobDone::AwaitRetrieveBlockTasks { env, next, });
                // },
            },
            Kont::ReadyItem { key_value_pair, lookup_context, next, } => {
                let sklavenwelt = sklave_job.sklavenwelt_mut();
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
                sklave_job.sklavenwelt_mut().kont =
                    Some(Kont::AwaitItemNext { next, });
                continue;
            },
            Kont::AwaitItemNext { next, } => {

                // next.proceed().map_err(Error::SearchRangesMerge)?,
                todo!()
            },
            Kont::AwaitItemValue { key, version, lookup_context, next, } => {

                todo!()
            },
        };

        loop {
            match merger_kont {
                search_ranges_merge::Kont::RequireBlockAsync(
                    search_ranges_merge::KontRequireBlockAsync { block_ref, async_token, next, },
                ) => {

                    todo!()
                    // let wheel_ref = env.wheels.get(block_ref.blockwheel_filename.clone())
                    //     .ok_or_else(|| Error::WheelNotFound {
                    //         blockwheel_filename: block_ref.blockwheel_filename.clone(),
                    //     })?;
                    // env.outgoing.retrieve_block_tasks.push(RetrieveBlockTask { wheel_ref, block_ref, async_token, });
                    // merger_kont = next.scheduled()
                    //     .map_err(Error::SearchRangesMerge)?;
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

                    todo!()
                    // assert!(env.outgoing.is_empty());
                    // assert!(env.incoming.is_empty());
                    // return Ok(JobDone::Finished);
                },
            }
        }
    }

}
