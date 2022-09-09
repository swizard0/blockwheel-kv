use std::{
    mem,
};

use crate::{
    job,
    wheels,
    storage,
    core::{
        performer,
        performer_sklave::{
            Welt,
            Error,
            Order,
            Pools,
            Context,
            OrderWheel,
            WheelRouteIterBlocksInit,
            WheelRouteIterBlocksNext,
        },
    },
    AccessPolicy,
};

use arbeitssklave::{
    komm,
};

pub struct WeltState {
    mode: WeltStateMode,
    forest: performer::SearchForest,
    blocks_total: usize,
}

impl WeltState {
    pub fn new() -> Self {
        WeltState {
            mode: WeltStateMode::NeedIterBlocksRequest,
            forest: performer::SearchForest::new(),
            blocks_total: 0,
        }
    }
}

pub enum Outcome<A> where A: AccessPolicy {
    Rasten {
        loading: WeltState,
    },
    Done {
        performer: performer::Performer<Context<A>>,
    },
}

enum WeltStateMode {
    NeedIterBlocksRequest,
    Loading { wheels_left: usize, },
}

pub fn job<A, P>(
    mut welt_state: WeltState,
    sklavenwelt: &mut Welt<A>,
    thread_pool: &P,
)
    -> Result<Outcome<A>, Error>
where A: AccessPolicy,
      P: edeltraud::ThreadPool<job::Job<A>>,
{
    loop {
        match mem::replace(&mut welt_state.mode, WeltStateMode::NeedIterBlocksRequest) {

            WeltStateMode::NeedIterBlocksRequest => {
                let wheels_iter = sklavenwelt.env.wheels.iter();
                let mut wheels_left = 0;
                for wheel_ref in wheels_iter {
                    wheel_ref.meister
                        .iter_blocks_init(
                            sklavenwelt.env
                                .sendegeraet
                                .rueckkopplung(WheelRouteIterBlocksInit {
                                    blockwheel_filename: wheel_ref.blockwheel_filename.clone(),
                                }),
                            &edeltraud::ThreadPoolMap::new(thread_pool),
                        )
                        .map_err(|error| Error::WheelIterBlocksInit {
                            wheel_filename: wheel_ref.blockwheel_filename.clone(),
                            error,
                        })?;
                    wheels_left += 1;
                }
                welt_state.mode =
                    WeltStateMode::Loading { wheels_left, };
                log::debug!("iter blocks requests sent to {wheels_left:?} wheels");
            },

            WeltStateMode::Loading { wheels_left, } if wheels_left == 0 => {
                log::info!(
                    "loading done, {} search_trees restored within {} blocks",
                    welt_state.forest.len(),
                    welt_state.blocks_total,
                );

                let pools = Pools::new();
                let performer = performer::Performer::new(
                    sklavenwelt.env.params.clone(),
                    sklavenwelt.env.version_provider.clone(),
                    pools.kv_pool.clone(),
                    pools.sources_pool.clone(),
                    pools.block_entry_steps_pool.clone(),
                    welt_state.forest,
                );

                return Ok(Outcome::Done { performer, });
            },

            WeltStateMode::Loading { mut wheels_left, } =>
                loop {
                    match sklavenwelt.env.incoming_orders.pop() {
                        None => {
                            welt_state.mode =
                                WeltStateMode::Loading { wheels_left, };
                            return Ok(Outcome::Rasten {
                                loading: welt_state,
                            });
                        },

                        Some(Order::Wheel(OrderWheel::IterBlocksInit(komm::Umschlag {
                            payload: blockwheel_fs::IterBlocks { iterator_next, blocks_total_count, blocks_total_size, },
                            stamp: WheelRouteIterBlocksInit { blockwheel_filename, },
                        }))) => {
                            log::debug!(
                                "initial blocks_iter received for {:?}: blocks_total_count = {}, blocks_total_size = {}",
                                blockwheel_filename,
                                blocks_total_count,
                                blocks_total_size,
                            );
                            let wheel_ref = sklavenwelt.env
                                .wheels
                                .get(&blockwheel_filename)
                                .ok_or_else(|| Error::WheelIterBlocksGetFailed {
                                    blockwheel_filename: blockwheel_filename.clone(),
                                })?;
                            wheel_ref.meister
                                .iter_blocks_next(
                                    iterator_next,
                                    sklavenwelt.env
                                        .sendegeraet
                                        .rueckkopplung(WheelRouteIterBlocksNext {
                                            blockwheel_filename,
                                        }),
                                    &edeltraud::ThreadPoolMap::new(thread_pool),
                                )
                                .map_err(|error| Error::WheelIterBlocksNext {
                                    wheel_filename: wheel_ref.blockwheel_filename.clone(),
                                    error,
                                })?;
                        },
                        Some(Order::Wheel(OrderWheel::IterBlocksNext(komm::Umschlag {
                            payload: blockwheel_fs::IterBlocksItem::Block { block_id, block_bytes, iterator_next, },
                            stamp: WheelRouteIterBlocksNext { blockwheel_filename, },
                        }))) => {
                            let block_ref = wheels::BlockRef {
                                blockwheel_filename: blockwheel_filename.clone(),
                                block_id,
                            };

                            welt_state.blocks_total += 1;
                            let deserializer = match storage::block_deserialize_iter(&block_bytes) {
                                Ok(deserializer) =>
                                    deserializer,
                                Err(storage::Error::InvalidBlockMagic { expected, provided, }) => {
                                    log::debug!("skipping block {:?} (invalid magic provided: {}, expected: {})", block_ref, provided, expected);
                                    continue;
                                },
                                Err(error) =>
                                    return Err(Error::DeserializeBlock { block_ref, error, }),
                            };
                            match deserializer.block_header().node_type {
                                storage::NodeType::Root { tree_entries_count, } => {
                                    log::debug!("root search_tree found with {:?} entries in {:?}", tree_entries_count, block_ref);
                                    welt_state.forest.add_constructed(block_ref, tree_entries_count);
                                },
                                storage::NodeType::Leaf =>
                                    (),
                            }

                            let wheel_ref = sklavenwelt.env
                                .wheels
                                .get(&blockwheel_filename)
                                .ok_or_else(|| Error::WheelIterBlocksGetFailed {
                                    blockwheel_filename: blockwheel_filename.clone(),
                                })?;
                            wheel_ref.meister
                                .iter_blocks_next(
                                    iterator_next,
                                    sklavenwelt.env
                                        .sendegeraet
                                        .rueckkopplung(WheelRouteIterBlocksNext {
                                            blockwheel_filename,
                                        }),
                                    &edeltraud::ThreadPoolMap::new(thread_pool),
                                )
                                .map_err(|error| Error::WheelIterBlocksNext {
                                    wheel_filename: wheel_ref.blockwheel_filename.clone(),
                                    error,
                                })?;
                        },
                        Some(Order::Wheel(OrderWheel::IterBlocksNext(komm::Umschlag {
                            payload: blockwheel_fs::IterBlocksItem::NoMoreBlocks,
                            stamp: WheelRouteIterBlocksNext { blockwheel_filename, },
                        }))) => {
                            log::debug!("iter blocks done on {blockwheel_filename:?}");
                            wheels_left -= 1;
                        },
                        Some(Order::Wheel(OrderWheel::IterBlocksInitCancel(
                            komm::UmschlagAbbrechen { stamp: WheelRouteIterBlocksInit { blockwheel_filename, }, },
                        ))) =>
                            return Err(Error::WheelIterBlocksInitCanceled { blockwheel_filename, }),
                        Some(Order::Wheel(OrderWheel::IterBlocksNextCancel(
                            komm::UmschlagAbbrechen { stamp: WheelRouteIterBlocksNext { blockwheel_filename, }, },
                        ))) =>
                            return Err(Error::WheelIterBlocksNextCanceled { blockwheel_filename, }),
                        Some(other_order) =>
                            sklavenwelt.env.delayed_orders.push(other_order),
                    }
                },

        }
    }
}
