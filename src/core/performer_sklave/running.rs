use arbeitssklave::{
    komm::{
        self,
        Echo,
    },
};

use crate::{
    wheels,
    core::{
        performer,
        performer_sklave::{
            Welt,
            Order,
            Pools,
            Context,
            OrderWheel,
            OrderRequest,
            OrderRequestInfo,
            OrderRequestInsert,
            OrderRequestLookupRange,
            OrderRequestLookupRangeNext,
            OrderRequestRemove,
            OrderRequestFlush,
            InfoRoute,
            LookupRangeRoute,
            LookupRangeMergeDrop,
            LookupRangeStream,
            FlushButcherDone,
            FlushButcherDrop,
            MergeSearchTreesDone,
            MergeSearchTreesDrop,
            MergeSearchTreesRoute,
            DemolishSearchTreeDone,
            DemolishSearchTreeDrop,
            DemolishSearchTreeRoute,
            WheelRouteInfo,
            WheelRouteFlush,
            WheelRouteReadBlock,
            WheelRouteWriteBlock,
            WheelRouteDeleteBlock,
        },
    },
    Info,
    Flushed,
    Removed,
    Inserted,
    WheelInfo,
    EchoPolicy,
};

pub mod flush_butcher;
pub mod lookup_range_merge;
pub mod merge_search_trees;
pub mod demolish_search_tree;

pub struct WeltState<E> where E: EchoPolicy {
    kont: Kont<E>,
    pools: Pools,
    active_flush: Option<ActiveFlush<E>>,
}

impl<E> WeltState<E> where E: EchoPolicy {
    pub fn new(performer: performer::Performer<Context<E>>, pools: Pools) -> Self {
        WeltState {
            kont: Kont::Start { performer, },
            pools,
            active_flush: None,
        }
    }
}

enum ActiveFlush<E> where E: EchoPolicy {
    Kv,
    Wheels {
        wheels_left: usize,
        echo: E::Flush,
    },
}

pub struct PendingInfo<E> where E: EchoPolicy {
    info: Info,
    echo: E::Info,
    wheels_left: usize,
}

pub enum Outcome<E> where E: EchoPolicy {
    Rasten { running: WeltState<E>, },
}

enum Kont<E> where E: EchoPolicy {
    Start {
        performer: performer::Performer<Context<E>>,
    },
    StepPoll {
        next: performer::KontPollNext<Context<E>>,
    },
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Error {
    UnexpectedIterBlocksReplyInRunningMode,
    CommitInfo(komm::EchoError),
    CommitInserted(komm::EchoError),
    CommitRemoved(komm::EchoError),
    CommitFlushed(komm::EchoError),
    LookupRangeMergerVersklaven(arbeitssklave::Error),
    FlushButcherVersklaven(arbeitssklave::Error),
    MergeSearchTreesVersklaven(arbeitssklave::Error),
    DemolishSearchTreeVersklaven(arbeitssklave::Error),
    FlushButcherSklaveCrashed,
    MergeSearchTreesSklaveCrashed,
    DemolishSearchTreeSklaveCrashed,
    WheelIsGoneDuringReadBlock {
        route: WheelRouteReadBlock,
    },
    WheelIsGoneDuringWriteBlock {
        route: WheelRouteWriteBlock,
    },
    WheelIsGoneDuringDeleteBlock {
        route: WheelRouteDeleteBlock,
    },
    WheelIsGoneDuringFlush,
    WheelIsGoneDuringInfo {
        route: WheelRouteInfo,
    },
    WheelInfo {
        blockwheel_filename: wheels::WheelFilename,
        error: blockwheel_fs::Error,
    },
    WheelFlush {
        blockwheel_filename: wheels::WheelFilename,
        error: blockwheel_fs::Error,
    },
}

pub fn job<E, J>(
    mut welt_state: WeltState<E>,
    sklavenwelt: &mut Welt<E>,
    sendegeraet: &komm::Sendegeraet<Order<E>>,
    thread_pool: &edeltraud::Handle<J>,
)
    -> Result<Outcome<E>, Error>
where E: EchoPolicy,
      J: From<blockwheel_fs::job::SklaveJob<wheels::WheelEchoPolicy<E>>>,
      J: From<flush_butcher::SklaveJob<E>>,
      J: From<lookup_range_merge::SklaveJob<E>>,
      J: From<merge_search_trees::SklaveJob<E>>,
      J: From<demolish_search_tree::SklaveJob<E>>,
{
    loop {
        let mut performer_kont = match welt_state.kont {
            Kont::Start { performer, } =>
                performer.step(),
            Kont::StepPoll { next, } =>
                loop {
                    match sklavenwelt.env.incoming_orders.pop() {
                        None => {
                            welt_state.kont = Kont::StepPoll { next, };
                            return Ok(Outcome::Rasten {
                                running: welt_state,
                            });
                        },
                        Some(Order::Request(request)) =>
                            match welt_state.active_flush {
                                None =>
                                    match request {
                                        OrderRequest::Info(OrderRequestInfo { echo, }) =>
                                            break next.incoming_info(echo),
                                        OrderRequest::Insert(OrderRequestInsert { key, value, echo, }) =>
                                            break next.incoming_insert(key, value, echo),
                                        OrderRequest::LookupRange(komm::StreamStarten {
                                            inhalt: OrderRequestLookupRange {
                                                search_range,
                                                stream_echo,
                                            },
                                            stream_token,
                                        }) =>
                                            break next.begin_lookup_range(search_range, LookupRangeStream { stream_echo, stream_token, }),
                                        OrderRequest::Remove(OrderRequestRemove { key, echo, }) =>
                                            break next.incoming_remove(key, echo),
                                        OrderRequest::Flush(OrderRequestFlush { echo, }) => {
                                            welt_state.active_flush = Some(ActiveFlush::Kv);
                                            break next.incoming_flush(echo)
                                        },
                                    },
                                Some(ActiveFlush::Kv | ActiveFlush::Wheels { .. }) =>
                                    sklavenwelt.env.delayed_orders.push(Order::Request(request)),
                            },
                        Some(Order::LookupRangeCancel(komm::StreamAbbrechen {
                            stream_id,
                        })) => {
                            let maybe_meister = sklavenwelt.env
                                .lookup_range_merge_sklaven
                                .get(&stream_id);
                            match maybe_meister {
                                Some(meister) =>
                                    if let Err(error) = meister.befehl(lookup_range_merge::Order::Terminate, thread_pool) {
                                        log::warn!("lookup range merge sklave terminate order failed: {error:?}, unregistering");
                                        sklavenwelt.env.lookup_range_merge_sklaven.remove(&stream_id);
                                    },
                                None =>
                                    log::debug!("lookup range merge sklave entry has already unregistered before cancel"),
                            }
                        },
                        Some(Order::LookupRangeNext(komm::StreamMehr {
                            inhalt: OrderRequestLookupRangeNext {
                                stream_echo,
                            },
                            stream_token,
                        })) => {
                            let stream_id = stream_token.stream_id().clone();
                            let maybe_meister = sklavenwelt.env
                                .lookup_range_merge_sklaven
                                .get(&stream_id);
                            match maybe_meister {
                                Some(meister) => {
                                    let send_result = meister.befehl(
                                        lookup_range_merge::Order::ItemNext(
                                            lookup_range_merge::OrderItemNext {
                                                lookup_context: LookupRangeStream {
                                                    stream_echo,
                                                    stream_token,
                                                },
                                            },
                                        ),
                                        thread_pool,
                                    );
                                    if let Err(error) = send_result {
                                        log::warn!("lookup range merge sklave item next order failed: {error:?}, unregistering");
                                        sklavenwelt.env.lookup_range_merge_sklaven.remove(&stream_id);
                                    }
                                },
                                None =>
                                    log::debug!("lookup range merge sklave entry has already unregistered before item next order"),
                            }
                        },
                        Some(Order::UnregisterLookupRangeMerge(komm::UmschlagAbbrechen {
                            stamp: LookupRangeMergeDrop {
                                access_token,
                                route: LookupRangeRoute { stream_id, },
                            },
                        })) => {
                            let maybe_removed = sklavenwelt.env
                                .lookup_range_merge_sklaven
                                .remove(&stream_id);
                            if maybe_removed.is_none() {
                                log::debug!("lookup range merge sklave entry has already unregistered");
                            }
                            break next.commit_lookup_range(access_token);
                        },
                        Some(Order::UnregisterFlushButcher(komm::UmschlagAbbrechen {
                            stamp: FlushButcherDrop { .. },
                        })) =>
                            return Err(Error::FlushButcherSklaveCrashed),
                        Some(Order::UnregisterMergeSearchTrees(komm::UmschlagAbbrechen {
                            stamp: MergeSearchTreesDrop {
                                route: MergeSearchTreesRoute { meister_ref, },
                                ..
                            },
                        })) => {
                            let maybe_removed = sklavenwelt.env
                                .merge_search_trees_sklaven
                                .remove(meister_ref);
                            if maybe_removed.is_none() {
                                log::debug!("merge search trees sklave entry has already unregistered");
                            }
                            return Err(Error::MergeSearchTreesSklaveCrashed);
                        },
                        Some(Order::UnregisterDemolishSearchTree(komm::UmschlagAbbrechen {
                            stamp: DemolishSearchTreeDrop {
                                route: DemolishSearchTreeRoute { meister_ref, },
                                ..
                            },
                        })) => {
                            let maybe_removed = sklavenwelt.env
                                .demolish_search_tree_sklaven
                                .remove(meister_ref);
                            if maybe_removed.is_none() {
                                log::debug!("demolish search tree sklave entry has already unregistered");
                            }
                            return Err(Error::DemolishSearchTreeSklaveCrashed);
                        },
                        Some(Order::FlushButcherDone(komm::Umschlag {
                            inhalt: FlushButcherDone { root_block, },
                            stamp: FlushButcherDrop { search_tree_id, },
                        })) =>
                            break next.butcher_flushed(search_tree_id, root_block),
                        Some(Order::MergeSearchTreesDone(komm::Umschlag {
                            inhalt: MergeSearchTreesDone {
                                merged_search_tree_ref,
                                merged_search_tree_items_count,
                            },
                            stamp: MergeSearchTreesDrop {
                                access_token,
                                route: MergeSearchTreesRoute { meister_ref, },
                            },
                        })) => {
                            let maybe_removed = sklavenwelt.env
                                .merge_search_trees_sklaven
                                .remove(meister_ref);
                            if maybe_removed.is_none() {
                                log::warn!("merge search trees sklave entry has already unregistered");
                            }
                            break next.search_trees_merged(
                                merged_search_tree_ref,
                                merged_search_tree_items_count,
                                access_token,
                            );
                        },
                        Some(Order::DemolishSearchTreeDone(komm::Umschlag {
                            inhalt: DemolishSearchTreeDone,
                            stamp: DemolishSearchTreeDrop {
                                demolish_group_ref,
                                route: DemolishSearchTreeRoute { meister_ref, },
                            },
                        })) => {
                            let maybe_removed = sklavenwelt.env
                                .demolish_search_tree_sklaven
                                .remove(meister_ref);
                            if maybe_removed.is_none() {
                                log::warn!("demolish search trees sklave entry has already unregistered");
                            }
                            break next.demolished(demolish_group_ref);
                        },
                        Some(Order::Wheel(OrderWheel::InfoCancel(komm::UmschlagAbbrechen { stamp: route, }))) =>
                            return Err(Error::WheelIsGoneDuringInfo { route, }),
                        Some(Order::Wheel(OrderWheel::Info(komm::Umschlag {
                            inhalt: info,
                            stamp: WheelRouteInfo {
                                route: InfoRoute { info_ref, },
                                blockwheel_filename,
                            },
                        }))) => {
                            let maybe_info = sklavenwelt.env
                                .pending_info_requests
                                .get_mut(info_ref);
                            match maybe_info {
                                Some(pending_info) => {
                                    pending_info.info.wheels.push(WheelInfo { info, blockwheel_filename, });
                                    pending_info.wheels_left -= 1;
                                    if pending_info.wheels_left == 0 {
                                        let PendingInfo { info, echo, .. } = sklavenwelt.env
                                            .pending_info_requests
                                            .remove(info_ref)
                                            .unwrap();
                                        echo.commit_echo(info)
                                            .map_err(Error::CommitInfo)?;
                                    }
                                },
                                None =>
                                    log::warn!("pending info entry has already removed"),
                            }
                        },
                        Some(Order::Wheel(OrderWheel::FlushCancel(komm::UmschlagAbbrechen { stamp: WheelRouteFlush, }))) =>
                            return Err(Error::WheelIsGoneDuringFlush),
                        Some(Order::Wheel(OrderWheel::Flush(komm::Umschlag { inhalt: blockwheel_fs::Flushed, stamp: WheelRouteFlush, }))) =>
                            match welt_state.active_flush {
                                Some(ActiveFlush::Wheels { wheels_left, echo, }) if wheels_left > 1 => {
                                    log::debug!("wheel flush is done, {} wheels is left", wheels_left - 1);
                                    welt_state.active_flush = Some(ActiveFlush::Wheels {
                                        wheels_left: wheels_left - 1,
                                        echo,
                                    });
                                },
                                Some(ActiveFlush::Wheels { echo, .. }) => {
                                    log::debug!("wheels flush is done, reporting");
                                    echo.commit_echo(Flushed)
                                        .map_err(Error::CommitFlushed)?;
                                    welt_state.active_flush = None;
                                    sklavenwelt.env.incoming_orders.append(
                                        &mut sklavenwelt.env.delayed_orders,
                                    );

                                    assert!(
                                        sklavenwelt.env.lookup_range_merge_sklaven.is_empty(),
                                        "expected zero `lookup_range_merge_sklaven` on flush but got #{}",
                                        sklavenwelt.env.lookup_range_merge_sklaven.len(),
                                    );
                                    assert!(sklavenwelt.env.merge_search_trees_sklaven.is_empty());
                                    assert!(sklavenwelt.env.demolish_search_tree_sklaven.is_empty());
                                },
                                None | Some(ActiveFlush::Kv) =>
                                    unreachable!(),
                            },
                        Some(Order::Wheel(OrderWheel::WriteBlockCancel(komm::UmschlagAbbrechen { stamp, }))) =>
                            return Err(Error::WheelIsGoneDuringWriteBlock { route: stamp, }),
                        Some(Order::Wheel(OrderWheel::WriteBlock(komm::Umschlag {
                            inhalt: write_block_result,
                            stamp: WheelRouteWriteBlock::MergeSearchTrees {
                                route: MergeSearchTreesRoute { meister_ref, },
                                target,
                            },
                        }))) => {
                            let maybe_meister = sklavenwelt.env
                                .merge_search_trees_sklaven
                                .get(meister_ref);
                            match maybe_meister {
                                Some(meister) => {
                                    let send_result = meister.befehl(
                                        merge_search_trees::Order::WriteBlock(
                                            merge_search_trees::OrderWriteBlock {
                                                write_block_result,
                                                target,
                                            },
                                        ),
                                        thread_pool,
                                    );
                                    if let Err(error) = send_result {
                                        log::warn!("merge search trees sklave write block order failed: {error:?}, unregistering");
                                        sklavenwelt.env.merge_search_trees_sklaven.remove(meister_ref);
                                    }
                                },
                                None =>
                                    log::warn!("merge search trees sklave entry has already unregistered before write block order"),
                            }
                        },
                        Some(Order::Wheel(OrderWheel::ReadBlockCancel(komm::UmschlagAbbrechen { stamp, }))) =>
                            return Err(Error::WheelIsGoneDuringReadBlock { route: stamp, }),
                        Some(Order::Wheel(OrderWheel::ReadBlock(komm::Umschlag {
                            inhalt: read_block_result,
                            stamp: WheelRouteReadBlock::LookupRangeMerge {
                                route: LookupRangeRoute { stream_id, },
                                target,
                            },
                        }))) => {
                            let maybe_meister = sklavenwelt.env
                                .lookup_range_merge_sklaven
                                .get(&stream_id);
                            match maybe_meister {
                                Some(meister) => {
                                    let send_result = meister.befehl(
                                        lookup_range_merge::Order::ReadBlock(
                                            lookup_range_merge::OrderReadBlock {
                                                read_block_result,
                                                target,
                                            },
                                        ),
                                        thread_pool,
                                    );
                                    if let Err(error) = send_result {
                                        log::warn!("lookup range merge sklave read block order failed: {error:?}, unregistering");
                                        sklavenwelt.env.lookup_range_merge_sklaven.remove(&stream_id);
                                    }
                                },
                                None =>
                                    log::debug!("lookup range merge sklave entry has already unregistered before read block order"),
                            }
                        },
                        Some(Order::Wheel(OrderWheel::ReadBlock(komm::Umschlag {
                            inhalt: read_block_result,
                            stamp: WheelRouteReadBlock::MergeSearchTrees {
                                route: MergeSearchTreesRoute { meister_ref, },
                                target,
                            },
                        }))) => {
                            let maybe_meister = sklavenwelt.env
                                .merge_search_trees_sklaven
                                .get(meister_ref);
                            match maybe_meister {
                                Some(meister) => {
                                    let send_result = meister.befehl(
                                        merge_search_trees::Order::ReadBlock(
                                            merge_search_trees::OrderReadBlock {
                                                read_block_result,
                                                target,
                                            },
                                        ),
                                        thread_pool,
                                    );
                                    if let Err(error) = send_result {
                                        log::warn!("merge search trees sklave read block order failed: {error:?}, unregistering");
                                        sklavenwelt.env.merge_search_trees_sklaven.remove(meister_ref);
                                    }
                                },
                                None =>
                                    log::warn!("merge search trees sklave entry has already unregistered before read block order"),
                            }
                        },
                        Some(Order::Wheel(OrderWheel::ReadBlock(komm::Umschlag {
                            inhalt: read_block_result,
                            stamp: WheelRouteReadBlock::DemolishSearchTree {
                                route: DemolishSearchTreeRoute { meister_ref, },
                                target,
                            },
                        }))) => {
                            let maybe_meister = sklavenwelt.env
                                .demolish_search_tree_sklaven
                                .get(meister_ref);
                            match maybe_meister {
                                Some(meister) => {
                                    let send_result = meister.befehl(
                                        demolish_search_tree::Order::ReadBlock(
                                            demolish_search_tree::OrderReadBlock {
                                                read_block_result,
                                                target,
                                            },
                                        ),
                                        thread_pool,
                                    );
                                    if let Err(error) = send_result {
                                        log::warn!("demolish search tree sklave read block order failed: {error:?}, unregistering");
                                        sklavenwelt.env.demolish_search_tree_sklaven.remove(meister_ref);
                                    }
                                },
                                None =>
                                    log::warn!("demolish search tree sklave entry has already unregistered before read block order"),
                            }
                        },
                        Some(Order::Wheel(OrderWheel::DeleteBlockCancel(komm::UmschlagAbbrechen { stamp, }))) =>
                            return Err(Error::WheelIsGoneDuringDeleteBlock { route: stamp, }),
                        Some(Order::Wheel(OrderWheel::DeleteBlock(komm::Umschlag {
                            inhalt: delete_block_result,
                            stamp: WheelRouteDeleteBlock::DemolishSearchTree {
                                route: DemolishSearchTreeRoute { meister_ref, },
                            },
                        }))) => {
                            let maybe_meister = sklavenwelt.env
                                .demolish_search_tree_sklaven
                                .get(meister_ref);
                            match maybe_meister {
                                Some(meister) => {
                                    let send_result = meister.befehl(
                                        demolish_search_tree::Order::DeleteBlock(
                                            demolish_search_tree::OrderDeleteBlock {
                                                delete_block_result,
                                            },
                                        ),
                                        thread_pool,
                                    );
                                    if let Err(error) = send_result {
                                        log::warn!("demolish search tree sklave delete block order failed: {error:?}, unregistering");
                                        sklavenwelt.env.demolish_search_tree_sklaven.remove(meister_ref);
                                    }
                                },
                                None =>
                                    log::warn!("demolish search tree sklave entry has already unregistered before delete block order"),
                            }
                        },
                        Some(Order::Wheel(OrderWheel::IterBlocksInitCancel(komm::UmschlagAbbrechen { .. }))) |
                        Some(Order::Wheel(OrderWheel::IterBlocksInit(komm::Umschlag { .. }))) |
                        Some(Order::Wheel(OrderWheel::IterBlocksNextCancel(komm::UmschlagAbbrechen { .. }))) |
                        Some(Order::Wheel(OrderWheel::IterBlocksNext(komm::Umschlag { .. }))) =>
                            return Err(Error::UnexpectedIterBlocksReplyInRunningMode),
                    }
                },
        };

        loop {
            performer_kont = match performer_kont {
                performer::Kont::Poll(performer::KontPoll { next, }) => {
                    welt_state.kont = Kont::StepPoll { next, };
                    break;
                },
                performer::Kont::InfoReady(performer::KontInfoReady {
                    info,
                    info_context: echo,
                    next,
                }) => {
                    let info_ref = sklavenwelt.env
                        .pending_info_requests
                        .insert(PendingInfo { info, echo, wheels_left: 0, });
                    for wheel_ref in sklavenwelt.env.wheels.iter() {
                        let info_rueckkopplung = sendegeraet
                            .rueckkopplung(WheelRouteInfo {
                                route: InfoRoute { info_ref, },
                                blockwheel_filename: wheel_ref.blockwheel_filename.clone(),
                            });
                        wheel_ref.meister
                            .info(info_rueckkopplung, thread_pool)
                            .map_err(|error| Error::WheelInfo {
                                blockwheel_filename: wheel_ref.blockwheel_filename.clone(),
                                error,
                            })?;
                        sklavenwelt.env.pending_info_requests
                            .get_mut(info_ref)
                            .unwrap()
                            .wheels_left += 1;
                    }
                    next.got_it()
                },
                performer::Kont::Inserted(performer::KontInserted {
                    version,
                    insert_context: echo,
                    next,
                }) => {
                    echo.commit_echo(Inserted { version, })
                        .map_err(Error::CommitInserted)?;
                    next.got_it()
                },
                performer::Kont::Removed(performer::KontRemoved {
                    version,
                    remove_context: echo,
                    next,
                }) => {
                    echo.commit_echo(Removed { version, })
                        .map_err(Error::CommitRemoved)?;
                    next.got_it()
                },
                performer::Kont::Flushed(performer::KontFlushed {
                    flush_context: echo,
                    next,
                }) => {
                    log::debug!(
                        "kv flush is done (lookup_range_merge_sklaven.len() = {}), performing flush for wheels",
                        sklavenwelt.env.lookup_range_merge_sklaven.len(),
                    );
                    let mut wheels_left = 0;
                    for wheel_ref in sklavenwelt.env.wheels.iter() {
                        let flush_rueckkopplung = sendegeraet
                            .rueckkopplung(WheelRouteFlush);
                        wheel_ref.meister
                            .flush(flush_rueckkopplung, thread_pool)
                            .map_err(|error| Error::WheelFlush {
                                blockwheel_filename: wheel_ref.blockwheel_filename.clone(),
                                error,
                            })?;
                        wheels_left += 1;
                        log::debug!("initiating flush for {:?}", wheel_ref.blockwheel_filename);
                    }

                    welt_state.active_flush = match welt_state.active_flush {
                        Some(ActiveFlush::Kv) =>
                            Some(ActiveFlush::Wheels { wheels_left, echo, }),
                        None | Some(ActiveFlush::Wheels { .. }) =>
                            unreachable!(),
                    };
                    next.commit_flush()
                },
                performer::Kont::FlushButcher(performer::KontFlushButcher {
                    search_tree_id,
                    frozen_memcache,
                    next,
                }) => {
                    let flush_butcher_meister = arbeitssklave::Freie::new()
                        .versklaven(
                            flush_butcher::Welt::new(
                                frozen_memcache,
                                sklavenwelt.env.params.tree_block_size,
                                sklavenwelt.env.params
                                    .search_tree_values_inline_size_limit,
                                sklavenwelt.env.wheels.clone(),
                                sklavenwelt.env.blocks_pool.clone(),
                                welt_state.pools.block_entries_pool.clone(),
                                sendegeraet.rueckkopplung(FlushButcherDrop { search_tree_id, }),
                            ),
                            thread_pool,
                        )
                        .map_err(Error::FlushButcherVersklaven)?;
                    next.scheduled()
                },
                performer::Kont::LookupRangeMergerReady(performer::KontLookupRangeMergerReady {
                    ranges_merger,
                    lookup_context,
                    next,
                }) => {
                    let stream_id = lookup_context
                        .stream_token
                        .stream_id()
                        .clone();
                    let merger_meister = arbeitssklave::Freie::new()
                        .versklaven(
                            lookup_range_merge::Welt::new(
                                ranges_merger.source,
                                lookup_context,
                                stream_id.clone(),
                                sendegeraet.clone(),
                                sklavenwelt.env.wheels.clone(),
                                sendegeraet
                                    .rueckkopplung(LookupRangeMergeDrop {
                                        access_token: ranges_merger.token,
                                        route: LookupRangeRoute { stream_id: stream_id.clone(), },
                                    }),
                            ),
                            thread_pool,
                        )
                        .map_err(Error::LookupRangeMergerVersklaven)?;
                    sklavenwelt.env
                        .lookup_range_merge_sklaven
                        .insert(stream_id, merger_meister);
                    next.got_it()
                },
                performer::Kont::MergeSearchTrees(performer::KontMergeSearchTrees {
                    ranges_merger,
                    next,
                }) => {
                    let insert_entry = sklavenwelt.env
                        .merge_search_trees_sklaven
                        .insert_entry();
                    let meister_ref = *insert_entry.set_ref();
                    let merge_meister = arbeitssklave::Freie::new()
                        .versklaven(
                            merge_search_trees::Welt::new(
                                ranges_merger.source_count_items,
                                ranges_merger.source_build,
                                meister_ref,
                                sendegeraet.clone(),
                                sklavenwelt.env.wheels.clone(),
                                sklavenwelt.env.blocks_pool.clone(),
                                welt_state.pools.block_entries_pool.clone(),
                                sklavenwelt.env.params.tree_block_size,
                                sendegeraet
                                    .rueckkopplung(MergeSearchTreesDrop {
                                        access_token: ranges_merger.token,
                                        route: MergeSearchTreesRoute { meister_ref, },
                                    }),
                            ),
                            thread_pool,
                        )
                        .map_err(Error::MergeSearchTreesVersklaven)?;
                    insert_entry.commit(merge_meister);
                    next.scheduled()
                },
                performer::Kont::DemolishSearchTree(performer::KontDemolishSearchTree {
                    order,
                    next,
                }) => {
                    let insert_entry = sklavenwelt.env
                        .demolish_search_tree_sklaven
                        .insert_entry();
                    let meister_ref = *insert_entry.set_ref();
                    let demolish_meister = arbeitssklave::Freie::new()
                        .versklaven(
                            demolish_search_tree::Welt::new(
                                order.source,
                                meister_ref,
                                sendegeraet.clone(),
                                sklavenwelt.env.wheels.clone(),
                                sendegeraet
                                     .rueckkopplung(DemolishSearchTreeDrop {
                                        demolish_group_ref: order.demolish_group_ref,
                                        route: DemolishSearchTreeRoute { meister_ref, },
                                    }),
                            ),
                            thread_pool,
                        )
                        .map_err(Error::DemolishSearchTreeVersklaven)?;
                    insert_entry.commit(demolish_meister);
                    next.roger_that()
                },
            };
        }
    }
}
