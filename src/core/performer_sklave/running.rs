use arbeitssklave::{
    komm,
};

use crate::{
    job,
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
            OrderRequestRemove,
            OrderRequestFlush,
            InfoRoute,
            LookupRangeRoute,
            LookupRangeMergeDrop,
            LookupRangeStreamNext,
            FlushButcherDone,
            FlushButcherDrop,
            FlushButcherRoute,
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
    AccessPolicy,
};

pub mod flush_butcher;
pub mod lookup_range_merge;
pub mod merge_search_trees;
pub mod demolish_search_tree;

pub struct WeltState<A> where A: AccessPolicy {
    kont: Kont<A>,
    pools: Pools,
    active_flush: Option<ActiveFlush<A>>,
}

impl<A> WeltState<A> where A: AccessPolicy {
    pub fn new(performer: performer::Performer<Context<A>>, pools: Pools) -> Self {
        WeltState {
            kont: Kont::Start { performer, },
            pools,
            active_flush: None,
        }
    }
}

enum ActiveFlush<A> where A: AccessPolicy {
    Kv,
    Wheels {
        wheels_left: usize,
        rueckkopplung: komm::Rueckkopplung<A::Order, A::Flush>,
    },
}

pub struct PendingInfo<A> where A: AccessPolicy {
    info: Info,
    rueckkopplung: komm::Rueckkopplung<A::Order, A::Info>,
    wheels_left: usize,
}

pub enum Outcome<A> where A: AccessPolicy {
    Rasten { running: WeltState<A>, },
}

enum Kont<A> where A: AccessPolicy {
    Start {
        performer: performer::Performer<Context<A>>,
    },
    StepPoll {
        next: performer::KontPollNext<Context<A>>,
    },
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Error {
    UnexpectedIterBlocksReplyInRunningMode,
    CommitInfo(komm::Error),
    CommitInserted(komm::Error),
    CommitRemoved(komm::Error),
    CommitFlushed(komm::Error),
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
        error: arbeitssklave::Error,
    },
    WheelFlush {
        blockwheel_filename: wheels::WheelFilename,
        error: arbeitssklave::Error,
    },
}

pub fn job<A, P>(
    mut welt_state: WeltState<A>,
    sklavenwelt: &mut Welt<A>,
    thread_pool: &P,
)
    -> Result<Outcome<A>, Error>
where A: AccessPolicy,
      P: edeltraud::ThreadPool<job::Job<A>>,
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
                                        OrderRequest::Info(OrderRequestInfo { rueckkopplung, }) =>
                                            break next.incoming_info(rueckkopplung),
                                        OrderRequest::Insert(OrderRequestInsert { key, value, rueckkopplung, }) =>
                                            break next.incoming_insert(key, value, rueckkopplung),
                                        OrderRequest::LookupRange(OrderRequestLookupRange { search_range, rueckkopplung, }) =>
                                            break next.begin_lookup_range(search_range, rueckkopplung),
                                        OrderRequest::Remove(OrderRequestRemove { key, rueckkopplung, }) =>
                                            break next.incoming_remove(key, rueckkopplung),
                                        OrderRequest::Flush(OrderRequestFlush { rueckkopplung, }) => {
                                            welt_state.active_flush = Some(ActiveFlush::Kv);
                                            break next.incoming_flush(rueckkopplung)
                                        },
                                    },
                                Some(ActiveFlush::Kv | ActiveFlush::Wheels { .. }) =>
                                    sklavenwelt.env.delayed_orders.push(Order::Request(request)),
                            },
                        Some(Order::LookupRangeStreamCancel(komm::UmschlagAbbrechen {
                            stamp: LookupRangeRoute { meister_ref, },
                        })) => {
                            log::debug!(" ;; lookup range merge sklave cancel initiated for: {meister_ref:?}");
                            let maybe_meister = sklavenwelt.env
                                .lookup_range_merge_sklaven
                                .get(meister_ref);
                            match maybe_meister {
                                Some(meister) =>
                                    if let Err(error) = meister.befehl(lookup_range_merge::Order::Terminate, thread_pool) {
                                        log::warn!("lookup range merge sklave terminate order failed: {error:?}, unregistering");
                                        sklavenwelt.env.lookup_range_merge_sklaven.remove(meister_ref);
                                    },
                                None =>
                                    log::debug!("lookup range merge sklave entry has already unregistered before cancel"),
                            }
                        },
                        Some(Order::LookupRangeStreamNext(komm::Umschlag {
                            inhalt: LookupRangeStreamNext { rueckkopplung, },
                            stamp: LookupRangeRoute { meister_ref, },
                        })) => {
                            let maybe_meister = sklavenwelt.env
                                .lookup_range_merge_sklaven
                                .get(meister_ref);
                            match maybe_meister {
                                Some(meister) => {
                                    let send_result = meister.befehl(
                                        lookup_range_merge::Order::ItemNext(
                                            lookup_range_merge::OrderItemNext {
                                                lookup_context: rueckkopplung,
                                            },
                                        ),
                                        thread_pool,
                                    );
                                    if let Err(error) = send_result {
                                        log::warn!("lookup range merge sklave item next order failed: {error:?}, unregistering");
                                        sklavenwelt.env.lookup_range_merge_sklaven.remove(meister_ref);
                                    }
                                },
                                None =>
                                    log::debug!("lookup range merge sklave entry has already unregistered before item next order"),
                            }
                        },
                        Some(Order::UnregisterLookupRangeMerge(komm::UmschlagAbbrechen {
                            stamp: LookupRangeMergeDrop {
                                access_token,
                                route: LookupRangeRoute { meister_ref, },
                            },
                        })) => {
                            log::debug!(" ;; unregistering lookup range merge sklave: {meister_ref:?}");
                            let maybe_removed = sklavenwelt.env
                                .lookup_range_merge_sklaven
                                .remove(meister_ref);
                            if maybe_removed.is_none() {
                                log::debug!("lookup range merge sklave entry has already unregistered");
                            }
                            break next.commit_lookup_range(access_token);
                        },
                        Some(Order::UnregisterFlushButcher(komm::UmschlagAbbrechen {
                            stamp: FlushButcherDrop {
                                route: FlushButcherRoute { meister_ref, },
                                ..
                            },
                        })) => {
                            log::debug!(" ;; unregistering flush butcher sklave: {meister_ref:?}");
                            let maybe_removed = sklavenwelt.env
                                .flush_butcher_sklaven
                                .remove(meister_ref);
                            if maybe_removed.is_none() {
                                log::debug!("flush butcher sklave entry has already unregistered");
                            }
                            return Err(Error::FlushButcherSklaveCrashed);
                        },
                        Some(Order::UnregisterMergeSearchTrees(komm::UmschlagAbbrechen {
                            stamp: MergeSearchTreesDrop {
                                route: MergeSearchTreesRoute { meister_ref, },
                                ..
                            },
                        })) => {
                            log::debug!(" ;; unregistering merge search trees sklave: {meister_ref:?}");
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
                            log::debug!(" ;; unregistering demolish search tree sklave: {meister_ref:?}");
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
                            stamp: FlushButcherDrop {
                                search_tree_id,
                                route: FlushButcherRoute { meister_ref, },
                            },
                        })) => {
                            log::debug!(" ;; flush butcher process {meister_ref:?} done, root block = {root_block:?}");
                            let maybe_removed = sklavenwelt.env
                                .flush_butcher_sklaven
                                .remove(meister_ref);
                            if maybe_removed.is_none() {
                                log::warn!("flush butcher sklave entry has already unregistered");
                            }
                            break next.butcher_flushed(search_tree_id, root_block);
                        },
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
                            log::debug!(
                                " ;; merge search trees process {meister_ref:?} done, tree ref: {:?}, items_count: {:?}",
                                merged_search_tree_ref,
                                merged_search_tree_items_count,
                            );
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
                            log::debug!(" ;; demolish search tree process {meister_ref:?} done");
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
                                        let PendingInfo { info, rueckkopplung, .. } = sklavenwelt.env
                                            .pending_info_requests
                                            .remove(info_ref)
                                            .unwrap();
                                        rueckkopplung.commit(info)
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
                                Some(ActiveFlush::Wheels { wheels_left, rueckkopplung, }) if wheels_left > 1 => {
                                    log::debug!("wheel flush is done, {} wheels is left", wheels_left - 1);
                                    welt_state.active_flush = Some(ActiveFlush::Wheels {
                                        wheels_left: wheels_left - 1,
                                        rueckkopplung,
                                    });
                                },
                                Some(ActiveFlush::Wheels { rueckkopplung, .. }) => {
                                    log::debug!("wheels flush is done, reporting");
                                    rueckkopplung.commit(Flushed)
                                        .map_err(Error::CommitFlushed)?;
                                    welt_state.active_flush = None;
                                    sklavenwelt.env.incoming_orders.append(
                                        &mut sklavenwelt.env.delayed_orders,
                                    );

                                    assert!(sklavenwelt.env.lookup_range_merge_sklaven.is_empty());
                                    assert!(sklavenwelt.env.flush_butcher_sklaven.is_empty());
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
                            stamp: WheelRouteWriteBlock::FlushButcher {
                                route: FlushButcherRoute { meister_ref, },
                                target,
                            },
                        }))) => {
                            let maybe_meister = sklavenwelt.env
                                .flush_butcher_sklaven
                                .get(meister_ref);
                            match maybe_meister {
                                Some(meister) => {
                                    let send_result = meister.befehl(
                                        flush_butcher::Order::WriteBlock(
                                            flush_butcher::OrderWriteBlock {
                                                write_block_result,
                                                target,
                                            },
                                        ),
                                        thread_pool,
                                    );
                                    if let Err(error) = send_result {
                                        log::warn!("flush butcher sklave write block order failed: {error:?}, unregistering");
                                        sklavenwelt.env.flush_butcher_sklaven.remove(meister_ref);
                                    }
                                },
                                None =>
                                    log::warn!("flush butcher sklave entry has already unregistered before write block order"),
                            }
                        },
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
                                route: LookupRangeRoute { meister_ref, },
                                target,
                            },
                        }))) => {
                            let maybe_meister = sklavenwelt.env
                                .lookup_range_merge_sklaven
                                .get(meister_ref);
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
                                        sklavenwelt.env.lookup_range_merge_sklaven.remove(meister_ref);
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
                performer::Kont::InfoReady(performer::KontInfoReady { info, info_context: rueckkopplung, next, }) => {
                    let info_ref = sklavenwelt.env
                        .pending_info_requests
                        .insert(PendingInfo { info, rueckkopplung, wheels_left: 0, });
                    for wheel_ref in sklavenwelt.env.wheels.iter() {
                        let info_rueckkopplung = sklavenwelt.env
                            .sendegeraet
                            .rueckkopplung(WheelRouteInfo {
                                route: InfoRoute { info_ref, },
                                blockwheel_filename: wheel_ref.blockwheel_filename.clone(),
                            });
                        wheel_ref.meister
                            .info(info_rueckkopplung, &edeltraud::ThreadPoolMap::new(thread_pool))
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
                performer::Kont::Inserted(performer::KontInserted { version, insert_context: rueckkopplung, next, }) => {
                    rueckkopplung.commit(Inserted { version, })
                        .map_err(Error::CommitInserted)?;
                    next.got_it()
                },
                performer::Kont::Removed(performer::KontRemoved { version, remove_context: rueckkopplung, next, }) => {
                    rueckkopplung.commit(Removed { version, })
                        .map_err(Error::CommitRemoved)?;
                    next.got_it()
                },
                performer::Kont::Flushed(performer::KontFlushed { flush_context: rueckkopplung, next, }) => {
                    log::debug!("kv flush is done, performing flush for wheels");
                    let mut wheels_left = 0;
                    for wheel_ref in sklavenwelt.env.wheels.iter() {
                        let flush_rueckkopplung = sklavenwelt.env
                            .sendegeraet
                            .rueckkopplung(WheelRouteFlush);
                        wheel_ref.meister
                            .flush(flush_rueckkopplung, &edeltraud::ThreadPoolMap::new(thread_pool))
                            .map_err(|error| Error::WheelFlush {
                                blockwheel_filename: wheel_ref.blockwheel_filename.clone(),
                                error,
                            })?;
                        wheels_left += 1;
                        log::debug!("initiating flush for {:?}", wheel_ref.blockwheel_filename);
                    }

                    welt_state.active_flush = match welt_state.active_flush {
                        Some(ActiveFlush::Kv) =>
                            Some(ActiveFlush::Wheels { wheels_left, rueckkopplung, }),
                        None | Some(ActiveFlush::Wheels { .. }) =>
                            unreachable!(),
                    };
                    next.commit_flush()
                },
                performer::Kont::FlushButcher(performer::KontFlushButcher { search_tree_id, frozen_memcache, next, }) => {
                    let flush_butcher_freie = arbeitssklave::Freie::new();
                    let flush_butcher_meister = flush_butcher_freie.meister();
                    let meister_ref = sklavenwelt.env
                        .flush_butcher_sklaven
                        .insert(flush_butcher_meister);
                    flush_butcher_freie
                        .versklaven(
                            flush_butcher::Welt::new(
                                frozen_memcache,
                                sklavenwelt.env.params.tree_block_size,
                                sklavenwelt.env.params
                                    .search_tree_values_inline_size_limit,
                                meister_ref,
                                sklavenwelt.env.sendegeraet.clone(),
                                sklavenwelt.env.wheels.clone(),
                                sklavenwelt.env.blocks_pool.clone(),
                                welt_state.pools.block_entries_pool.clone(),
                                sklavenwelt.env
                                    .sendegeraet
                                    .rueckkopplung(FlushButcherDrop {
                                        search_tree_id,
                                        route: FlushButcherRoute {
                                            meister_ref,
                                        },
                                    }),
                            ),
                            thread_pool,
                        )
                        .map_err(Error::FlushButcherVersklaven)?;
                    next.scheduled()
                },
                performer::Kont::LookupRangeMergerReady(performer::KontLookupRangeMergerReady { ranges_merger, lookup_context, next, }) => {
                    let merger_freie = arbeitssklave::Freie::new();
                    let merger_meister = merger_freie.meister();
                    let meister_ref = sklavenwelt.env
                        .lookup_range_merge_sklaven
                        .insert(merger_meister);
                    merger_freie
                        .versklaven(
                            lookup_range_merge::Welt::new(
                                ranges_merger.source,
                                lookup_context,
                                meister_ref,
                                sklavenwelt.env.sendegeraet.clone(),
                                sklavenwelt.env.wheels.clone(),
                                sklavenwelt.env
                                    .sendegeraet
                                    .rueckkopplung(LookupRangeMergeDrop {
                                        access_token: ranges_merger.token,
                                        route: LookupRangeRoute { meister_ref, },
                                    }),
                            ),
                            thread_pool,
                        )
                        .map_err(Error::LookupRangeMergerVersklaven)?;
                    next.got_it()
                },
                performer::Kont::MergeSearchTrees(performer::KontMergeSearchTrees { ranges_merger, next, }) => {
                    let merge_freie = arbeitssklave::Freie::new();
                    let merge_meister = merge_freie.meister();
                    let meister_ref = sklavenwelt.env
                        .merge_search_trees_sklaven
                        .insert(merge_meister);
                    merge_freie
                        .versklaven(
                            merge_search_trees::Welt::new(
                                ranges_merger.source_count_items,
                                ranges_merger.source_build,
                                meister_ref,
                                sklavenwelt.env.sendegeraet.clone(),
                                sklavenwelt.env.wheels.clone(),
                                sklavenwelt.env.blocks_pool.clone(),
                                welt_state.pools.block_entries_pool.clone(),
                                sklavenwelt.env.params.tree_block_size,
                                sklavenwelt.env
                                    .sendegeraet
                                    .rueckkopplung(MergeSearchTreesDrop {
                                        access_token: ranges_merger.token,
                                        route: MergeSearchTreesRoute { meister_ref, },
                                    }),
                            ),
                            thread_pool,
                        )
                        .map_err(Error::MergeSearchTreesVersklaven)?;
                    next.scheduled()
                },
                performer::Kont::DemolishSearchTree(performer::KontDemolishSearchTree { order, next, }) => {
                    let demolish_freie = arbeitssklave::Freie::new();
                    let demolish_meister = demolish_freie.meister();
                    let meister_ref = sklavenwelt.env
                        .demolish_search_tree_sklaven
                        .insert(demolish_meister);
                    demolish_freie
                        .versklaven(
                            demolish_search_tree::Welt::new(
                                order.source,
                                meister_ref,
                                sklavenwelt.env.sendegeraet.clone(),
                                sklavenwelt.env.wheels.clone(),
                                sklavenwelt.env
                                    .sendegeraet
                                    .rueckkopplung(DemolishSearchTreeDrop {
                                        demolish_group_ref: order.demolish_group_ref,
                                        route: DemolishSearchTreeRoute { meister_ref, },
                                    }),
                            ),
                            thread_pool,
                        )
                        .map_err(Error::DemolishSearchTreeVersklaven)?;
                    next.roger_that()
                },
            };
        }
    }
}