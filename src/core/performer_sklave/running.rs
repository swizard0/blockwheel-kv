use arbeitssklave::{
    komm,
};

use crate::{
    job,
    core::{
        performer,
        performer_sklave::{
            Welt,
            Order,
            Context,
            OrderWheel,
            OrderRequest,
            OrderRequestInfo,
            OrderRequestInsert,
            OrderRequestLookupRange,
            OrderRequestRemove,
            OrderRequestFlush,
            LookupRangeRoute,
            LookupRangeMergeDrop,
            LookupRangeStreamNext,
            WheelRouteReadBlock,
        },
    },
    Flushed,
    Removed,
    Inserted,
    AccessPolicy,
};

pub mod lookup_range_merge;

pub struct WeltState<A> where A: AccessPolicy {
    kont: Kont<A>,
}

impl<A> WeltState<A> where A: AccessPolicy {
    pub fn new(performer: performer::Performer<Context<A>>) -> Self {
        WeltState {
            kont: Kont::Start { performer, },
        }
    }
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
pub enum Error {
    UnexpectedIterBlocksReplyInRunningMode,
    CommitInfo(komm::Error),
    CommitInserted(komm::Error),
    CommitRemoved(komm::Error),
    CommitFlushed(komm::Error),
    LookupRangeMergerVersklaven(arbeitssklave::Error),
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
                        Some(Order::Request(OrderRequest::Info(OrderRequestInfo { rueckkopplung, }))) =>
                            break next.incoming_info(rueckkopplung),
                        Some(Order::Request(OrderRequest::Insert(OrderRequestInsert { key, value, rueckkopplung, }))) =>
                            break next.incoming_insert(key, value, rueckkopplung),
                        Some(Order::Request(OrderRequest::LookupRange(OrderRequestLookupRange { search_range, rueckkopplung, }))) =>
                            break next.begin_lookup_range(search_range, rueckkopplung),
                        Some(Order::Request(OrderRequest::Remove(OrderRequestRemove { key, rueckkopplung, }))) =>
                            break next.incoming_remove(key, rueckkopplung),
                        Some(Order::Request(OrderRequest::Flush(OrderRequestFlush { rueckkopplung, }))) =>
                            break next.incoming_flush(rueckkopplung),
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
                            payload: LookupRangeStreamNext { rueckkopplung, },
                            stamp: lookup_range_router,
                        })) => {
                            todo!();
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
                            if let None = maybe_removed {
                                log::debug!("lookup range merge sklave entry has already unregistered");
                            }
                            break next.commit_lookup_range(access_token);
                        },
                        Some(Order::Wheel(OrderWheel::InfoCancel(komm::UmschlagAbbrechen { stamp: wheel_route_info, }))) => {
                            todo!();
                        },
                        Some(Order::Wheel(OrderWheel::Info(komm::Umschlag { payload: info, stamp: wheel_route_info, }))) => {
                            todo!();
                        },
                        Some(Order::Wheel(OrderWheel::FlushCancel(komm::UmschlagAbbrechen { stamp: wheel_route_flush, }))) => {
                            todo!();
                        },
                        Some(Order::Wheel(OrderWheel::Flush(komm::Umschlag { payload: blockwheel_fs::Flushed, stamp: wheel_route_flush, }))) => {
                            todo!();
                        },
                        Some(Order::Wheel(OrderWheel::WriteBlockCancel(komm::UmschlagAbbrechen { stamp: wheel_route_write_block, }))) => {
                            todo!();
                        },
                        Some(Order::Wheel(OrderWheel::WriteBlock(komm::Umschlag {
                            payload: write_block_result,
                            stamp: wheel_route_write_block,
                        }))) => {
                            todo!();
                        },
                        Some(Order::Wheel(OrderWheel::ReadBlockCancel(komm::UmschlagAbbrechen { stamp: wheel_route_read_block, }))) => {
                            todo!();
                        },
                        Some(Order::Wheel(OrderWheel::ReadBlock(komm::Umschlag {
                            payload: read_block_result,
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
                                    log::debug!("lookup range merge sklave entry has already unregistered before cancel"),
                            }
                        },
                        Some(Order::Wheel(OrderWheel::DeleteBlockCancel(komm::UmschlagAbbrechen { stamp: wheel_route_delete_block, }))) => {
                            todo!();
                        },
                        Some(Order::Wheel(OrderWheel::DeleteBlock(komm::Umschlag {
                            payload: delete_block_result,
                            stamp: wheel_route_delete_block,
                        }))) => {
                            todo!();
                        },
                        Some(Order::Wheel(OrderWheel::IterBlocksInitCancel(komm::UmschlagAbbrechen { .. }))) |
                        Some(Order::Wheel(OrderWheel::IterBlocksInit(komm::Umschlag { .. }))) |
                        Some(Order::Wheel(OrderWheel::IterBlocksNextCancel(komm::UmschlagAbbrechen { .. }))) |
                        Some(Order::Wheel(OrderWheel::IterBlocksNext(komm::Umschlag { .. }))) =>
                            return Err(Error::UnexpectedIterBlocksReplyInRunningMode),
                    }
                },
    //             } else if let Some(RequestLookupRange { search_range, reply_kind, }) = env.incoming.request_lookup_range.pop() {
    //                 next.begin_lookup_range(search_range, reply_kind)
    //             } else if let Some(EventButcherFlushed { search_tree_id, root_block, }) = env.incoming.butcher_flushed.pop() {
    //                 next.butcher_flushed(search_tree_id, root_block)
    //             } else if let Some(EventLookupRangeMergeDone { access_token, }) = env.incoming.lookup_range_merge_done.pop() {
    //                 next.commit_lookup_range(access_token)
    //             } else if let Some(merge_search_trees_done) = env.incoming.merge_search_trees_done.pop() {
    //                 let EventMergeSearchTreesDone {
    //                     merged_search_tree_ref,
    //                     merged_search_tree_items_count,
    //                     access_token,
    //                 } = merge_search_trees_done;
    //                 next.search_trees_merged(
    //                     merged_search_tree_ref,
    //                     merged_search_tree_items_count,
    //                     access_token,
    //                 )
    //             } else if let Some(EventDemolishDone { search_tree_id, }) = env.incoming.demolish_done.pop() {
    //                 next.search_tree_demolished(search_tree_id)
    //             } else {
    //                 return Ok(Done { env, next: Next::Poll { next, }, });
    //             },
        };

        loop {
            performer_kont = match performer_kont {
                performer::Kont::Poll(performer::KontPoll { next, }) => {
                    welt_state.kont = Kont::StepPoll { next, };
                    break;
                },
                performer::Kont::InfoReady(performer::KontInfoReady { info, info_context: rueckkopplung, next, }) => {
                    rueckkopplung.commit(info)
                        .map_err(Error::CommitInfo)?;
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
                    rueckkopplung.commit(Flushed)
                        .map_err(Error::CommitFlushed)?;
                    next.commit_flush()
                },
                performer::Kont::FlushButcher(performer::KontFlushButcher { search_tree_id, frozen_memcache, next, }) => {
                    todo!()
                    // next.scheduled()
                },
                performer::Kont::LookupRangeMergerReady(performer::KontLookupRangeMergerReady { ranges_merger, lookup_context, next, }) => {
                    let merger_freie = arbeitssklave::Freie::new();
                    let merger_meister = merger_freie.meister();
                    let meister_ref = sklavenwelt.env
                        .lookup_range_merge_sklaven
                        .insert(merger_meister);
                    merger_freie
                        .versklaven(
                            lookup_range_merge::Welt {
                                kont: Some(lookup_range_merge::Kont::Start {
                                    merger: ranges_merger.source,
                                    lookup_context,
                                }),
                                meister_ref,
                                sendegeraet: sklavenwelt.env.sendegeraet.clone(),
                                wheels: sklavenwelt.env.wheels.clone(),
                                drop_bomb: sklavenwelt.env
                                    .sendegeraet
                                    .rueckkopplung(LookupRangeMergeDrop {
                                        access_token: ranges_merger.token,
                                        route: LookupRangeRoute { meister_ref, },
                                    }),
                            },
                            thread_pool,
                        )
                        .map_err(Error::LookupRangeMergerVersklaven)?;
                    next.got_it()
                },
                performer::Kont::MergeSearchTrees(performer::KontMergeSearchTrees { ranges_merger, next, }) => {
                    todo!()
                    // next.scheduled()
                },
                performer::Kont::DemolishSearchTree(performer::KontDemolishSearchTree { order, next, }) => {
                    todo!()
                    // next.roger_that()
                },
            };
        }
    }
}
