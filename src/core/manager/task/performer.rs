use std::{
    sync::{
        Arc,
    },
};

use crate::{
    job,
    core::{
        performer,
        MemCache,
        BlockRef,
        Context as C,
        RequestInfo,
        RequestInsert,
        RequestRemove,
        RequestLookupKind,
        RequestLookupRange,
        RequestFlush,
        RequestFlushReplyTx,
    },
    Inserted,
    Removed,
};

pub struct Args<J> where J: edeltraud::Job {
    pub job_args: JobArgs,
    pub thread_pool: edeltraud::Edeltraud<J>,
}

pub struct Done {
    pub env: Env,
    pub next: Next,
}

#[derive(Debug)]
pub enum Error {
    ThreadPoolGone,
}

pub type Output = Result<Done, Error>;

pub async fn run<J>(args: Args<J>) -> Output
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    let job = job::Job::ManagerTaskPerformer(args.job_args);
    let job_output = args.thread_pool.spawn(job).await
        .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
    let job_output: job::JobOutput = job_output.into();
    let job::ManagerTaskPerformerDone(job_done) = job_output.into();
    job_done
}

pub struct JobArgs {
    pub env: Env,
    pub kont: Kont,
}

#[derive(Default)]
pub struct Env {
    pub incoming: Incoming,
    pub outgoing: Outgoing,
}

#[derive(Default)]
pub struct Incoming {
    pub request_info: Vec<RequestInfo>,
    pub request_insert: Vec<RequestInsert>,
    pub request_remove: Vec<RequestRemove>,
    pub request_lookup_range: Vec<RequestLookupRange>,
    pub request_flush: Vec<RequestFlush>,
    pub butcher_flushed: Vec<EventButcherFlushed>,
    pub lookup_range_merge_done: Vec<EventLookupRangeMergeDone>,
    pub merge_search_trees_done: Vec<EventMergeSearchTreesDone>,
    pub demolish_done: Vec<EventDemolishDone>,
}

impl Incoming {
    pub fn is_empty(&self) -> bool {
        self.request_info.is_empty() &&
            self.request_insert.is_empty() &&
            self.request_remove.is_empty() &&
            self.request_lookup_range.is_empty() &&
            self.request_flush.is_empty() &&
            self.butcher_flushed.is_empty() &&
            self.lookup_range_merge_done.is_empty() &&
            self.merge_search_trees_done.is_empty() &&
            self.demolish_done.is_empty()
    }

    pub fn transfill_from(&mut self, from: &mut Self) {
        self.request_info.extend(from.request_info.drain(..));
        self.request_insert.extend(from.request_insert.drain(..));
        self.request_remove.extend(from.request_remove.drain(..));
        self.request_lookup_range.extend(from.request_lookup_range.drain(..));
        self.request_flush.extend(from.request_flush.drain(..));
        self.butcher_flushed.extend(from.butcher_flushed.drain(..));
        self.lookup_range_merge_done.extend(from.lookup_range_merge_done.drain(..));
        self.merge_search_trees_done.extend(from.merge_search_trees_done.drain(..));
        self.demolish_done.extend(from.demolish_done.drain(..));
    }
}

pub struct EventButcherFlushed {
    pub search_tree_id: u64,
    pub root_block: BlockRef,
}

pub struct EventLookupRangeMergeDone {
    pub access_token: performer::AccessToken,
}

pub struct EventMergeSearchTreesDone {
    pub merged_search_tree_ref: BlockRef,
    pub merged_search_tree_items_count: usize,
    pub access_token: performer::AccessToken,
}

pub struct EventDemolishDone {
    pub search_tree_id: u64,
}

#[derive(Default)]
pub struct Outgoing {
    pub flush_butcher: Vec<FlushButcherQuery>,
    pub lookup_range_merger_ready: Vec<LookupRangeMergerReadyQuery>,
    pub flushed: Vec<FlushedQuery>,
    pub merge_search_trees: Vec<MergeSearchTreesQuery>,
    pub demolish_search_tree: Vec<DemolishSearchTreeQuery>,
}

impl Outgoing {
    fn is_empty(&self) -> bool {
        self.flush_butcher.is_empty() &&
            self.lookup_range_merger_ready.is_empty() &&
            self.flushed.is_empty() &&
            self.merge_search_trees.is_empty() &&
            self.demolish_search_tree.is_empty()
    }
}

pub struct FlushButcherQuery {
    pub search_tree_id: u64,
    pub frozen_memcache: Arc<MemCache>,
}

pub struct LookupRangeMergerReadyQuery {
    pub ranges_merger: performer::LookupRangesMerger,
    pub lookup_context: RequestLookupKind,
}

pub struct FlushedQuery {
    pub flush_context: RequestFlushReplyTx,
}

pub struct MergeSearchTreesQuery {
    pub ranges_merger: performer::SearchTreesMerger,
}

pub struct DemolishSearchTreeQuery {
    pub order: performer::DemolishOrder,
}

pub enum Kont {
    Start {
        performer: performer::Performer<C>,
    },
    StepPoll {
        next: performer::KontPollNext<C>,
    },
}

pub enum Next {
    Poll {
        next: performer::KontPollNext<C>,
    },
}

pub fn job(JobArgs { mut env, mut kont, }: JobArgs) -> Output {
    assert!(env.outgoing.is_empty());

    loop {
        let mut performer_kont = match kont {
            Kont::Start { performer, } =>
                performer.step(),
            Kont::StepPoll { next, } =>
                if let Some(RequestInfo { reply_tx, }) = env.incoming.request_info.pop() {
                    next.incoming_info(reply_tx)
                } else if let Some(RequestInsert { key, value, reply_tx, }) = env.incoming.request_insert.pop() {
                    next.incoming_insert(key, value, reply_tx)
                } else if let Some(RequestRemove { key, reply_tx, }) = env.incoming.request_remove.pop() {
                    next.incoming_remove(key, reply_tx)
                } else if let Some(RequestLookupRange { search_range, reply_kind, }) = env.incoming.request_lookup_range.pop() {
                    next.begin_lookup_range(search_range, reply_kind)
                } else if let Some(RequestFlush { reply_tx, }) = env.incoming.request_flush.pop() {
                    next.incoming_flush(reply_tx)
                } else if let Some(EventButcherFlushed { search_tree_id, root_block, }) = env.incoming.butcher_flushed.pop() {
                    next.butcher_flushed(search_tree_id, root_block)
                } else if let Some(EventLookupRangeMergeDone { access_token, }) = env.incoming.lookup_range_merge_done.pop() {
                    next.commit_lookup_range(access_token)
                } else if let Some(merge_search_trees_done) = env.incoming.merge_search_trees_done.pop() {
                    let EventMergeSearchTreesDone {
                        merged_search_tree_ref,
                        merged_search_tree_items_count,
                        access_token,
                    } = merge_search_trees_done;
                    next.search_trees_merged(
                        merged_search_tree_ref,
                        merged_search_tree_items_count,
                        access_token,
                    )
                } else if let Some(EventDemolishDone { search_tree_id, }) = env.incoming.demolish_done.pop() {
                    next.search_tree_demolished(search_tree_id)
                } else {
                    return Ok(Done { env, next: Next::Poll { next, }, });
                },
        };

        loop {
            performer_kont = match performer_kont {
                performer::Kont::Poll(performer::KontPoll { next, }) => {
                    kont = Kont::StepPoll { next, };
                    break;
                },
                performer::Kont::InfoReady(performer::KontInfoReady { info, info_context: reply_tx, next, }) => {
                    if let Err(_send_error) = reply_tx.send(info) {
                        log::warn!("client canceled info request");
                    }
                    next.got_it()
                },
                performer::Kont::Inserted(performer::KontInserted { version, insert_context: reply_tx, next, }) => {
                    if let Err(_send_error) = reply_tx.send(Inserted { version, }) {
                        log::warn!("client canceled insert request");
                    }
                    next.got_it()
                },
                performer::Kont::Removed(performer::KontRemoved { version, remove_context: reply_tx, next, }) => {
                    if let Err(_send_error) = reply_tx.send(Removed { version, }) {
                        log::warn!("client canceled remove request");
                    }
                    next.got_it()
                },
                performer::Kont::Flushed(performer::KontFlushed { flush_context, next, }) => {
                    env.outgoing.flushed.push(FlushedQuery { flush_context, });
                    next.commit_flush()
                },
                performer::Kont::FlushButcher(performer::KontFlushButcher { search_tree_id, frozen_memcache, next, }) => {
                    env.outgoing.flush_butcher.push(FlushButcherQuery {
                        search_tree_id,
                        frozen_memcache,
                    });
                    next.scheduled()
                },
                performer::Kont::LookupRangeMergerReady(performer::KontLookupRangeMergerReady { ranges_merger, lookup_context, next, }) => {
                    env.outgoing.lookup_range_merger_ready.push(LookupRangeMergerReadyQuery {
                        ranges_merger,
                        lookup_context,
                    });
                    next.got_it()
                },
                performer::Kont::MergeSearchTrees(performer::KontMergeSearchTrees { ranges_merger, next, }) => {
                    env.outgoing.merge_search_trees.push(MergeSearchTreesQuery { ranges_merger, });
                    next.scheduled()
                },
                performer::Kont::DemolishSearchTree(performer::KontDemolishSearchTree { order, next, }) => {
                    env.outgoing.demolish_search_tree.push(DemolishSearchTreeQuery { order, });
                    next.roger_that()
                },
            };
        }
    }
}
