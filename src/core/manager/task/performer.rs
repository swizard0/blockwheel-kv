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
    pub request_insert: Vec<RequestInsert>,
    pub request_remove: Vec<RequestRemove>,
    pub request_lookup_range: Vec<RequestLookupRange>,
    pub request_flush: Vec<RequestFlush>,
    pub butcher_flushed: Vec<EventButcherFlushed>,
    pub lookup_range_merge_done: Vec<EventLookupRangeMergeDone>,
}

impl Incoming {
    pub fn is_empty(&self) -> bool {
        self.request_insert.is_empty() &&
            self.request_remove.is_empty() &&
            self.request_lookup_range.is_empty() &&
            self.request_flush.is_empty() &&
            self.butcher_flushed.is_empty() &&
            self.lookup_range_merge_done.is_empty()
    }

    pub fn transfill_from(&mut self, from: &mut Self) {
        self.request_insert.extend(from.request_insert.drain(..));
        self.request_remove.extend(from.request_remove.drain(..));
        self.request_lookup_range.extend(from.request_lookup_range.drain(..));
        self.request_flush.extend(from.request_flush.drain(..));
        self.butcher_flushed.extend(from.butcher_flushed.drain(..));
        self.lookup_range_merge_done.extend(from.lookup_range_merge_done.drain(..));
    }
}

pub struct EventButcherFlushed {
    pub search_tree_id: u64,
    pub root_block: BlockRef,
}

pub struct EventLookupRangeMergeDone {
    pub lookup_range_token: performer::LookupRangeToken,
}

#[derive(Default)]
pub struct Outgoing {
    pub flush_butcher: Vec<FlushButcherQuery>,
    pub lookup_range_merger_ready: Vec<LookupRangeMergerReadyQuery>,
    pub flushed: Vec<FlushedQuery>,
}

impl Outgoing {
    fn is_empty(&self) -> bool {
        self.flush_butcher.is_empty() &&
            self.lookup_range_merger_ready.is_empty() &&
            self.flushed.is_empty()
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
                if let Some(RequestInsert { key, value, reply_tx, }) = env.incoming.request_insert.pop() {
                    next.incoming_insert(key, value, reply_tx)
                } else if let Some(RequestRemove { key, reply_tx, }) = env.incoming.request_remove.pop() {
                    next.incoming_remove(key, reply_tx)
                } else if let Some(RequestLookupRange { search_range, reply_kind, }) = env.incoming.request_lookup_range.pop() {
                    next.begin_lookup_range(search_range, reply_kind)
                } else if let Some(RequestFlush { reply_tx, }) = env.incoming.request_flush.pop() {
                    next.incoming_flush(reply_tx)
                } else if let Some(EventButcherFlushed { search_tree_id, root_block, }) = env.incoming.butcher_flushed.pop() {
                    next.butcher_flushed(search_tree_id, root_block)
                } else if let Some(EventLookupRangeMergeDone { lookup_range_token, }) = env.incoming.lookup_range_merge_done.pop() {
                    next.commit_lookup_range(lookup_range_token)
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
            };
        }
    }
}
