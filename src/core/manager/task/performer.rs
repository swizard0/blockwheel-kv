use std::{
    sync::{
        Arc,
    },
};

use o1::{
    set::{
        Ref,
    },
};

use crate::{
    job,
    core::{
        performer,
        MemCache,
        Context as C,
        RequestInsert,
    },
    Inserted,
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
}

#[derive(Default)]
pub struct Outgoing {
    pub flush_butcher: Vec<FlushButcherQuery>,
}

pub struct FlushButcherQuery {
    pub search_tree_ref: Ref,
    pub frozen_memcache: Arc<MemCache>,
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
    loop {
        let mut performer_kont = match kont {
            Kont::Start { performer, } =>
                performer.step(),
            Kont::StepPoll { next, } =>
                if let Some(RequestInsert { key, value, reply_tx, }) = env.incoming.request_insert.pop() {
                    next.incoming_insert(key, value, reply_tx)
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
                performer::Kont::FlushButcher(performer::KontFlushButcher { search_tree_ref, frozen_memcache, next, }) => {
                    env.outgoing.flush_butcher.push(FlushButcherQuery {
                        search_tree_ref,
                        frozen_memcache,
                    });
                    next.scheduled()
                },
            };
        }
    }
}
