use futures::{
    stream::{
        FuturesUnordered,
    },
    StreamExt,
};

use alloc_pool::{
    Unique,
    bytes::{
        Bytes,
    },
};

use crate::{
    job,
    core::{
        performer,
        search_ranges_merge,
        BlockRef,
        RequestLookupKind,
    },
};

pub struct Args<J> where J: edeltraud::Job {
    pub ranges_merger: performer::LookupRangesMerger,
    pub lookup_context: RequestLookupKind,
    pub thread_pool: edeltraud::Edeltraud<J>,
}

pub struct Done {
    pub lookup_range_sources: Unique<Vec<performer::LookupRangeSource>>,
}

#[derive(Debug)]
pub enum Error {
    ThreadPoolGone,
}

pub async fn run<J>(
    Args {
        ranges_merger,
        lookup_context,
        thread_pool,
    }: Args<J>,
)
    -> Result<Done, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    inner_run(
        ranges_merger,
        lookup_context,
        thread_pool,
    ).await
}

async fn inner_run<J>(
    ranges_merger: performer::LookupRangesMerger,
    lookup_context: RequestLookupKind,
    thread_pool: edeltraud::Edeltraud<J>,
)
    -> Result<Done, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    enum Task<J> where J: edeltraud::Job + From<job::Job> {
        Job(edeltraud::Handle<J::Output>),
    }

    enum TaskOutput {
        Job(JobDone),
    }

    impl<J> Task<J>
    where J: edeltraud::Job + From<job::Job>,
          J::Output: From<job::JobOutput>,
          job::JobOutput: From<J::Output>,
    {
        async fn run(self) -> Result<TaskOutput, Error> {
            match self {
                Task::Job(job_handle) => {
                    let job_output = job_handle.await
                        .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
                    let job_output: job::JobOutput = job_output.into();
                    let job::ManagerTaskLookupRangeMergeDone(job_done) = job_output.into();
                    Ok(TaskOutput::Job(job_done?))
                },
            }
        }
    }

    let mut tasks = FuturesUnordered::new();

    enum MergerState {
        Ready { job_args: JobArgs, },
        InProgress,
    }

    let mut merger_state = MergerState::Ready {
        job_args: JobArgs {
            env: Env {
                incoming: Incoming::default(),
                outgoing: Outgoing::default(),
            },
            kont: Kont::Start {
                merger: ranges_merger.source,
            },
        },
    };

    let mut incoming = Incoming::default();

    loop {
        enum MergerAction<A, S> {
            Run(A),
            KeepState(S),
        }

        let merger_action = match merger_state {
            MergerState::Ready { job_args: job_args @ JobArgs { kont: Kont::Start { .. }, .. }, } =>
                MergerAction::Run(job_args),
            MergerState::Ready { job_args, } if !incoming.is_empty() =>
                MergerAction::Run(job_args),
            other =>
                MergerAction::KeepState(other),
        };

        merger_state = match merger_action {
            MergerAction::Run(mut job_args) => {
                job_args.env.incoming.transfill_from(&mut incoming);
                let job = job::Job::ManagerTaskLookupRangeMerge(job_args);
                let job_handle = thread_pool.spawn_handle(job)
                    .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
                tasks.push(Task::<J>::Job(job_handle).run());
                MergerState::InProgress
            },
            MergerAction::KeepState(state) =>
                state,
        };

        match tasks.next().await.unwrap()? {

            TaskOutput::Job(JobDone::AwaitRetrieveBlockTasks { mut env, next, }) => {

                todo!();
            },

            TaskOutput::Job(JobDone::Finished { lookup_range_sources, }) => {
                assert!(incoming.is_empty());
                return Ok(Done { lookup_range_sources, });
            },

        }
    }
}

pub struct JobArgs {
    env: Env,
    kont: Kont,
}

pub struct Env {
    incoming: Incoming,
    outgoing: Outgoing,
}

#[derive(Default)]
struct Incoming {
    receved_block_tasks: Vec<ReceivedBlockTask>,
}

impl Incoming {
    fn is_empty(&self) -> bool {
        self.receved_block_tasks.is_empty()
    }

    pub fn transfill_from(&mut self, from: &mut Self) {
        self.receved_block_tasks.extend(from.receved_block_tasks.drain(..));
    }
}

struct ReceivedBlockTask {
    async_token: search_ranges_merge::AsyncToken<performer::LookupRangeSource>,
    block_bytes: Bytes,
}

#[derive(Default)]
struct Outgoing {
    retrieve_block_tasks: Vec<RetrieveBlockTask>,
}

impl Outgoing {
    fn is_empty(&self) -> bool {
        self.retrieve_block_tasks.is_empty()
    }
}

struct RetrieveBlockTask {
    block_ref: BlockRef,
    async_token: search_ranges_merge::AsyncToken<performer::LookupRangeSource>,
}

pub enum Kont {
    Start {
        merger: search_ranges_merge::RangesMergeCps<performer::LookupRangeSource>,
    },
    ProceedAwaitBlocks {
        next: search_ranges_merge::KontAwaitBlocksNext<performer::LookupRangeSource>,
    },
}

pub enum JobDone {
    AwaitRetrieveBlockTasks {
        env: Env,
        next: search_ranges_merge::KontAwaitBlocksNext<performer::LookupRangeSource>,
    },
    Finished {
        lookup_range_sources: Unique<Vec<performer::LookupRangeSource>>,
    },
}

pub type Output = Result<JobDone, Error>;

pub fn job(JobArgs { env, kont, }: JobArgs) -> Output {

    todo!()
}
