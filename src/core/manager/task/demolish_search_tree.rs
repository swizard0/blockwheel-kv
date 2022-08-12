use futures::{
    stream::{
        FuturesUnordered,
    },
    StreamExt,
};

use alloc_pool::{
    bytes::{
        Bytes,
    },
};

use ero_blockwheel_fs as blockwheel_fs;

use crate::{
    job,
    wheels,
    core::{
        manager::{
            task::{
                Wheels,
                WheelRef,
            },
        },
        performer,
        search_tree_walker,
        BlockRef,
    },
};

pub struct Args<J> where J: edeltraud::Job {
    pub order: performer::DemolishOrder,
    pub wheels: wheels::Wheels,
    pub thread_pool: edeltraud::Edeltraud<J>,
}

pub struct Done {
    pub search_tree_id: u64,
}

#[derive(Debug)]
pub enum Error {
    ThreadPoolGone,
    WheelNotFound {
        blockwheel_filename: wheels::WheelFilename,
    },
    ReadBlock(blockwheel_fs::ReadBlockError),
    DeleteBlock(blockwheel_fs::DeleteBlockError),
    SearchTreeWalker(search_tree_walker::Error),
}

pub async fn run<J>(
    Args {
        order,
        wheels,
        thread_pool,
    }: Args<J>,
)
    -> Result<Done, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    inner_run(
        order,
        Wheels::Regular(wheels),
        thread_pool,
    ).await
}

async fn inner_run<J>(
    order: performer::DemolishOrder,
    wheels: Wheels,
    thread_pool: edeltraud::Edeltraud<J>,
)
    -> Result<Done, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    enum Task<J> where J: edeltraud::Job + From<job::Job> {
        Job(edeltraud::Handle<J::Output>),
        ReadBlock {
            read_block_task: ReadBlockTask,
        },
        DeleteBlock {
            delete_block_task: DeleteBlockTask,
        },
    }

    enum TaskOutput {
        Job(JobDone),
        BlockRead { block_bytes: Bytes, },
        BlockDeleted,
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
                    let job::ManagerTaskDemolishSearchTreeDone(job_done) = job_output.into();
                    Ok(TaskOutput::Job(job_done?))
                },
                Task::ReadBlock { read_block_task: ReadBlockTask { mut wheel_ref, block_ref, }, } => {
                    let block_bytes = wheel_ref.blockwheel_pid.read_block(block_ref.block_id.clone()).await
                        .map_err(Error::ReadBlock)?;
                    Ok(TaskOutput::BlockRead { block_bytes, })
                },
                Task::DeleteBlock { delete_block_task: DeleteBlockTask { mut wheel_ref, block_ref, }, } => {
                     match wheel_ref.blockwheel_pid.delete_block(block_ref.block_id.clone()).await {
                        Ok(blockwheel_fs::Deleted) =>
                             Ok(TaskOutput::BlockDeleted),
                        Err(error) =>
                            return Err(Error::DeleteBlock(error)),
                     }
                },
            }
        }
    }

    let mut tasks = FuturesUnordered::new();

    enum JobState {
        Ready { job_args: JobArgs, },
        InProgress,
        Finished,
    }

    let mut job_state = JobState::Ready {
        job_args: JobArgs {
            env: Env {
                wheels: wheels.clone(),
                incoming: Incoming::default(),
                outgoing: Outgoing::default(),
            },
            kont: Kont::Start {
                walker: order.walker,
            },
        },
    };

    let mut incoming = Incoming::default();
    let mut pending_delete_tasks = 0;

    loop {
        enum JobAction<A, S> {
            Run(A),
            KeepState(S),
        }

        let job_action = match job_state {
            JobState::Ready { job_args: job_args @ JobArgs { kont: Kont::Start { .. }, .. }, } =>
                JobAction::Run(job_args),
            JobState::Ready { job_args: job_args @ JobArgs { kont: Kont::ProceedAwaitBlocks { .. }, .. }, } if !incoming.is_empty() =>
                JobAction::Run(job_args),
            JobState::Finished if pending_delete_tasks == 0 =>
                return Ok(Done { search_tree_id: order.search_tree_id, }),
            other =>
                JobAction::KeepState(other),
        };

        job_state = match job_action {
            JobAction::Run(mut job_args) => {
                job_args.env.incoming.transfill_from(&mut incoming);
                let job = job::Job::ManagerTaskDemolishSearchTree(job_args);
                let job_handle = thread_pool.spawn_handle(job)
                    .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
                tasks.push(Task::<J>::Job(job_handle).run());
                JobState::InProgress
            },
            JobAction::KeepState(state) =>
                state,
        };

        match tasks.next().await.unwrap()? {

            TaskOutput::Job(JobDone::Await { mut env, kont, }) => {
                if let Some(read_block_task) = env.outgoing.read_block_task.take() {
                    tasks.push(Task::ReadBlock { read_block_task, }.run());
                }
                for delete_block_task in env.outgoing.delete_block_tasks.drain(..) {
                    tasks.push(Task::DeleteBlock { delete_block_task, }.run());
                    pending_delete_tasks += 1;
                }

                job_state = match job_state {
                    JobState::InProgress =>
                        JobState::Ready {
                            job_args: JobArgs { env, kont, },
                        },
                    JobState::Ready { .. } | JobState::Finished =>
                        unreachable!(),
                };
            },

            TaskOutput::Job(JobDone::Finished { env, }) => {
                assert!(env.outgoing.read_block_task.is_none());
                for delete_block_task in env.outgoing.delete_block_tasks {
                    tasks.push(Task::DeleteBlock { delete_block_task, }.run());
                    pending_delete_tasks += 1;
                }

                job_state = match job_state {
                    JobState::InProgress =>
                        JobState::Finished,
                    JobState::Ready { .. } | JobState::Finished =>
                        unreachable!(),
                };
            },

            TaskOutput::BlockRead { block_bytes, } =>
                incoming.received_block_tasks.push(ReceivedBlockTask { block_bytes, }),

            TaskOutput::BlockDeleted => {
                assert!(pending_delete_tasks > 0);
                pending_delete_tasks -= 1;
            },

        }
    }
}

pub struct JobArgs {
    env: Env,
    kont: Kont,
}

pub struct Env {
    wheels: Wheels,
    incoming: Incoming,
    outgoing: Outgoing,
}

#[derive(Default)]
struct Incoming {
    received_block_tasks: Vec<ReceivedBlockTask>,
}

impl Incoming {
    fn is_empty(&self) -> bool {
        self.received_block_tasks.is_empty()
    }

    pub fn transfill_from(&mut self, from: &mut Self) {
        self.received_block_tasks.extend(from.received_block_tasks.drain(..));
    }
}

struct ReceivedBlockTask {
    block_bytes: Bytes,
}

#[derive(Default)]
struct Outgoing {
    read_block_task: Option<ReadBlockTask>,
    delete_block_tasks: Vec<DeleteBlockTask>,
}

struct ReadBlockTask {
    wheel_ref: WheelRef,
    block_ref: BlockRef,
}

struct DeleteBlockTask {
    wheel_ref: WheelRef,
    block_ref: BlockRef,
}

pub enum Kont {
    Start {
        walker: search_tree_walker::WalkerCps,
    },
    ProceedAwaitBlocks {
        next: search_tree_walker::KontRequireBlockNext,
    },
}

pub enum JobDone {
    Await {
        env: Env,
        kont: Kont,
    },
    Finished {
        env: Env,
    },
}

pub type Output = Result<JobDone, Error>;

pub fn job(JobArgs { mut env, mut kont, }: JobArgs) -> Output {
    loop {
        let mut walker_kont = match kont {
            Kont::Start { walker } => {
                walker.step()
                    .map_err(Error::SearchTreeWalker)?
            },
            Kont::ProceedAwaitBlocks { next, } =>
                if let Some(ReceivedBlockTask { block_bytes, }) = env.incoming.received_block_tasks.pop() {
                    next.block_arrived(block_bytes)
                        .map_err(Error::SearchTreeWalker)?
                } else {
                    return Ok(JobDone::Await { env, kont: Kont::ProceedAwaitBlocks { next, }, });
                },
        };

        loop {
            match walker_kont {
                search_tree_walker::Kont::RequireBlock(search_tree_walker::KontRequireBlock { block_ref, next, }) => {
                    let wheel_ref = env.wheels.get(block_ref.blockwheel_filename.clone())
                        .ok_or_else(|| Error::WheelNotFound {
                            blockwheel_filename: block_ref.blockwheel_filename.clone(),
                        })?;
                    assert!(env.outgoing.read_block_task.is_none());
                    env.outgoing.read_block_task = Some(ReadBlockTask { wheel_ref, block_ref, });
                    kont = Kont::ProceedAwaitBlocks { next, };
                    break;
                },
                search_tree_walker::Kont::ItemFound(search_tree_walker::KontItemFound { next, .. }) => {
                    walker_kont = next.item_received()
                        .map_err(Error::SearchTreeWalker)?;
                },
                search_tree_walker::Kont::BlockFinished(search_tree_walker::KontBlockFinished { block_ref, next, }) => {
                    let wheel_ref = env.wheels.get(block_ref.blockwheel_filename.clone())
                        .ok_or_else(|| Error::WheelNotFound {
                            blockwheel_filename: block_ref.blockwheel_filename.clone(),
                        })?;
                    env.outgoing.delete_block_tasks.push(DeleteBlockTask { wheel_ref, block_ref, });
                    walker_kont = next.proceed()
                        .map_err(Error::SearchTreeWalker)?;
                },
                search_tree_walker::Kont::Finished =>
                    return Ok(JobDone::Finished { env, }),
            }
        }
    }
}
