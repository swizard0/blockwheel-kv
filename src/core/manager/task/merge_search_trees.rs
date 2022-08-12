use futures::{
    stream::{
        FuturesUnordered,
    },
    StreamExt,
};

use alloc_pool::{
    pool,
    bytes::{
        Bytes,
        BytesPool,
    },
};

use o1::{
    set::{
        Ref,
    },
};

use ero_blockwheel_fs as blockwheel_fs;

use crate::{
    kv,
    job,
    wheels,
    storage,
    core::{
        manager::{
            task::{
                Wheels,
                WheelRef,
            },
        },
        performer,
        search_tree_builder,
        search_ranges_merge,
        BlockRef,
        SearchTreeBuilderCps,
        SearchTreeBuilderKont,
        SearchTreeBuilderBlockNext,
        SearchTreeBuilderBlockEntry,
        SearchTreeBuilderItemOrBlockNext,
        SearchRangesMergeCps,
        SearchRangesMergeKont,
        SearchRangesMergeBlockNext,
        SearchRangesMergeItemNext,
    },
};

pub struct Args<J> where J: edeltraud::Job {
    pub ranges_merger: performer::SearchTreesMerger,
    pub wheels: wheels::Wheels,
    pub blocks_pool: BytesPool,
    pub block_entries_pool: pool::Pool<Vec<SearchTreeBuilderBlockEntry>>,
    pub thread_pool: edeltraud::Edeltraud<J>,
    pub tree_block_size: usize,
}

pub struct Done {
    pub merged_search_tree_ref: BlockRef,
    pub merged_search_tree_items_count: usize,
    pub access_token: performer::AccessToken,
}

#[derive(Debug)]
pub enum Error {
    ThreadPoolGone,
    WheelNotFound {
        blockwheel_filename: wheels::WheelFilename,
    },
    SearchRangesMerge(search_ranges_merge::Error),
    SearchTreeBuilder(search_tree_builder::Error),
    SerializeBlockStorage(storage::Error),
    ReadBlock {
        block_ref: BlockRef,
        error: blockwheel_fs::ReadBlockError,
    },
    WriteBlock(blockwheel_fs::WriteBlockError),
    DeleteBlock {
        block_ref: BlockRef,
        error: blockwheel_fs::DeleteBlockError,
    },
}

pub async fn run<J>(
    Args {
        ranges_merger,
        wheels,
        blocks_pool,
        block_entries_pool,
        thread_pool,
        tree_block_size,
    }: Args<J>,
)
    -> Result<Done, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    inner_run(
        ranges_merger,
        Wheels::Regular(wheels),
        blocks_pool,
        block_entries_pool,
        thread_pool,
        tree_block_size,
    ).await
}

async fn inner_run<J>(
    ranges_merger: performer::SearchTreesMerger,
    wheels: Wheels,
    blocks_pool: BytesPool,
    block_entries_pool: pool::Pool<Vec<SearchTreeBuilderBlockEntry>>,
    thread_pool: edeltraud::Edeltraud<J>,
    tree_block_size: usize,
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
        WriteBlock {
            write_block_task: WriteBlockTask,
        },
        DeleteBlock {
            delete_block_task: DeleteBlockTask,
        },
    }

    enum TaskOutput {
        Job(JobDone),
        BlockRead {
            async_token: search_ranges_merge::AsyncToken<performer::LookupRangeSource>,
            block_bytes: Bytes,
        },
        WriteBlock {
            block_ref: BlockRef,
            async_ref: Ref,
        },
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
                    let job::ManagerTaskMergeSearchTreesDone(job_done) = job_output.into();
                    Ok(TaskOutput::Job(job_done?))
                },
                Task::ReadBlock { read_block_task: ReadBlockTask { mut wheel_ref, async_token, block_ref, }, } => {
                    let block_bytes = wheel_ref.blockwheel_pid.read_block(block_ref.block_id.clone()).await
                        .map_err(|error| Error::ReadBlock {
                            block_ref,
                            error,
                        })?;
                    Ok(TaskOutput::BlockRead { async_token, block_bytes, })
                },
                Task::WriteBlock { write_block_task: WriteBlockTask { mut wheel_ref, async_ref, block_bytes, }, } => {
                    let block_id = wheel_ref.blockwheel_pid.write_block(block_bytes).await
                        .map_err(Error::WriteBlock)?;
                    Ok(TaskOutput::WriteBlock {
                        block_ref: BlockRef {
                            blockwheel_filename: wheel_ref.blockwheel_filename,
                            block_id,
                        },
                        async_ref,
                    })
                },
                Task::DeleteBlock { delete_block_task: DeleteBlockTask { mut wheel_ref, block_ref, }, } =>
                    match wheel_ref.blockwheel_pid.delete_block(block_ref.block_id.clone()).await {
                        Ok(blockwheel_fs::Deleted) =>
                            Ok(TaskOutput::BlockDeleted),
                        Err(error) =>
                            return Err(Error::DeleteBlock { block_ref, error, }),
                    },
            }
        }
    }

    let mut tasks = FuturesUnordered::new();

    enum JobState {
        Ready { job_args: JobArgs, },
        InProgress,
        Finished {
            items_count: usize,
            root_block: BlockRef,
        },
    }

    let mut job_state = JobState::Ready {
        job_args: JobArgs {
            env: Env {
                wheels: wheels.clone(),
                blocks_pool: blocks_pool.clone(),
                incoming: Incoming::default(),
                outgoing: Outgoing::default(),
            },
            merge_kont: MergeKont::Start {
                merger: ranges_merger.source_count_items,
            },
            build_kont: BuildKont::CountItems {
                items_count: 0,
                merger_source_build: ranges_merger.source_build,
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
            JobState::Ready { job_args: job_args @ JobArgs { merge_kont: MergeKont::Start { .. }, .. }, } =>
                JobAction::Run(job_args),
            JobState::Ready { job_args: job_args @ JobArgs { build_kont: BuildKont::Active { kont: BuildKontActive::Start { .. }, .. }, .. }, } =>
                JobAction::Run(job_args),
            JobState::Ready { job_args: job_args @ JobArgs { merge_kont: MergeKont::ProceedAwaitBlocks { .. }, .. }, } if !incoming.is_empty() =>
                JobAction::Run(job_args),
            JobState::Ready {
                job_args: job_args @ JobArgs {
                    build_kont: BuildKont::Active {
                        kont: BuildKontActive::ProceedItemOrWrittenBlock { .. },
                        ..
                    },
                    ..
                },
            } if !incoming.is_empty() =>
                JobAction::Run(job_args),
            JobState::Ready {
                job_args: job_args @ JobArgs {
                    build_kont: BuildKont::Active {
                        kont: BuildKontActive::ProceedWrittenBlock { .. },
                        ..
                    },
                    ..
                },
            } if !incoming.is_empty() =>
                JobAction::Run(job_args),
            JobState::Finished { items_count, root_block, } if pending_delete_tasks == 0 =>
                return Ok(Done {
                    merged_search_tree_ref: root_block,
                    merged_search_tree_items_count: items_count,
                    access_token: ranges_merger.token,
                }),
            other =>
                JobAction::KeepState(other),
        };

        job_state = match job_action {
            JobAction::Run(mut job_args) => {
                job_args.env.incoming.transfill_from(&mut incoming);
                let job = job::Job::ManagerTaskMergeSearchTrees(job_args);
                let job_handle = thread_pool.spawn_handle(job)
                    .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
                tasks.push(Task::<J>::Job(job_handle).run());
                JobState::InProgress
            },
            JobAction::KeepState(state) =>
                state,
        };

        match tasks.next().await.unwrap()? {

            TaskOutput::Job(JobDone::Await { mut env, await_merge_kont, await_build_kont, }) => {
                for read_block_task in env.outgoing.read_block_tasks.drain(..) {
                    tasks.push(Task::ReadBlock { read_block_task, }.run());
                }
                for write_block_task in env.outgoing.write_block_tasks.drain(..) {
                    tasks.push(Task::WriteBlock { write_block_task, }.run());
                }
                for delete_block_task in env.outgoing.delete_block_tasks.drain(..) {
                    tasks.push(Task::DeleteBlock { delete_block_task, }.run());
                    pending_delete_tasks += 1;
                }

                job_state = match job_state {
                    JobState::InProgress =>
                        JobState::Ready {
                            job_args: JobArgs {
                                env,
                                merge_kont: await_merge_kont,
                                build_kont: await_build_kont,
                            },
                        },
                    JobState::Ready { .. } | JobState::Finished { .. } =>
                        unreachable!(),
                };
            },

            TaskOutput::Job(JobDone::ItemsCounted { mut env, items_count, merger_source_build, }) => {
                assert!(env.outgoing.read_block_tasks.is_empty());
                assert!(env.outgoing.write_block_tasks.is_empty());
                for delete_block_task in env.outgoing.delete_block_tasks.drain(..) {
                    tasks.push(Task::DeleteBlock { delete_block_task, }.run());
                    pending_delete_tasks += 1;
                }

                job_state = match job_state {
                    JobState::InProgress =>
                        JobState::Ready {
                            job_args: JobArgs {
                                env,
                                merge_kont: MergeKont::Start {
                                    merger: merger_source_build,
                                },
                                build_kont: BuildKont::Active {
                                    item_arrived: None,
                                    kont: BuildKontActive::Start {
                                        builder: search_tree_builder::BuilderCps::new(
                                            block_entries_pool.clone(),
                                            search_tree_builder::Params {
                                                tree_items_count: items_count,
                                                tree_block_size,
                                            },
                                        ),
                                    },
                                },
                            },
                        },
                    JobState::Ready { .. } | JobState::Finished { .. } =>
                        unreachable!(),
                };
            },

            TaskOutput::Job(JobDone::Finished { items_count, root_block, }) =>
                job_state = match job_state {
                    JobState::InProgress =>
                        JobState::Finished { items_count, root_block, },
                    JobState::Ready { .. } | JobState::Finished { .. } =>
                        unreachable!(),
                },

            TaskOutput::BlockRead { async_token, block_bytes, } =>
                incoming.received_block_tasks.push(ReceivedBlockTask { async_token, block_bytes, }),

            TaskOutput::WriteBlock { block_ref, async_ref, } =>
                incoming.written_block_tasks.push(WrittenBlockTask { block_ref, async_ref, }),

            TaskOutput::BlockDeleted => {
                assert!(pending_delete_tasks > 0);
                pending_delete_tasks -= 1;
            },

        }

    }
}

pub struct JobArgs {
    env: Env,
    merge_kont: MergeKont,
    build_kont: BuildKont,
}

pub struct Env {
    wheels: Wheels,
    blocks_pool: BytesPool,
    incoming: Incoming,
    outgoing: Outgoing,
}

#[derive(Default)]
struct Incoming {
    received_block_tasks: Vec<ReceivedBlockTask>,
    written_block_tasks: Vec<WrittenBlockTask>,
}

impl Incoming {
    fn is_empty(&self) -> bool {
        self.received_block_tasks.is_empty() &&
            self.written_block_tasks.is_empty()
    }

    fn transfill_from(&mut self, from: &mut Self) {
        self.received_block_tasks.extend(from.received_block_tasks.drain(..));
        self.written_block_tasks.extend(from.written_block_tasks.drain(..));
    }
}

struct ReceivedBlockTask {
    async_token: search_ranges_merge::AsyncToken<performer::LookupRangeSource>,
    block_bytes: Bytes,
}

struct WrittenBlockTask {
    async_ref: Ref,
    block_ref: BlockRef,
}

#[derive(Default)]
struct Outgoing {
    read_block_tasks: Vec<ReadBlockTask>,
    write_block_tasks: Vec<WriteBlockTask>,
    delete_block_tasks: Vec<DeleteBlockTask>,
}

impl Outgoing {
    fn is_empty(&self) -> bool {
        self.read_block_tasks.is_empty() &&
            self.write_block_tasks.is_empty() &&
            self.delete_block_tasks.is_empty()
    }
}

struct ReadBlockTask {
    wheel_ref: WheelRef,
    block_ref: BlockRef,
    async_token: search_ranges_merge::AsyncToken<performer::LookupRangeSource>,
}

struct WriteBlockTask {
    async_ref: Ref,
    wheel_ref: WheelRef,
    block_bytes: Bytes,
}

struct DeleteBlockTask {
    wheel_ref: WheelRef,
    block_ref: BlockRef,
}

pub enum MergeKont {
    Start {
        merger: SearchRangesMergeCps,
    },
    ProceedAwaitBlocks {
        next: SearchRangesMergeBlockNext,
    },
    ProceedItem {
        item: kv::KeyValuePair<storage::OwnedValueBlockRef>,
        next: SearchRangesMergeItemNext,
    },
    Finished,
}

pub enum BuildKont {
    CountItems {
        items_count: usize,
        merger_source_build: SearchRangesMergeCps,
    },
    Active {
        item_arrived: Option<kv::KeyValuePair<storage::OwnedValueBlockRef>>,
        kont: BuildKontActive,
    },
}

pub enum BuildKontActive {
    Start {
        builder: SearchTreeBuilderCps,
    },
    ProceedItemOrWrittenBlock {
        next: SearchTreeBuilderItemOrBlockNext,
    },
    ProceedWrittenBlock {
        next: SearchTreeBuilderBlockNext,
    },
    Finished {
        items_count: usize,
        root_block: BlockRef,
    },
}

pub enum JobDone {
    Await {
        env: Env,
        await_merge_kont: MergeKont,
        await_build_kont: BuildKont,
    },
    ItemsCounted {
        items_count: usize,
        merger_source_build: SearchRangesMergeCps,
        env: Env,
    },
    Finished {
        items_count: usize,
        root_block: BlockRef,
    },
}

pub type Output = Result<JobDone, Error>;

pub fn job(JobArgs { mut env, mut merge_kont, mut build_kont, }: JobArgs) -> Output {
    loop {
        enum MergeKontState<K, A, I> {
            Ready(K),
            Await(A),
            Idle(I),
            Finished,
        }

        let merge_kont_state = match merge_kont {
            MergeKont::Start { merger, } =>
                MergeKontState::Ready(
                    merger.step()
                        .map_err(Error::SearchRangesMerge)?,
                ),
            MergeKont::ProceedAwaitBlocks { next, } =>
                if let Some(ReceivedBlockTask { async_token, block_bytes, }) = env.incoming.received_block_tasks.pop() {
                    MergeKontState::Ready(
                        next.block_arrived(async_token, block_bytes)
                            .map_err(Error::SearchRangesMerge)?,
                    )
                } else {
                    MergeKontState::Await(MergeKont::ProceedAwaitBlocks { next, })
                },
            MergeKont::ProceedItem { item, next, } =>
                match &mut build_kont {
                    BuildKont::CountItems { items_count, .. } => {
                        *items_count += 1;
                        MergeKontState::Ready(next.proceed().map_err(Error::SearchRangesMerge)?)
                    },
                    BuildKont::Active { item_arrived: item_arrived @ None, .. } => {
                        *item_arrived = Some(item);
                        MergeKontState::Ready(next.proceed().map_err(Error::SearchRangesMerge)?)
                    },
                    BuildKont::Active { item_arrived: Some(..), .. } =>
                        MergeKontState::Idle(MergeKont::ProceedItem { item, next, }),
                },
            MergeKont::Finished =>
                MergeKontState::Finished,
        };

        enum BuildKontState<K, A, I> {
            CountItems {
                items_count: usize,
                merger_source_build: SearchRangesMergeCps,
            },
            Active {
                item_arrived: I,
                kont: Active<K, A>,
            },
        }

        enum Active<K, A> {
            Ready(K),
            Await(A),
            Finished {
                items_count: usize,
                root_block: BlockRef,
            },
        }

        let build_kont_state = match build_kont {
            BuildKont::CountItems { items_count, merger_source_build, } =>
                BuildKontState::CountItems { items_count, merger_source_build, },
            BuildKont::Active { item_arrived, kont: BuildKontActive::Start { builder, }, } =>
                BuildKontState::Active {
                    item_arrived,
                    kont: Active::Ready(
                        builder.step()
                            .map_err(Error::SearchTreeBuilder)?,
                    ),
                },
            BuildKont::Active { item_arrived, kont: BuildKontActive::ProceedWrittenBlock { next, }, } =>
                if let Some(WrittenBlockTask { async_ref, block_ref, }) = env.incoming.written_block_tasks.pop() {
                    BuildKontState::Active {
                        item_arrived,
                        kont: Active::Ready(
                            next.block_processed(async_ref, block_ref)
                                .map_err(Error::SearchTreeBuilder)?,
                        ),
                    }
                } else {
                    BuildKontState::Active {
                        item_arrived,
                        kont: Active::Await(BuildKontActive::ProceedWrittenBlock { next, }),
                    }
                },
            BuildKont::Active { mut item_arrived, kont: BuildKontActive::ProceedItemOrWrittenBlock { next, }, } =>
                if let Some(WrittenBlockTask { async_ref, block_ref, }) = env.incoming.written_block_tasks.pop() {
                    BuildKontState::Active {
                        item_arrived,
                        kont: Active::Ready(
                            next.block_processed(async_ref, block_ref)
                                .map_err(Error::SearchTreeBuilder)?,
                        ),
                    }
                } else if let Some(item) = item_arrived.take() {
                    BuildKontState::Active {
                        item_arrived,
                        kont: Active::Ready(
                            next.item_arrived(item)
                                .map_err(Error::SearchTreeBuilder)?,
                        ),
                    }
                } else {
                    BuildKontState::Active {
                        item_arrived,
                        kont: Active::Await(BuildKontActive::ProceedItemOrWrittenBlock { next, }),
                    }
                },
            BuildKont::Active { item_arrived, kont: BuildKontActive::Finished { items_count, root_block, }, .. } =>
                BuildKontState::Active { item_arrived, kont: Active::Finished { items_count, root_block, }, },
        };

        match (merge_kont_state, build_kont_state) {
            (MergeKontState::Ready(merger_kont), BuildKontState::CountItems { items_count, merger_source_build, }) => {
                merge_kont = job_step_merger_emit_deprecated(&mut env, merger_kont)?;
                build_kont = BuildKont::CountItems { items_count, merger_source_build, };
            },
            (MergeKontState::Await(await_merge_kont), BuildKontState::CountItems { items_count, merger_source_build, }) => {
                return Ok(JobDone::Await {
                    env,
                    await_merge_kont,
                    await_build_kont: BuildKont::CountItems { items_count, merger_source_build, },
                });
            },
            (MergeKontState::Idle(..), BuildKontState::CountItems { .. }) =>
                unreachable!(),
            (MergeKontState::Finished, BuildKontState::CountItems { items_count, merger_source_build, }) =>
                return Ok(JobDone::ItemsCounted { items_count, merger_source_build, env, }),

            (MergeKontState::Ready(merger_kont), BuildKontState::Active { item_arrived, kont: Active::Ready(builder_kont), }) => {
                merge_kont = job_step_merger(&mut env, merger_kont)?;
                build_kont = job_step_builder(&mut env, item_arrived, builder_kont)?;
            },
            (MergeKontState::Ready(merger_kont), BuildKontState::Active { item_arrived, kont: Active::Await(await_build_kont), }) => {
                merge_kont = job_step_merger(&mut env, merger_kont)?;
                build_kont = BuildKont::Active { item_arrived, kont: await_build_kont, };
            },
            (MergeKontState::Await(await_merge_kont), BuildKontState::Active { item_arrived, kont: Active::Ready(builder_kont), }) => {
                merge_kont = await_merge_kont;
                build_kont = job_step_builder(&mut env, item_arrived, builder_kont)?;
            },
            (MergeKontState::Await(await_merge_kont), BuildKontState::Active { item_arrived, kont: Active::Await(await_build_kont), }) => {
                return Ok(JobDone::Await {
                    env,
                    await_merge_kont,
                    await_build_kont: BuildKont::Active { item_arrived, kont: await_build_kont, },
                });
            },
            (MergeKontState::Idle(idle_merge_kont), BuildKontState::Active { item_arrived, kont: Active::Ready(builder_kont), }) => {
                merge_kont = idle_merge_kont;
                build_kont = job_step_builder(&mut env, item_arrived, builder_kont)?;
            },
            (MergeKontState::Idle(idle_merge_kont), BuildKontState::Active { item_arrived, kont: Active::Await(await_build_kont), }) => {
                return Ok(JobDone::Await {
                    env,
                    await_merge_kont: idle_merge_kont,
                    await_build_kont: BuildKont::Active { item_arrived, kont: await_build_kont, },
                });
            },
            (MergeKontState::Finished, BuildKontState::Active { item_arrived, kont: Active::Ready(builder_kont), }) => {
                merge_kont = MergeKont::Finished;
                build_kont = job_step_builder(&mut env, item_arrived, builder_kont)?;
            },
            (MergeKontState::Finished, BuildKontState::Active { item_arrived, kont: Active::Await(await_build_kont), }) => {
                return Ok(JobDone::Await {
                    env,
                    await_merge_kont: MergeKont::Finished,
                    await_build_kont: BuildKont::Active { item_arrived, kont: await_build_kont, },
                });
            },
            (MergeKontState::Ready(merger_kont), BuildKontState::Active { item_arrived, kont: Active::Finished { items_count, root_block, }, }) => {
                merge_kont = job_step_merger(&mut env, merger_kont)?;
                build_kont = BuildKont::Active { item_arrived, kont: BuildKontActive::Finished { items_count, root_block, }, };
            },
            (MergeKontState::Await(await_merge_kont), BuildKontState::Active { item_arrived, kont: Active::Finished { items_count, root_block, }, }) => {
                return Ok(JobDone::Await {
                    env,
                    await_merge_kont,
                    await_build_kont: BuildKont::Active { item_arrived, kont: BuildKontActive::Finished { items_count, root_block, }, },
                });
            },
            (MergeKontState::Idle(..), BuildKontState::Active { kont: Active::Finished { .. }, .. }) =>
                unreachable!(),
            (MergeKontState::Finished, BuildKontState::Active { kont: Active::Finished { items_count, root_block, }, .. }) => {
                assert!(env.outgoing.is_empty());
                assert!(env.incoming.is_empty());
                return Ok(JobDone::Finished { items_count, root_block, });
            },
        }
    }
}

fn job_step_merger_emit_deprecated(env: &mut Env, merger_kont: SearchRangesMergeKont) -> Result<MergeKont, Error> {
    job_step_merger_actual(env, merger_kont, true)
}

fn job_step_merger(env: &mut Env, merger_kont: SearchRangesMergeKont) -> Result<MergeKont, Error> {
    job_step_merger_actual(env, merger_kont, false)
}

fn job_step_merger_actual(env: &mut Env, mut merger_kont: SearchRangesMergeKont, emit_deprecated: bool) -> Result<MergeKont, Error> {
    loop {
        match merger_kont {
            search_ranges_merge::Kont::RequireBlockAsync(
                search_ranges_merge::KontRequireBlockAsync { block_ref, async_token, next, },
            ) => {
                let wheel_ref = env.wheels.get(block_ref.blockwheel_filename.clone())
                    .ok_or_else(|| Error::WheelNotFound {
                        blockwheel_filename: block_ref.blockwheel_filename.clone(),
                    })?;
                env.outgoing.read_block_tasks.push(ReadBlockTask { wheel_ref, block_ref, async_token, });
                merger_kont = next.scheduled()
                    .map_err(Error::SearchRangesMerge)?;
            },
            search_ranges_merge::Kont::AwaitBlocks(search_ranges_merge::KontAwaitBlocks { next, }) =>
                return Ok(MergeKont::ProceedAwaitBlocks { next, }),
            search_ranges_merge::Kont::BlockFinished(search_ranges_merge::KontBlockFinished { next, .. }) => {
                merger_kont = next.proceed()
                    .map_err(Error::SearchRangesMerge)?;
            },
            search_ranges_merge::Kont::EmitDeprecated(search_ranges_merge::KontEmitDeprecated { item, next, }) if emit_deprecated => {
                match item {
                    kv::KeyValuePair {
                        value_cell: kv::ValueCell {
                            cell: kv::Cell::Value(storage::OwnedValueBlockRef::Ref(block_ref)),
                            ..
                        },
                        ..
                    } => {
                        let wheel_ref = env.wheels.get(block_ref.blockwheel_filename.clone())
                            .ok_or_else(|| Error::WheelNotFound {
                                blockwheel_filename: block_ref.blockwheel_filename.clone(),
                            })?;
                        env.outgoing.delete_block_tasks.push(DeleteBlockTask { wheel_ref, block_ref, });
                    },
                    _ =>
                        (),
                }
                merger_kont = next.proceed()
                    .map_err(Error::SearchRangesMerge)?;
            },
            search_ranges_merge::Kont::EmitDeprecated(search_ranges_merge::KontEmitDeprecated { next, .. }) => {
                merger_kont = next.proceed()
                    .map_err(Error::SearchRangesMerge)?;
            },
            search_ranges_merge::Kont::EmitItem(
                search_ranges_merge::KontEmitItem { item, next, },
            ) =>
                return Ok(MergeKont::ProceedItem { item, next, }),
            search_ranges_merge::Kont::Finished =>
                return Ok(MergeKont::Finished),
        }
    }
}

fn job_step_builder(
    env: &mut Env,
    item_arrived: Option<kv::KeyValuePair<storage::OwnedValueBlockRef>>,
    mut builder_kont: SearchTreeBuilderKont,
)
    -> Result<BuildKont, Error>
{
    loop {
        match builder_kont {
            search_tree_builder::Kont::PollNextItemOrProcessedBlock(
                search_tree_builder::KontPollNextItemOrProcessedBlock { next, },
            ) =>
                return Ok(BuildKont::Active { item_arrived, kont: BuildKontActive::ProceedItemOrWrittenBlock { next, }, }),
            search_tree_builder::Kont::PollProcessedBlock(
                search_tree_builder::KontPollProcessedBlock { next, },
            ) =>
                return Ok(BuildKont::Active { item_arrived, kont: BuildKontActive::ProceedWrittenBlock { next, }, }),
            search_tree_builder::Kont::ProcessBlockAsync(
                search_tree_builder::KontProcessBlockAsync { node_type, mut block_entries, async_ref, next, },
            ) => {
                // acquire target wheel
                let wheel_ref = env.wheels.acquire();

                // serialize block
                let block_bytes = env.blocks_pool.lend();
                let mut serialize_kont = storage::BlockSerializer::start(node_type, block_entries.len(), block_bytes)
                    .map_err(Error::SerializeBlockStorage)?;
                let mut block_entries_iter = block_entries.drain(..);
                let block_bytes = loop {
                    match serialize_kont {
                        storage::BlockSerializerContinue::Done(block_bytes) =>
                            break block_bytes.freeze(),
                        storage::BlockSerializerContinue::More(serializer) => {
                            let block_entry = block_entries_iter.next().unwrap();
                            let ref value_ref_cell = block_entry.item.value_cell
                                .into_owned_value_ref(&wheel_ref.blockwheel_filename);
                            let entry = storage::Entry {
                                jump_ref: storage::JumpRef::from_maybe_block_ref(
                                    &block_entry.child_block_ref,
                                    &wheel_ref.blockwheel_filename,
                                ),
                                key: &block_entry.item.key.key_bytes,
                                value_cell: value_ref_cell.into(),
                            };
                            serialize_kont = serializer.entry(entry)
                                .map_err(Error::SerializeBlockStorage)?;
                        },
                    }
                };

                env.outgoing.write_block_tasks.push(WriteBlockTask {
                    wheel_ref,
                    async_ref,
                    block_bytes,
                });
                builder_kont = next.process_scheduled()
                    .map_err(Error::SearchTreeBuilder)?;
            },
            search_tree_builder::Kont::Finished { items_count, root_block_ref: root_block, } =>
                return Ok(BuildKont::Active { item_arrived, kont: BuildKontActive::Finished { items_count, root_block, }, }),
        }
    }
}
