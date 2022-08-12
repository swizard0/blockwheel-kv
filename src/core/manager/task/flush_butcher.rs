use std::{
    sync::{
        Arc,
    },
    collections::{
        HashMap,
        hash_map::{
            Entry,
        },
    },
};

use futures::{
     stream::{
         FuturesUnordered,
     },
     StreamExt,
};

use o1::{
    set::{
        Ref,
    },
};

use alloc_pool::{
    pool,
    Unique,
    bytes::{
        Bytes,
        BytesPool,
    },
};

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
        search_tree_builder,
        MemCache,
        BlockRef,
        SearchTreeBuilderCps,
        SearchTreeBuilderBlockNext,
        SearchTreeBuilderBlockEntry,
    },
};

#[cfg(test)]
mod tests;

pub struct Args<J> where J: edeltraud::Job {
    pub search_tree_id: u64,
    pub frozen_memcache: Arc<MemCache>,
    pub wheels: wheels::Wheels,
    pub blocks_pool: BytesPool,
    pub block_entries_pool: pool::Pool<Vec<SearchTreeBuilderBlockEntry>>,
    pub search_tree_builder_params: search_tree_builder::Params,
    pub values_inline_size_limit: usize,
    pub thread_pool: edeltraud::Edeltraud<J>,
}

pub struct Done {
    pub search_tree_id: u64,
    pub root_block: BlockRef,
}

#[derive(Debug)]
pub enum Error {
    WheelsEmpty,
    ThreadPoolGone,
    WriteBlock(ero_blockwheel_fs::WriteBlockError),
    SearchTreeBuilder(search_tree_builder::Error),
    SerializeBlockStorage(storage::Error),
    SerializeValueBlockStorage(storage::Error),
}

pub async fn run<J>(
    Args {
        search_tree_id,
        frozen_memcache,
        wheels,
        blocks_pool,
        block_entries_pool,
        search_tree_builder_params,
        values_inline_size_limit,
        thread_pool,
    }: Args<J>,
)
    -> Result<Done, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    inner_run(
        search_tree_id,
        frozen_memcache,
        Wheels::Regular(wheels),
        blocks_pool,
        block_entries_pool,
        search_tree_builder_params,
        values_inline_size_limit,
        thread_pool,
    ).await
}

async fn inner_run<J>(
    search_tree_id: u64,
    frozen_memcache: Arc<MemCache>,
    mut wheels: Wheels,
    blocks_pool: BytesPool,
    block_entries_pool: pool::Pool<Vec<SearchTreeBuilderBlockEntry>>,
    search_tree_builder_params: search_tree_builder::Params,
    values_inline_size_limit: usize,
    thread_pool: edeltraud::Edeltraud<J>,
)
    -> Result<Done, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    enum Task<J> where J: edeltraud::Job + From<job::Job> {
        Job(edeltraud::Handle<J::Output>),
        WriteValue {
            wheel_ref: WheelRef,
            block_bytes: Bytes,
            async_ref: Ref,
            block_entry_index: usize,
        },
        WriteBlock {
            write_block_task: WriteBlockTask,
        },
    }

    enum TaskOutput {
        Job(JobDone),
        WriteValue {
            block_ref: BlockRef,
            async_ref: Ref,
            block_entry_index: usize,
        },
        WriteBlock {
            block_ref: BlockRef,
            async_ref: Ref,
        },
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
                    let job::ManagerTaskFlushButcherDone(job_done) = job_output.into();
                    Ok(TaskOutput::Job(job_done?))
                },
                Task::WriteValue { mut wheel_ref, block_bytes, async_ref, block_entry_index, } => {
                    let blockwheel_filename = wheel_ref.blockwheel_filename.clone();
                    let block_id = wheel_ref.blockwheel_pid.write_block(block_bytes).await
                        .map_err(Error::WriteBlock)?;
                    let block_ref = BlockRef { blockwheel_filename, block_id, };
                    Ok(TaskOutput::WriteValue { block_ref, async_ref, block_entry_index, })
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
            }
        }
    }

    let mut tasks = FuturesUnordered::new();

    enum BuilderState {
        Ready { job_args: JobArgs, },
        InProgress,
    }

    let mut builder_state = BuilderState::Ready {
        job_args: JobArgs {
            env: Env {
                blocks_pool: blocks_pool.clone(),
                incoming: Incoming::default(),
                outgoing: Outgoing::default(),
            },
            kont: Kont::Start {
                frozen_memcache,
                builder: search_tree_builder::BuilderCps::new(
                    block_entries_pool,
                    search_tree_builder_params,
                ),
            },
        },
    };

    struct ValueWritePending {
        prepare_block_task: PrepareBlockTask,
        pending_count: usize,
    }

    let mut value_writes_pending = HashMap::new();
    let mut incoming = Incoming::default();

    loop {
        enum BuilderAction<A, S> {
            Run(A),
            KeepState(S),
        }

        let builder_action = match builder_state {
            BuilderState::Ready { job_args: job_args @ JobArgs { kont: Kont::Start { .. }, .. }, } =>
                BuilderAction::Run(job_args),
            BuilderState::Ready { job_args, } if !incoming.is_empty() =>
                BuilderAction::Run(job_args),
            other =>
                BuilderAction::KeepState(other),
        };

        builder_state = match builder_action {
            BuilderAction::Run(mut job_args) => {
                job_args.env.incoming.transfill_from(&mut incoming);
                let job = job::Job::ManagerTaskFlushButcher(job_args);
                let job_handle = thread_pool.spawn_handle(job)
                    .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
                tasks.push(Task::<J>::Job(job_handle).run());
                BuilderState::InProgress
            },
            BuilderAction::KeepState(state) =>
                state,
        };

        match tasks.next().await.unwrap()? {

            TaskOutput::Job(JobDone::AwaitWriteBlockTasks { mut env, next, }) => {
                for prepare_block_task in env.outgoing.prepare_block_tasks.drain(..) {
                    let mut values_write_pending = 0;
                    for (block_entry_index, block_entry) in prepare_block_task.block_entries.iter().enumerate() {
                        match &block_entry.item.value_cell.cell {
                            kv::Cell::Value(storage::OwnedValueBlockRef::Inline(
                                kv::Value { ref value_bytes, },
                            )) if value_bytes.len() > values_inline_size_limit => {
                                let mut block_bytes = blocks_pool.lend();
                                storage::value_block_serialize(value_bytes, &mut block_bytes)
                                    .map_err(Error::SerializeValueBlockStorage)?;
                                let wheel_ref = wheels.acquire();
                                tasks.push(Task::WriteValue {
                                    wheel_ref,
                                    block_bytes: block_bytes.freeze(),
                                    async_ref: prepare_block_task.async_ref,
                                    block_entry_index,
                                }.run());
                                values_write_pending += 1;
                            },
                            kv::Cell::Value(..) | kv::Cell::Tombstone =>
                                (),
                        }
                    }

                    if values_write_pending == 0 {
                        incoming.serialize_block_tasks.push(
                            SerializeBlockTask {
                                wheel_ref: wheels.acquire(),
                                node_type: prepare_block_task.node_type,
                                async_ref: prepare_block_task.async_ref,
                                block_entries: prepare_block_task.block_entries,
                            },
                        );
                    } else {
                        value_writes_pending.insert(
                            prepare_block_task.async_ref,
                            ValueWritePending {
                                prepare_block_task,
                                pending_count: values_write_pending,
                            },
                        );
                    }
                }
                for write_block_task in env.outgoing.write_block_tasks.drain(..) {
                    tasks.push(Task::WriteBlock { write_block_task, }.run());
                }

                builder_state = BuilderState::Ready {
                    job_args: JobArgs { env, kont: Kont::Continue { next, }, },
                };
            },

            TaskOutput::Job(JobDone::Finished { root_block, }) => {
                assert!(incoming.is_empty());
                return Ok(Done { search_tree_id, root_block, });
            },

            TaskOutput::WriteValue { block_ref, async_ref, block_entry_index, } =>
                match value_writes_pending.entry(async_ref) {
                    Entry::Occupied(mut oe) => {
                        let value_write_pending = oe.get_mut();
                        assert!(value_write_pending.pending_count > 0);
                        value_write_pending.pending_count -= 1;
                        let block_entry = &mut value_write_pending
                            .prepare_block_task
                            .block_entries[block_entry_index];
                        match &mut block_entry.item.value_cell.cell {
                            value @ kv::Cell::Value(storage::OwnedValueBlockRef::Inline(..)) =>
                                *value = kv::Cell::Value(storage::OwnedValueBlockRef::Ref(block_ref)),
                            kv::Cell::Value(storage::OwnedValueBlockRef::Ref(..)) =>
                                unreachable!(),
                            kv::Cell::Tombstone =>
                                unreachable!(),
                        }
                        if value_write_pending.pending_count == 0 {
                            let (_, value_write_pending) = oe.remove_entry();
                            incoming.serialize_block_tasks.push(
                                SerializeBlockTask {
                                    wheel_ref: wheels.acquire(),
                                    node_type: value_write_pending.prepare_block_task.node_type,
                                    async_ref: value_write_pending.prepare_block_task.async_ref,
                                    block_entries: value_write_pending.prepare_block_task.block_entries,
                                },
                            );
                        }
                    },
                    Entry::Vacant(..) =>
                        unreachable!(),
                },

            TaskOutput::WriteBlock { block_ref, async_ref, } =>
                incoming.commit_block_tasks.push(CommitBlockTask { block_ref, async_ref, }),

        }
    }
}

pub struct JobArgs {
    env: Env,
    kont: Kont,
}

pub struct Env {
    blocks_pool: BytesPool,
    incoming: Incoming,
    outgoing: Outgoing,
}

#[derive(Default)]
struct Incoming {
    serialize_block_tasks: Vec<SerializeBlockTask>,
    commit_block_tasks: Vec<CommitBlockTask>,
}

impl Incoming {
    fn is_empty(&self) -> bool {
        self.serialize_block_tasks.is_empty() &&
            self.commit_block_tasks.is_empty()
    }

    pub fn transfill_from(&mut self, from: &mut Self) {
        self.serialize_block_tasks.extend(from.serialize_block_tasks.drain(..));
        self.commit_block_tasks.extend(from.commit_block_tasks.drain(..));
    }
}

struct SerializeBlockTask {
    wheel_ref: WheelRef,
    node_type: storage::NodeType,
    async_ref: Ref,
    block_entries: Unique<Vec<SearchTreeBuilderBlockEntry>>,
}

struct CommitBlockTask {
    block_ref: BlockRef,
    async_ref: Ref,
}

#[derive(Default)]
struct Outgoing {
    prepare_block_tasks: Vec<PrepareBlockTask>,
    write_block_tasks: Vec<WriteBlockTask>,
}

impl Outgoing {
    fn is_empty(&self) -> bool {
        self.prepare_block_tasks.is_empty() &&
            self.write_block_tasks.is_empty()
    }
}

struct PrepareBlockTask {
    node_type: storage::NodeType,
    async_ref: Ref,
    block_entries: Unique<Vec<SearchTreeBuilderBlockEntry>>,
}

struct WriteBlockTask {
    wheel_ref: WheelRef,
    async_ref: Ref,
    block_bytes: Bytes,
}

pub enum Kont {
    Start {
        frozen_memcache: Arc<MemCache>,
        builder: SearchTreeBuilderCps,
    },
    Continue {
        next: SearchTreeBuilderBlockNext,
    },
}

pub enum JobDone {
    AwaitWriteBlockTasks {
        env: Env,
        next: SearchTreeBuilderBlockNext,
    },
    Finished {
        root_block: BlockRef,
    },
}

pub type Output = Result<JobDone, Error>;

pub fn job(JobArgs { env, kont, }: JobArgs) -> Output {
    match kont {
        Kont::Start { frozen_memcache, builder, } =>
            job_build(env, frozen_memcache, builder),
        Kont::Continue { next, } =>
            job_continue(env, next),
    }
}

fn job_build(mut env: Env, frozen_memcache: Arc<MemCache>, builder: SearchTreeBuilderCps) -> Output {
    assert!(env.outgoing.prepare_block_tasks.is_empty());
    assert!(env.outgoing.write_block_tasks.is_empty());

    let mut memcache_iter = frozen_memcache.iter();

    let mut builder_kont = builder.step()
        .map_err(Error::SearchTreeBuilder)?;
    loop {
        match builder_kont {
            search_tree_builder::Kont::PollNextItemOrProcessedBlock(
                search_tree_builder::KontPollNextItemOrProcessedBlock { next, },
            ) => {
                let (ord_key, value_cell) = memcache_iter.next().unwrap();
                let item = kv::KeyValuePair {
                    key: ord_key.as_ref().clone(),
                    value_cell: value_cell.clone().into(),
                };
                builder_kont = next.item_arrived(item)
                    .map_err(Error::SearchTreeBuilder)?;
            },
            search_tree_builder::Kont::PollProcessedBlock(
                search_tree_builder::KontPollProcessedBlock { next, },
            ) => {
                assert!(memcache_iter.next().is_none());
                return Ok(JobDone::AwaitWriteBlockTasks { env, next, });
            }
            search_tree_builder::Kont::ProcessBlockAsync(
                search_tree_builder::KontProcessBlockAsync { node_type, block_entries, async_ref, next, },
            ) => {
                env.outgoing.prepare_block_tasks.push(PrepareBlockTask {
                    node_type,
                    async_ref,
                    block_entries,
                });
                builder_kont = next.process_scheduled()
                    .map_err(Error::SearchTreeBuilder)?;
            },
            search_tree_builder::Kont::Finished { .. } =>
                unreachable!("totally unexpected Kont::Finished during search tree building in butcher flush"),
        }
    }
}

fn job_continue(mut env: Env, mut builder_next: SearchTreeBuilderBlockNext) -> Output {
    loop {
        let mut builder_kont = if let Some(mut serialize_block_task) = env.incoming.serialize_block_tasks.pop() {
            let block_bytes = env.blocks_pool.lend();
            let mut kont =
                storage::BlockSerializer::start(
                    serialize_block_task.node_type,
                    serialize_block_task.block_entries.len(),
                    block_bytes,
                )
                .map_err(Error::SerializeBlockStorage)?;
            let mut block_entries_iter = serialize_block_task.block_entries.drain(..);
            let block_bytes = loop {
                match kont {
                    storage::BlockSerializerContinue::Done(block_bytes) =>
                        break block_bytes.freeze(),
                    storage::BlockSerializerContinue::More(serializer) =>
                        match block_entries_iter.next() {
                            Some(block_entry) => {
                                let ref value_ref_cell = block_entry.item.value_cell
                                    .into_owned_value_ref(&serialize_block_task.wheel_ref.blockwheel_filename);
                                let entry = storage::Entry {
                                    jump_ref: storage::JumpRef::from_maybe_block_ref(
                                        &block_entry.child_block_ref,
                                        &serialize_block_task.wheel_ref.blockwheel_filename,
                                    ),
                                    key: &block_entry.item.key.key_bytes,
                                    value_cell: value_ref_cell.into(),
                                };
                                kont = serializer.entry(entry)
                                    .map_err(Error::SerializeBlockStorage)?;
                            },
                            _ =>
                                unreachable!(),
                        },
                }
            };
            env.outgoing.write_block_tasks.push(WriteBlockTask {
                wheel_ref: serialize_block_task.wheel_ref,
                async_ref: serialize_block_task.async_ref,
                block_bytes,
            });
            continue;
        } else if let Some(commit_block_task) = env.incoming.commit_block_tasks.pop() {
            builder_next.block_processed(commit_block_task.async_ref, commit_block_task.block_ref)
                .map_err(Error::SearchTreeBuilder)?
        } else {
            return Ok(JobDone::AwaitWriteBlockTasks { env, next: builder_next, });
        };

        loop {
            match builder_kont {
                search_tree_builder::Kont::PollNextItemOrProcessedBlock(..) =>
                    unreachable!("totally unexpected Kont::PollNextItemOrProcessedBlock during search tree writing in butcher flush"),
                search_tree_builder::Kont::PollProcessedBlock(
                    search_tree_builder::KontPollProcessedBlock { next, },
                ) => {
                    builder_next = next;
                    break;
                },
                search_tree_builder::Kont::ProcessBlockAsync(
                    search_tree_builder::KontProcessBlockAsync { node_type, block_entries, async_ref, next, },
                ) => {
                    env.outgoing.prepare_block_tasks.push(PrepareBlockTask {
                        node_type,
                        async_ref,
                        block_entries,
                    });
                    builder_kont = next.process_scheduled()
                        .map_err(Error::SearchTreeBuilder)?;
                },
                search_tree_builder::Kont::Finished { root_block_ref: root_block, .. } => {
                    assert!(env.outgoing.is_empty());
                    assert!(env.incoming.is_empty());
                    return Ok(JobDone::Finished { root_block, });
                },
            }
        }
    }
}
