use std::{
    str,
    cmp::{
        Ordering,
    },
};

use alloc_pool::bytes::{
    Bytes,
};

use crate::{
    kv,
    job,
    storage,
    core::{
        BlockRef,
        search_tree::{
            task::{
                Outcome,
                SearchOutcome,
                LookupRequestsQueue,
                SearchOutcomes,
            },
        },
    },
};

pub struct Args<J> where J: edeltraud::Job {
    pub block_ref: BlockRef,
    pub thread_pool: edeltraud::Edeltraud<J>,
    pub block_bytes: Bytes,
    pub lookup_requests_queue: LookupRequestsQueue,
    pub outcomes: SearchOutcomes,
}

pub struct Done {
    pub block_ref: BlockRef,
    pub outcomes: SearchOutcomes,
}

#[derive(Debug)]
pub enum Error {
    ReadBlockStorage { block_ref: BlockRef, error: storage::Error, },
    FilenameUtf8 { block_ref: BlockRef, error: str::Utf8Error, },
    ThreadPoolGone,
}

pub type JobOutput = Result<JobDone, Error>;

pub struct JobArgs {
    search_block_ref: BlockRef,
    block_bytes: Bytes,
    lookup_requests_queue: LookupRequestsQueue,
    outcomes: SearchOutcomes,
}

pub struct JobDone {
    outcomes: SearchOutcomes,
}

pub fn job(JobArgs { search_block_ref, block_bytes, mut lookup_requests_queue, mut outcomes, }: JobArgs) -> JobOutput {
    let mut entries_iter = storage::block_deserialize_iter(&block_bytes)
        .map_err(|error| Error::ReadBlockStorage {
            block_ref: search_block_ref.clone(),
            error,
        })?;
    let mut maybe_entry = entries_iter.next();
    let mut maybe_request = lookup_requests_queue.pop();
    loop {
        match maybe_request {
            None =>
                break,
            Some(request_key) =>
                match maybe_entry {
                    None => {
                        outcomes.push(SearchOutcome {
                            request: request_key,
                            outcome: Outcome::NotFound,
                        });
                        maybe_request = lookup_requests_queue.pop();
                        maybe_entry = None;
                    },
                    Some(entry_result) => {
                        let iter_entry = entry_result
                            .map_err(|error| Error::ReadBlockStorage {
                                block_ref: search_block_ref.clone(),
                                error,
                            })?;
                        match iter_entry.key.cmp(&request_key.key.key_bytes) {
                            Ordering::Less => {
                                maybe_request = Some(request_key);
                                maybe_entry = entries_iter.next();
                            },
                            Ordering::Equal => {
                                let owned_entry = entries_iter.to_owned_entry(&iter_entry);
                                let value_cell = match owned_entry.value_cell {
                                    kv::ValueCell { version, cell: kv::Cell::Value(ref value_ref), } =>
                                        kv::ValueCell {
                                            version,
                                            cell: kv::Cell::Value(storage::OwnedValueBlockRef::from_owned_value_ref(
                                                value_ref.clone(),
                                                &search_block_ref.blockwheel_filename,
                                            )),
                                        },
                                    kv::ValueCell { version, cell: kv::Cell::Tombstone, } =>
                                        kv::ValueCell { version, cell: kv::Cell::Tombstone, },
                                };
                                outcomes.push(SearchOutcome {
                                    request: request_key,
                                    outcome: Outcome::Found { value_cell, },
                                });
                                maybe_request = lookup_requests_queue.pop();
                                maybe_entry = Some(Ok(iter_entry));
                            },
                            Ordering::Greater => {
                                let owned_jump_ref = entries_iter.to_owned_jump_ref(&iter_entry.jump_ref);
                                let outcome = match owned_jump_ref {
                                    storage::OwnedJumpRef::None =>
                                        Outcome::NotFound,
                                    storage::OwnedJumpRef::Local(storage::LocalRef { ref block_id, }) =>
                                        Outcome::Jump {
                                            block_ref: BlockRef {
                                                blockwheel_filename: search_block_ref.blockwheel_filename.clone(),
                                                block_id: block_id.clone(),
                                            },
                                        },
                                    storage::OwnedJumpRef::External(ref block_ref) =>
                                        Outcome::Jump { block_ref: block_ref.clone(), },
                                };
                                outcomes.push(SearchOutcome {
                                    request: request_key,
                                    outcome,
                                });
                                maybe_request = lookup_requests_queue.pop();
                                maybe_entry = Some(Ok(iter_entry));
                            },
                        }
                    },
                },
        }
    }
    Ok(JobDone { outcomes, })
}

pub async fn run<J>(Args { block_ref, thread_pool, block_bytes, lookup_requests_queue, outcomes, }: Args<J>) -> Result<Done, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    let job_task = thread_pool.spawn(job::Job::SearchTreeSearchBlock(JobArgs {
        search_block_ref: block_ref.clone(),
        block_bytes,
        lookup_requests_queue,
        outcomes,
    }));
    let job_output = job_task.await
        .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
    let job_output: job::JobOutput = job_output.into();
    let job::SearchTreeSearchBlockDone(job_result) = job_output.into();
    let JobDone { outcomes, } = job_result?;
    Ok(Done { block_ref, outcomes, })
}
