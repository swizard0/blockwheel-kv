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

pub struct Args {
    pub block_ref: BlockRef,
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
    SearchBlockJoin(tokio::task::JoinError),
}

pub async fn run(Args { block_ref, block_bytes, mut lookup_requests_queue, mut outcomes, }: Args) -> Result<Done, Error> {
    let search_block_ref = block_ref.clone();
    let search_task = tokio::task::spawn_blocking(move || {
        let mut entries_iter = storage::BlockDeserializeIter::new(block_bytes)
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
                            let (jump_ref, key_value_pair) = entry_result
                                .map_err(|error| Error::ReadBlockStorage {
                                    block_ref: search_block_ref.clone(),
                                    error,
                                })?;
                            match key_value_pair.key.key_bytes.cmp(&request_key.key.key_bytes) {
                                Ordering::Less => {
                                    maybe_request = Some(request_key);
                                    maybe_entry = entries_iter.next();
                                },
                                Ordering::Equal => {
                                    let value_cell = key_value_pair.value_cell.clone();
                                    outcomes.push(SearchOutcome {
                                        request: request_key,
                                        outcome: Outcome::Found { value_cell, },
                                    });
                                    maybe_request = lookup_requests_queue.pop();
                                    maybe_entry = Some(Ok((jump_ref, key_value_pair)));
                                },
                                Ordering::Greater => {
                                    let outcome = match jump_ref {
                                        storage::JumpRef::None =>
                                            Outcome::NotFound,
                                        storage::JumpRef::Local(storage::LocalJumpRef { ref block_id, }) =>
                                            Outcome::Jump {
                                                block_ref: BlockRef {
                                                    blockwheel_filename: search_block_ref.blockwheel_filename.clone(),
                                                    block_id: block_id.clone(),
                                                },
                                            },
                                        storage::JumpRef::External(storage::ExternalJumpRef { ref filename, ref block_id, }) =>
                                            Outcome::Jump {
                                                block_ref: BlockRef {
                                                    blockwheel_filename: str::from_utf8(&*filename)
                                                        .map_err(|error| Error::FilenameUtf8 {
                                                            block_ref: search_block_ref.clone(),
                                                            error,
                                                        })?
                                                        .into(),
                                                    block_id: block_id.clone(),
                                                },
                                            },
                                    };
                                    outcomes.push(SearchOutcome {
                                        request: request_key,
                                        outcome,
                                    });
                                    maybe_request = lookup_requests_queue.pop();
                                    maybe_entry = Some(Ok((jump_ref, key_value_pair)));
                                },
                            }
                        },
                    },
            }
        }
        Ok(outcomes)
    });
    let outcomes = search_task.await
        .map_err(Error::SearchBlockJoin)??;
    Ok(Done { block_ref, outcomes, })
}
