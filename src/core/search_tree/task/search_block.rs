use std::{
    cmp::{
        Reverse,
        Ordering,
    },
};

use alloc_pool::bytes::{
    Bytes,
    BytesPool,
};

use crate::{
    kv::{
        self,
        ContainsKey,
    },
    storage,
    core::{
        BlockRef,
        search_tree::{
            task::{
                Outcome,
                SearchOutcome,
                RequestsQueue,
                SearchOutcomes,
            },
        },
    },
};

pub struct Args {
    pub block_ref: BlockRef,
    pub blocks_pool: BytesPool,
    pub block_bytes: Bytes,
    pub requests_queue: RequestsQueue,
    pub outcomes: SearchOutcomes,
}

pub struct Done {
    pub block_ref: BlockRef,
    pub outcomes: SearchOutcomes,
}

#[derive(Debug)]
pub enum Error {
    ReadBlockStorage(storage::Error),
    SearchBlockJoin(tokio::task::JoinError),
}

pub async fn run(Args { block_ref, blocks_pool, block_bytes, mut requests_queue, mut outcomes, }: Args) -> Result<Done, Error> {
    let search_block_ref = block_ref.clone();
    let search_task = tokio::task::spawn_blocking(move || {
        let mut entries_iter = storage::block_deserialize_iter(&block_bytes)?;
        let mut maybe_entry = entries_iter.next();
        let mut maybe_request = requests_queue.pop();
        loop {
            match maybe_request {
                None =>
                    break,
                Some(Reverse(request_key)) =>
                    match maybe_entry {
                        None => {
                            outcomes.push(SearchOutcome {
                                request: request_key.into_inner(),
                                outcome: Outcome::NotFound,
                            });
                            maybe_request = requests_queue.pop();
                            maybe_entry = None;
                        },
                        Some(entry_result) => {
                            let entry = entry_result?;
                            match entry.key.cmp(request_key.key_data()) {
                                Ordering::Less => {
                                    maybe_request = Some(Reverse(request_key));
                                    maybe_entry = entries_iter.next();
                                },
                                Ordering::Equal => {
                                    let mut key_builder = kv::Builder::new(blocks_pool.lend())
                                        .build_key();
                                    key_builder.writer().extend(entry.key.iter().cloned());
                                    let mut value_builder = key_builder
                                        .build_value();
                                    value_builder.writer().extend(entry.value.iter().cloned());
                                    let kv = value_builder.key_value_done();
                                    outcomes.push(SearchOutcome {
                                        request: request_key.into_inner(),
                                        outcome: Outcome::Found { kv, },
                                    });
                                    maybe_request = requests_queue.pop();
                                    maybe_entry = Some(Ok(entry));
                                },
                                Ordering::Greater => {
                                    let outcome = match entry.jump_ref {
                                        storage::JumpRef::None =>
                                            Outcome::NotFound,
                                        storage::JumpRef::Local(storage::LocalJumpRef { ref block_id, }) =>
                                            Outcome::Jump {
                                                block_ref: BlockRef {
                                                    blockwheel_filename: search_block_ref.blockwheel_filename.clone(),
                                                    block_id: block_id.clone(),
                                                },
                                            },
                                        storage::JumpRef::External(storage::ExternalJumpRef { filename, ref block_id, }) =>
                                            Outcome::Jump {
                                                block_ref: BlockRef {
                                                    blockwheel_filename: filename.into(),
                                                    block_id: block_id.clone(),
                                                },
                                            },
                                    };
                                    outcomes.push(SearchOutcome {
                                        request: request_key.into_inner(),
                                        outcome,
                                    });
                                    maybe_request = requests_queue.pop();
                                    maybe_entry = Some(Ok(entry));
                                },
                            }
                        },
                    },
            }
        }
        Ok(outcomes)
    });
    let outcomes = search_task.await
        .map_err(Error::SearchBlockJoin)?
        .map_err(Error::ReadBlockStorage)?;
    Ok(Done { block_ref, outcomes, })
}
