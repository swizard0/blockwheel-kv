use std::{
    mem,
    ops::{
        Bound,
    },
    cmp::{
        Ordering,
    },
};

use alloc_pool::{
    pool,
    bytes::{
        Bytes,
    },
    Unique,
};

use crate::{
    kv,
    storage,
    core::{
        BlockRef,
        SearchRangeBounds,
    },
};

#[cfg(test)]
mod tests;

pub struct WalkerCps {
    inner: Inner,
}

pub enum Kont {
    RequireBlock(KontRequireBlock),
    ItemFound(KontItemFound),
    Finished,
}

#[derive(Debug)]
pub enum Error {
    ReadBlockStorage { block_ref: BlockRef, error: storage::Error, },
}

struct Inner {
    state: State,
    block_entry_steps_pool: pool::Pool<Vec<BlockEntryStep>>,
    levels: Vec<WalkerLevel>,
}

enum State {
    Taken,
    AwaitBlock {
        search_range: SearchRangeBounds,
        block_ref: BlockRef,
    },
    BlockReceived {
        search_range: SearchRangeBounds,
        block_ref: BlockRef,
        block_bytes: Bytes,
    },
    WalkLevel,
}

pub struct KontRequireBlock {
    pub block_ref: BlockRef,
    pub next: KontRequireBlockNext,
}

pub struct KontRequireBlockNext {
    search_range: SearchRangeBounds,
    block_ref: BlockRef,
    inner: Inner,
}

pub struct KontItemFound {
    pub item: kv::KeyValuePair<storage::OwnedValueBlockRef>,
    pub next: KontItemFoundNext,
}

pub struct KontItemFoundNext {
    inner: Inner,
}

struct WalkerLevel {
    block_ref: BlockRef,
    search_range: SearchRangeBounds,
    block_entry_steps: Unique<Vec<BlockEntryStep>>,
    block_entry_steps_index: usize,
}

pub enum BlockEntryStep {
    TryJump(BlockEntryAction),
    TryStep(BlockEntryAction),
    Done,
}

pub enum BlockEntryAction {
    OnlyJump(BlockRef),
    OnlyEntry {
        key: kv::Key,
        value_cell: kv::ValueCell<storage::OwnedValueBlockRef>,
    },
    JumpAndEntry {
        jump: BlockRef,
        key: kv::Key,
        value_cell: kv::ValueCell<storage::OwnedValueBlockRef>,
    },
}

impl WalkerCps {
    pub fn new(
        root_block: BlockRef,
        search_range: SearchRangeBounds,
        block_entry_steps_pool: pool::Pool<Vec<BlockEntryStep>>,
    ) -> Self {
        WalkerCps {
            inner: Inner {
                state: State::AwaitBlock {
                    search_range,
                    block_ref: root_block,
                },
                block_entry_steps_pool,
                levels: Vec::new(),
            },
        }
    }

    pub fn step(mut self) -> Result<Kont, Error> {
        loop {
            match self.inner.state {

                State::Taken =>
                    unreachable!("unexpected State::Taken encountered"),

                State::AwaitBlock { search_range, block_ref, } => {
                    self.inner.state = State::Taken;
                    return Ok(Kont::RequireBlock(KontRequireBlock {
                        block_ref: block_ref.clone(),
                        next: KontRequireBlockNext {
                            search_range,
                            block_ref,
                            inner: self.inner,
                        },
                    }));
                },

                State::BlockReceived { search_range, block_ref, block_bytes, } => {
                    let mut block_entry_steps = self.inner.block_entry_steps_pool.lend(Vec::new);
                    block_entry_steps.clear();

                    let entries_iter = storage::block_deserialize_iter(&block_bytes)
                        .map_err(|error| Error::ReadBlockStorage { block_ref: block_ref.clone(), error, })?;
                    for maybe_entry in entries_iter {
                        let iter_entry = maybe_entry
                            .map_err(|error| Error::ReadBlockStorage { block_ref: block_ref.clone(), error, })?;
                        match &search_range {
                            SearchRangeBounds { range_from: Bound::Unbounded, .. } =>
                                (),
                            SearchRangeBounds { range_from: Bound::Excluded(key), .. } =>
                                match key.key_bytes[..].cmp(iter_entry.key) {
                                    Ordering::Less =>
                                        (),
                                    Ordering::Equal | Ordering::Greater =>
                                        continue,
                                },
                            SearchRangeBounds { range_from: Bound::Included(key), .. } =>
                                match key.key_bytes[..].cmp(iter_entry.key) {
                                    Ordering::Less | Ordering::Equal =>
                                        (),
                                    Ordering::Greater =>
                                        continue,
                                },
                        }

                        let owned_jump_ref = storage::OwnedJumpRef::from_jump_ref(&iter_entry.jump_ref, &block_bytes);
                        let maybe_jump_block_ref = match owned_jump_ref {
                            storage::OwnedJumpRef::None =>
                                None,
                            storage::OwnedJumpRef::Local(storage::LocalRef { block_id, }) =>
                                Some(BlockRef {
                                    blockwheel_filename: block_ref.blockwheel_filename.clone(),
                                    block_id: block_id.clone(),
                                }),
                            storage::OwnedJumpRef::External(block_ref) =>
                                Some(block_ref),
                        };

                        let force_stop = match &search_range {
                            SearchRangeBounds { range_to: Bound::Unbounded, .. } =>
                                false,
                            SearchRangeBounds { range_to: Bound::Excluded(key), .. } =>
                                match key.key_bytes[..].cmp(iter_entry.key) {
                                    Ordering::Less | Ordering::Equal =>
                                        true,
                                    Ordering::Greater =>
                                        false,
                                },
                            SearchRangeBounds { range_to: Bound::Included(key), .. } =>
                                match key.key_bytes[..].cmp(iter_entry.key) {
                                    Ordering::Less  =>
                                        true,
                                    Ordering::Equal | Ordering::Greater =>
                                        false,
                                },
                        };

                        let owned_entry = storage::OwnedEntry::from_entry(&iter_entry, &block_bytes);
                        let key = owned_entry.key;
                        let value_cell = match owned_entry.value_cell {
                            kv::ValueCell { version, cell: kv::Cell::Value(value_ref), } =>
                                kv::ValueCell {
                                    version,
                                    cell: kv::Cell::Value(storage::OwnedValueBlockRef::from_owned_value_ref(
                                        value_ref,
                                        &block_ref.blockwheel_filename,
                                    )),
                                },
                            kv::ValueCell { version, cell: kv::Cell::Tombstone, } =>
                                kv::ValueCell { version, cell: kv::Cell::Tombstone, },
                        };

                        match (maybe_jump_block_ref, force_stop) {
                            (None, true) =>
                                break,
                            (None, false) =>
                                block_entry_steps.push(BlockEntryStep::TryJump(
                                    BlockEntryAction::OnlyEntry { key, value_cell, },
                                )),
                            (Some(jump_block_ref), true) => {
                                block_entry_steps.push(BlockEntryStep::TryJump(
                                    BlockEntryAction::OnlyJump(jump_block_ref),
                                ));
                                break;
                            },
                            (Some(jump_block_ref), false) =>
                                block_entry_steps.push(BlockEntryStep::TryJump(
                                    BlockEntryAction::JumpAndEntry {
                                        jump: jump_block_ref,
                                        key,
                                        value_cell,
                                    },
                                )),
                        }
                    }
                    block_entry_steps.shrink_to_fit();

                    self.inner.levels.push(WalkerLevel {
                        block_ref,
                        search_range,
                        block_entry_steps,
                        block_entry_steps_index: 0,
                    });
                    self.inner.state = State::WalkLevel;
                },

                State::WalkLevel if self.inner.levels.is_empty() =>
                    return Ok(Kont::Finished),

                State::WalkLevel => {
                    let current_level = self.inner.levels.last_mut().unwrap();
                    if current_level.block_entry_steps_index >= current_level.block_entry_steps.len() {
                        self.inner.levels.pop();
                        continue;
                    }
                    let current_block_entry_step =
                        &mut current_level.block_entry_steps[current_level.block_entry_steps_index];
                    let block_entry_step = mem::replace(
                        current_block_entry_step,
                        BlockEntryStep::Done,
                    );
                    match block_entry_step {
                        BlockEntryStep::TryJump(block_entry_action) => {
                            match &block_entry_action {
                                BlockEntryAction::OnlyJump(jump_block_ref) |
                                BlockEntryAction::JumpAndEntry { jump: jump_block_ref, .. } =>
                                    self.inner.state = State::AwaitBlock {
                                        search_range: current_level.search_range.clone(),
                                        block_ref: jump_block_ref.clone(),
                                    },
                                BlockEntryAction::OnlyEntry { .. } =>
                                    (),
                            }
                            *current_block_entry_step = BlockEntryStep::TryStep(block_entry_action);
                        },
                        BlockEntryStep::TryStep(block_entry_action) => {
                            match block_entry_action {
                                BlockEntryAction::OnlyEntry { key, value_cell, } |
                                BlockEntryAction::JumpAndEntry { key, value_cell, .. } =>
                                    return Ok(Kont::ItemFound(KontItemFound {
                                        item: kv::KeyValuePair { key, value_cell, },
                                        next: KontItemFoundNext {
                                            inner: self.inner,
                                        },
                                    })),
                                BlockEntryAction::OnlyJump(..) =>
                                    (),
                            }
                        },
                        BlockEntryStep::Done =>
                            current_level.block_entry_steps_index += 1,
                    }
                },

            }
        }
    }
}

impl KontRequireBlockNext {
    pub fn block_arrived(self, block_bytes: Bytes) -> Result<Kont, Error> {
        let walker = WalkerCps {
            inner: Inner {
                state: State::BlockReceived {
                    search_range: self.search_range,
                    block_ref: self.block_ref,
                    block_bytes,
                },
                ..self.inner
            },
        };
        walker.step()
    }
}

impl KontItemFoundNext {
    pub fn item_received(self) -> Result<Kont, Error> {
        WalkerCps { inner: self.inner, }.step()
    }
}
