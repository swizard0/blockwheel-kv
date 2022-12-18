use std::{
    mem,
};

use bntree::{
    sketch,
    writer::{
        plan,
        fold,
    },
};

use o1::{
    set::{
        Set,
        Ref,
    },
};

use crate::{
    storage,
    core::{
        VecW,
    },
};

#[cfg(test)]
mod tests;

#[derive(Clone, Copy)]
pub struct Params {
    pub tree_items_count: usize,
    pub tree_block_size: usize,
}

pub struct BuilderCps<T, R> {
    inner: Inner<T, R>,
}

pub enum Kont<T, R> {
    PollNextItemOrProcessedBlock(KontPollNextItemOrProcessedBlock<T, R>),
    PollProcessedBlock(KontPollProcessedBlock<T, R>),
    ProcessBlockAsync(KontProcessBlockAsync<T, R>),
    Finished {
        items_count: usize,
        root_block_ref: R,
    },
}

#[derive(Debug)]
pub enum Error {
    BuildTree(fold::Error),
    BuildTreeUnexpectedLevelSeedOnVisitBlockStart,
    BuildTreeUnexpectedLevelSeedOnVisitItem,
    BuildTreeUnexpectedLevelSeedOnVisitBlockFinish,
    BuildTreeUnexpectedChildRefOnVisitBlockFinish { level_index: usize, block_index: usize, },
    BuildTreeUnexpectedEmptyTree,
    BlockProcessedInvalidAsyncRef { async_ref: Ref, },
}

struct Inner<T, R> {
    state: State<T, R>,
    done_blocks: Set<DoneBlock<T, R>>,
    arrived_item: Option<T>,
    ready_head: Option<Ref>,
    params: Params,
}

enum State<T, R> {
    Init,
    Building {
        fold_ctx: fold::Context<LevelSeed<T, R>>,
        tree_kont: fold::Continue,
        child_block_ref: Option<Ref>,
    },
    AwaitingItem {
        fold_ctx: fold::Context<LevelSeed<T, R>>,
        child_block_ref: Option<Ref>,
        block_in_progress: Block<T, R>,
        next: fold::VisitItemNext,
    },
    Finished {
        root_block_ref: Ref,
    },
}

pub struct KontPollNextItemOrProcessedBlock<T, R> {
    pub next: KontPollNextItemOrProcessedBlockNext<T, R>,
}

pub struct KontPollNextItemOrProcessedBlockNext<T, R> {
    inner: Inner<T, R>,
}

pub struct KontPollProcessedBlock<T, R> {
    pub next: KontPollProcessedBlockNext<T, R>,
}

pub struct KontPollProcessedBlockNext<T, R> {
    inner: Inner<T, R>,
}

pub struct KontProcessBlockAsync<T, R> {
    pub node_type: storage::NodeType,
    pub block_entries: VecW<BlockEntry<T, R>>,
    pub async_ref: Ref,
    pub next: KontProcessBlockAsyncNext<T, R>,
}

pub struct KontProcessBlockAsyncNext<T, R> {
    inner: Inner<T, R>,
}

enum LevelSeed<T, R> {
    Empty,
    InProgress(Block<T, R>),
}

struct Block<T, R> {
    node_type: storage::NodeType,
    block_entries: VecW<BlockEntry<T, R>>,
    child_refs_in_progress: usize,
    ready_next: Option<Ref>,
}

struct DoneBlock<T, R> {
    parent_block_ref: Option<Ref>,
    kind: DoneBlockKind<T, R>,
}

enum DoneBlockKind<T, R> {
    Issued(Block<T, R>),
    Processing,
    RefReady(R),
}

#[derive(Debug)]
pub struct BlockEntry<T, R> {
    pub child_block_ref: Option<R>,
    pub item: T,
    done_blocks_child_ref: Option<Ref>,
}

impl<T, R> BuilderCps<T, R> {
    pub fn new(
        params: Params,
    )
        -> Self
    {
        Self {
            inner: Inner {
                state: State::Init,
                done_blocks: Set::new(),
                arrived_item: None,
                ready_head: None,
                params,
            },
        }
    }

    pub fn step(mut self) -> Result<Kont<T, R>, Error> {
        loop {
            if let Some(ready_ref) = self.inner.ready_head {
                let ready_block_kind = mem::replace(
                    &mut self.inner.done_blocks.get_mut(ready_ref).unwrap().kind,
                    DoneBlockKind::Processing,
                );
                match ready_block_kind {
                    DoneBlockKind::Issued(mut block) => {
                        self.inner.ready_head = block.ready_next;
                        for block_entry in &mut **block.block_entries {
                            if let Some(block_ref) = block_entry.done_blocks_child_ref {
                                match self.inner.done_blocks.remove(block_ref) {
                                    None =>
                                        (),
                                    Some(DoneBlock { kind: DoneBlockKind::RefReady(child_block_ref), .. }) =>
                                        block_entry.child_block_ref = Some(child_block_ref),
                                    Some(DoneBlock { kind: DoneBlockKind::Issued(..) | DoneBlockKind::Processing, .. }) =>
                                        unreachable!(),
                                }
                            }
                        }
                        return Ok(Kont::ProcessBlockAsync(KontProcessBlockAsync {
                            node_type: block.node_type,
                            block_entries: block.block_entries,
                            async_ref: ready_ref,
                            next: KontProcessBlockAsyncNext {
                                inner: self.inner,
                            },
                        }));
                    },
                    DoneBlockKind::Processing | DoneBlockKind::RefReady(..) =>
                        unreachable!(),
                }
            }

            match self.inner.state {
                State::Init => {
                    let sketch = sketch::Tree::new(
                        self.inner.params.tree_items_count,
                        self.inner.params.tree_block_size,
                    );
                    let fold_ctx = fold::Context::new(
                        plan::Context::new(&sketch),
                        &sketch,
                    );
                    let tree_kont =
                        fold::Script::boot();
                    self.inner.state = State::Building {
                        fold_ctx,
                        tree_kont,
                        child_block_ref: None,
                    };
                },
                State::Building { mut fold_ctx, mut tree_kont, mut child_block_ref, } => {
                    let kont_step = tree_kont.step_rec(&mut fold_ctx)
                        .map_err(Error::BuildTree)?;
                    tree_kont = match kont_step {

                        // VisitLevel
                        fold::Instruction::Op(fold::Op::VisitLevel(fold::VisitLevel { next, .. })) => {
                            next.level_ready(LevelSeed::Empty, &mut fold_ctx)
                                .map_err(Error::BuildTree)?
                        },

                        // VisitBlockStart
                        fold::Instruction::Op(fold::Op::VisitBlockStart(fold::VisitBlockStart {
                            level_seed: LevelSeed::Empty,
                            level_index,
                            items_count,
                            next,
                            ..
                        })) => {
                            let node_type = if level_index == 0 {
                                storage::NodeType::Root {
                                    tree_entries_count: self.inner.params.tree_items_count,
                                }
                            } else {
                                storage::NodeType::Leaf
                            };
                            let block_entries = VecW::from(Vec::with_capacity(items_count));
                            let level_seed = LevelSeed::InProgress(Block {
                                block_entries,
                                node_type,
                                child_refs_in_progress: 0,
                                ready_next: None,
                            });
                            next.block_ready(level_seed, &mut fold_ctx)
                                .map_err(Error::BuildTree)?
                        },
                        fold::Instruction::Op(fold::Op::VisitBlockStart(fold::VisitBlockStart { .. })) =>
                            return Err(Error::BuildTreeUnexpectedLevelSeedOnVisitBlockStart),

                        // VisitItem
                        fold::Instruction::Op(fold::Op::VisitItem(fold::VisitItem {
                            level_seed: LevelSeed::InProgress(block_in_progress),
                            next,
                            ..
                        })) => {
                            self.inner.state = State::AwaitingItem {
                                fold_ctx,
                                child_block_ref,
                                block_in_progress,
                                next,
                            };
                            continue;
                        },

                        fold::Instruction::Op(fold::Op::VisitItem(fold::VisitItem { level_seed: LevelSeed::Empty, .. })) =>
                            return Err(Error::BuildTreeUnexpectedLevelSeedOnVisitItem),

                        // VisitBlockFinish
                        fold::Instruction::Op(fold::Op::VisitBlockFinish(fold::VisitBlockFinish {
                            level_seed: LevelSeed::InProgress(mut block_in_progress),
                            level_index,
                            block_index,
                            next,
                            ..
                        })) => {
                            if let Some(..) = child_block_ref {
                                return Err(Error::BuildTreeUnexpectedChildRefOnVisitBlockFinish { level_index, block_index, });
                            }

                            // allocate space in `done_blocks` for `block_in_progress`
                            let done_blocks_ref = self.inner.done_blocks.insert(
                                DoneBlock {
                                    parent_block_ref: None,
                                    kind: DoneBlockKind::Processing,
                                },
                            );

                            block_in_progress.block_entries.shrink_to_fit();
                            for block_entry in &**block_in_progress.block_entries {
                                if let Some(block_ref) = block_entry.done_blocks_child_ref {
                                    let done_child_block = self.inner.done_blocks.get_mut(block_ref).unwrap();
                                    done_child_block.parent_block_ref = Some(done_blocks_ref);
                                    match done_child_block.kind {
                                        DoneBlockKind::Issued(..) | DoneBlockKind::Processing =>
                                            block_in_progress.child_refs_in_progress += 1,
                                        DoneBlockKind::RefReady(..) =>
                                            (),
                                    }
                                }
                            }
                            if block_in_progress.child_refs_in_progress == 0 {
                                block_in_progress.ready_next = self.inner.ready_head;
                                self.inner.ready_head = Some(done_blocks_ref);
                            }
                            self.inner.done_blocks.get_mut(done_blocks_ref).unwrap().kind =
                                DoneBlockKind::Issued(block_in_progress);

                            child_block_ref = Some(done_blocks_ref);
                            let level_seed = LevelSeed::Empty;
                            next.block_flushed(level_seed, &mut fold_ctx)
                                .map_err(Error::BuildTree)?
                        },
                        fold::Instruction::Op(fold::Op::VisitBlockFinish(fold::VisitBlockFinish { level_seed: LevelSeed::Empty, .. })) =>
                            return Err(Error::BuildTreeUnexpectedLevelSeedOnVisitBlockFinish),

                        // Done
                        fold::Instruction::Done =>
                            match child_block_ref {
                                Some(root_block_ref) => {
                                    self.inner.state = State::Finished { root_block_ref, };
                                    continue;
                                },
                                None =>
                                    return Err(Error::BuildTreeUnexpectedEmptyTree),
                            },
                    };
                    self.inner.state =
                        State::Building { fold_ctx, tree_kont, child_block_ref, };
                },
                State::AwaitingItem {
                    mut fold_ctx,
                    mut child_block_ref,
                    mut block_in_progress,
                    next,
                } =>
                    match self.inner.arrived_item.take() {
                        None =>
                            return Ok(Kont::PollNextItemOrProcessedBlock(KontPollNextItemOrProcessedBlock {
                                next: KontPollNextItemOrProcessedBlockNext {
                                    inner: Inner {
                                        state: State::AwaitingItem { fold_ctx, child_block_ref, block_in_progress, next, },
                                        ..self.inner
                                    },
                                },
                            })),
                        Some(item) => {
                            let child_block_ref_taken = child_block_ref.take();
                            block_in_progress.block_entries.push(BlockEntry {
                                child_block_ref: None,
                                item,
                                done_blocks_child_ref: child_block_ref_taken,
                            });
                            let level_seed = LevelSeed::InProgress(block_in_progress);
                            let tree_kont = next.item_ready(level_seed, &mut fold_ctx)
                                .map_err(Error::BuildTree)?;
                            self.inner.state =
                                State::Building { fold_ctx, tree_kont, child_block_ref, };
                        },
                    },
                State::Finished { root_block_ref, } =>
                    return Ok(Kont::PollProcessedBlock(KontPollProcessedBlock {
                        next: KontPollProcessedBlockNext {
                            inner: Inner {
                                state: State::Finished { root_block_ref, },
                                ..self.inner
                            },
                        },
                    })),
            }
        }
    }
}

impl<T, R> KontPollNextItemOrProcessedBlockNext<T, R> {
    pub fn item_arrived(self, item: T) -> Result<Kont<T, R>, Error> {
        assert!(self.inner.arrived_item.is_none());
        let builder = BuilderCps {
            inner: Inner {
                arrived_item: Some(item),
                ..self.inner
            },
        };
        builder.step()
    }

    pub fn block_processed(self, async_ref: Ref, block_ref: R) -> Result<Kont<T, R>, Error> {
        let map_kont = KontPollProcessedBlockNext { inner: self.inner, };
        map_kont.block_processed(async_ref, block_ref)
    }
}

impl<T, R> KontPollProcessedBlockNext<T, R> {
    pub fn block_processed(mut self, async_ref: Ref, block_ref: R) -> Result<Kont<T, R>, Error> {
        let processing_block = self.inner.done_blocks.get_mut(async_ref);
        match processing_block {
            Some(&mut DoneBlock { parent_block_ref: maybe_parent_block_ref, kind: ref mut kind @ DoneBlockKind::Processing, }) =>
                match maybe_parent_block_ref {
                    Some(parent_block_ref) => {
                        *kind = DoneBlockKind::RefReady(block_ref);
                        let maybe_done_parent_block = self.inner.done_blocks.get_mut(parent_block_ref);
                        if let Some(DoneBlock { kind: DoneBlockKind::Issued(done_parent_block), .. }) = maybe_done_parent_block {
                            assert!(done_parent_block.child_refs_in_progress > 0);
                            done_parent_block.child_refs_in_progress -= 1;
                            if done_parent_block.child_refs_in_progress == 0 {
                                done_parent_block.ready_next = self.inner.ready_head;
                                self.inner.ready_head = Some(parent_block_ref);
                            }
                        }
                        BuilderCps { inner: self.inner, }.step()
                    },
                    None if matches!(self.inner.state, State::Finished { .. }) => {
                        assert_eq!(self.inner.done_blocks.len(), 1);
                        Ok(Kont::Finished {
                            items_count: self.inner.params.tree_items_count,
                            root_block_ref: block_ref,
                        })
                    },
                    None => {
                        *kind = DoneBlockKind::RefReady(block_ref);
                        BuilderCps { inner: self.inner, }.step()
                    },
                },
            _ =>
                Err(Error::BlockProcessedInvalidAsyncRef { async_ref, }),
        }
    }
}

impl<T, R> KontProcessBlockAsyncNext<T, R> {
    pub fn process_scheduled(self) -> Result<Kont<T, R>, Error> {
        BuilderCps { inner: self.inner, }.step()
    }
}
