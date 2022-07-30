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

use alloc_pool::{
    pool,
    Unique,
};

use crate::{
    storage,
};

pub struct Params {
    pub tree_items_count: usize,
    pub tree_block_size: usize,
}

pub struct BuilderCps<T, R> {
    inner: Inner<T, R>,
}

pub enum Kont<T, R> {
    RequestNextItem(KontRequestNextItem<T, R>),
    ProcessBlockAsync(KontProcessBlockAsync<T, R>),
}

#[derive(Debug)]
pub enum Error {
    BuildTree(fold::Error),
    BuildTreeUnexpectedLevelSeedOnVisitBlockStart,
    BuildTreeUnexpectedLevelSeedOnVisitItem,
    BuildTreeUnexpectedLevelSeedOnVisitBlockFinish,
    BuildTreeUnexpectedChildRefOnVisitBlockFinish { level_index: usize, block_index: usize, }
}

struct Inner<T, R> {
    state: State<T, R>,
    block_entries_pool: pool::Pool<Vec<BlockEntry<T, R>>>,
    done_blocks: Set<DoneBlock<T, R>>,
    ready_head: Option<Ref>,
    params: Params,
}

enum State<T, R> {
    Init,
    Building {
        sketch: sketch::Tree,
        fold_ctx: fold::Context<LevelSeed<T, R>>,
        tree_kont: fold::Continue,
        child_block_ref: Option<Ref>,
    },
}

pub struct KontRequestNextItem<T, R> {
    pub next: KontRequestNextItemNext<T, R>,
}

pub struct KontRequestNextItemNext<T, R> {
    inner: Inner<T, R>,
    sketch: sketch::Tree,
    fold_ctx: fold::Context<LevelSeed<T, R>>,
    child_block_ref: Option<Ref>,
    block_in_progress: Block<T, R>,
    next: fold::VisitItemNext,
}

pub struct KontProcessBlockAsync<T, R> {
    pub node_type: storage::NodeType,
    pub block_entries: Unique<Vec<BlockEntry<T, R>>>,
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
    block_entries: Unique<Vec<BlockEntry<T, R>>>,
    child_refs_in_progress: usize,
    ready_next: Option<Ref>,
}

enum DoneBlock<T, R> {
    Issued(Block<T, R>),
    Processing,
    RefReady(R),
}

pub struct BlockEntry<T, R> {
    pub child_block_ref: Option<R>,
    pub item: T,
    done_blocks_child_ref: Option<Ref>,
}

impl<T, R> BuilderCps<T, R> {
    pub fn new(
        block_entries_pool: pool::Pool<Vec<BlockEntry<T, R>>>,
        params: Params,
    )
        -> Self
    {
        Self {
            inner: Inner {
                state: State::Init,
                block_entries_pool,
                done_blocks: Set::new(),
                ready_head: None,
                params,
            },
        }
    }

    pub fn step(mut self) -> Result<Kont<T, R>, Error> {
        loop {
            if let Some(ready_ref) = self.inner.ready_head {
                let ready_block =
                    mem::replace(self.inner.done_blocks.get_mut(ready_ref).unwrap(), DoneBlock::Processing);
                match ready_block {
                    DoneBlock::Issued(mut block) => {
                        self.inner.ready_head = block.ready_next;
                        for block_entry in &mut **block.block_entries {
                            if let Some(block_ref) = block_entry.done_blocks_child_ref {
                                match self.inner.done_blocks.remove(block_ref) {
                                    None =>
                                        (),
                                    Some(DoneBlock::RefReady(child_block_ref)) =>
                                        block_entry.child_block_ref = Some(child_block_ref),
                                    Some(DoneBlock::Issued(..) | DoneBlock::Processing) =>
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
                    DoneBlock::Processing | DoneBlock::RefReady(..) =>
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
                        sketch,
                        fold_ctx,
                        tree_kont,
                        child_block_ref: None,
                    };
                },
                State::Building { sketch, mut fold_ctx, mut tree_kont, mut child_block_ref, } => {
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
                            let mut block_entries = self.inner.block_entries_pool.lend(Vec::new);
                            block_entries.clear();
                            block_entries.reserve(items_count);
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
                        })) =>
                            return Ok(Kont::RequestNextItem(KontRequestNextItem {
                                next: KontRequestNextItemNext {
                                    inner: Inner {
                                        state: State::Init,
                                        ..self.inner
                                    },
                                    sketch,
                                    fold_ctx,
                                    child_block_ref,
                                    block_in_progress,
                                    next,
                                },
                            })),
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

                            for block_entry in &**block_in_progress.block_entries {
                                if let Some(block_ref) = block_entry.done_blocks_child_ref {
                                    match self.inner.done_blocks.get(block_ref) {
                                        None =>
                                            unreachable!(),
                                        Some(DoneBlock::Issued(..) | DoneBlock::Processing) =>
                                            block_in_progress.child_refs_in_progress += 1,
                                        Some(DoneBlock::RefReady(..)) =>
                                            (),
                                    }
                                }
                            }
                            let is_block_ready = if block_in_progress.child_refs_in_progress == 0 {
                                block_in_progress.ready_next = self.inner.ready_head;
                                true
                            } else {
                                false
                            };
                            let done_blocks_ref =
                                self.inner.done_blocks.insert(DoneBlock::Issued(block_in_progress));
                            if is_block_ready {
                                self.inner.ready_head = Some(done_blocks_ref);
                            }

                            child_block_ref = Some(done_blocks_ref);
                            let level_seed = LevelSeed::Empty;
                            next.block_flushed(level_seed, &mut fold_ctx)
                                .map_err(Error::BuildTree)?
                        },
                        fold::Instruction::Op(fold::Op::VisitBlockFinish(fold::VisitBlockFinish { level_seed: LevelSeed::Empty, .. })) =>
                            return Err(Error::BuildTreeUnexpectedLevelSeedOnVisitBlockFinish),

                        // Done
                        fold::Instruction::Done => {

                            todo!()

                            // match child_ref {
                            //     Some(root_block_ref) =>
                            //         break root_block_ref,
                            //     None =>
                            //         return Err(Error::BuildTreeUnexpectedEmptyTree),
                            // },
                        },
                    };
                    self.inner.state =
                        State::Building { sketch, fold_ctx, tree_kont, child_block_ref, };
                },
            }
        }
    }
}

impl<T, R> KontRequestNextItemNext<T, R> {
    pub fn item_arrived(mut self, item: T) -> Result<BuilderCps<T, R>, Error> {
        let child_block_ref_taken = self.child_block_ref.take();
        self.block_in_progress.block_entries.push(BlockEntry {
            child_block_ref: None,
            item,
            done_blocks_child_ref: child_block_ref_taken,
        });
        let level_seed = LevelSeed::InProgress(self.block_in_progress);
        let tree_kont = self.next.item_ready(level_seed, &mut self.fold_ctx)
            .map_err(Error::BuildTree)?;
        let builder = BuilderCps {
            inner: Inner {
                state: State::Building {
                    sketch: self.sketch,
                    fold_ctx: self.fold_ctx,
                    tree_kont,
                    child_block_ref: self.child_block_ref,
                },
                ..self.inner
            },
        };
        Ok(builder)
    }
}

impl<T, R> KontProcessBlockAsyncNext<T, R> {
    pub fn process_scheduled(self) -> BuilderCps<T, R> {
        BuilderCps { inner: self.inner, }
    }
}
