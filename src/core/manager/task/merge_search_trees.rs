
use o1::set::Ref;

use bntree::{
    sketch,
    writer::{
        plan,
        fold,
    },
};

use alloc_pool::{
    pool,
    bytes::{
        BytesPool,
    },
    Unique,
};

use crate::{
    kv,
    wheels,
    storage,
    blockwheel,
    core::{
        merger,
        search_tree,
        BlockRef,
        SearchRangeBounds,
    },
};

pub struct Args {
    pub search_tree_a_ref: Ref,
    pub search_tree_b_ref: Ref,
    pub search_tree_a_pid: search_tree::Pid,
    pub search_tree_b_pid: search_tree::Pid,
    pub blocks_pool: BytesPool,
    pub merger_iters_pool: pool::Pool<Vec<merger::KeyValuesIter>>,
    pub wheels_pid: wheels::Pid,
    pub tree_block_size: usize,
}

pub struct Done {
    pub search_tree_a_ref: Ref,
    pub search_tree_b_ref: Ref,
    pub root_block: BlockRef,
    pub items_count: usize,
}

#[derive(Debug)]
pub enum Error {
    SearchTreeIter {
        search_tree_ref: Ref,
        error: search_tree::IterError,
    },
    WheelsGone,
    WheelsEmpty,
    BuildTree(fold::Error),
    BuildTreeUnexpectedLevelSeedOnVisitBlockStart,
    BuildTreeUnexpectedLevelSeedOnVisitItem,
    BuildTreeUnexpectedSerializeDoneOnVisitItem,
    BuildTreeUnexpectedLevelSeedOnVisitBlockFinish,
    BuildTreeUnexpectedSerializeContinueOnVisitBlockFinish,
    BuildTreeUnexpectedChildRefOnVisitBlockFinish {
        level_index: usize,
        block_index: usize,
    },
    BuildTreeUnexpectedEmptyTree,
    BuildTreeMergeIterDepleted,
    BlockSerializerStart(storage::Error),
    BlockSerializerEntry(storage::Error),
    WriteBlock(blockwheel::WriteBlockError),
    Merger(merger::Error),
}

pub async fn run(
    Args {
        search_tree_a_ref,
        search_tree_b_ref,
        mut search_tree_a_pid,
        mut search_tree_b_pid,
        blocks_pool,
        merger_iters_pool,
        mut wheels_pid,
        tree_block_size,
    }: Args,
)
    -> Result<Done, Error>
{
    let mut tree_items_count = 0;

    let mut merger = merger_start(
        search_tree_a_ref,
        search_tree_b_ref,
        &mut search_tree_a_pid,
        &mut search_tree_b_pid,
        &merger_iters_pool,
    ).await?;

    while let Some(..) = merger.next().await.map_err(Error::Merger)? {
        tree_items_count += 1;
    }

    let sketch = sketch::Tree::new(tree_items_count, tree_block_size);
    let mut fold_ctx = fold::Context::new(
        plan::Context::new(&sketch),
        &sketch,
    );

    enum LevelSeed<B> {
        Empty,
        BlockInProgress {
            block_serializer_kont: storage::BlockSerializerContinue<B>,
            wheel_ref: wheels::WheelRef,
        },
    }

    let mut merger = merger_start(
        search_tree_a_ref,
        search_tree_b_ref,
        &mut search_tree_a_pid,
        &mut search_tree_b_pid,
        &merger_iters_pool,
    ).await?;

    let mut child_ref = None;

    let mut kont = fold::Script::boot();
    let root_block = loop {
        let kont_step = kont.step_rec(&mut fold_ctx)
            .map_err(Error::BuildTree)?;
        kont = match kont_step {
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
                let wheel_ref = wheels_pid.acquire().await
                    .map_err(|ero::NoProcError| Error::WheelsGone)?
                    .ok_or(Error::WheelsEmpty)?;
                let node_type = if level_index == 0 {
                    storage::NodeType::Root { tree_entries_count: tree_items_count, }
                } else {
                    storage::NodeType::Leaf
                };
                let block_bytes = blocks_pool.lend();
                let block_serializer_kont = storage::BlockSerializer::start(node_type, items_count, block_bytes)
                    .map_err(Error::BlockSerializerStart)?;
                let level_seed = LevelSeed::BlockInProgress {
                    block_serializer_kont,
                    wheel_ref,
                };
                next.block_ready(level_seed, &mut fold_ctx)
                    .map_err(Error::BuildTree)?
            },
            fold::Instruction::Op(fold::Op::VisitBlockStart(fold::VisitBlockStart { .. })) =>
                return Err(Error::BuildTreeUnexpectedLevelSeedOnVisitBlockStart),

            // VisitItem
            fold::Instruction::Op(fold::Op::VisitItem(fold::VisitItem {
                level_seed: LevelSeed::BlockInProgress {
                    block_serializer_kont: storage::BlockSerializerContinue::More(block_serializer),
                    wheel_ref,
                },
                next,
                ..
            })) => {
                let kv::KeyValuePair { ref key, ref value_cell, } = merger.next().await
                    .map_err(Error::Merger)?
                    .ok_or(Error::BuildTreeMergeIterDepleted)?;
                let child_ref_taken = child_ref.take();
                let jump_ref = match &child_ref_taken {
                    None =>
                        storage::JumpRef::None,
                    Some(BlockRef { blockwheel_filename, block_id, }) if blockwheel_filename == &wheel_ref.blockwheel_filename =>
                        storage::JumpRef::Local(storage::LocalJumpRef { block_id: block_id.clone(), }),
                    Some(BlockRef { blockwheel_filename, block_id, }) =>
                        storage::JumpRef::External(storage::ExternalJumpRef {
                            filename: blockwheel_filename,
                            block_id: block_id.clone(),
                        }),
                };
                let block_serializer_kont = block_serializer.entry(&key, &value_cell, jump_ref)
                    .map_err(Error::BlockSerializerEntry)?;
                let level_seed = LevelSeed::BlockInProgress {
                    block_serializer_kont,
                    wheel_ref,
                };
                next.item_ready(level_seed, &mut fold_ctx)
                    .map_err(Error::BuildTree)?
            },
            fold::Instruction::Op(fold::Op::VisitItem(fold::VisitItem { level_seed: LevelSeed::Empty, .. })) =>
                return Err(Error::BuildTreeUnexpectedLevelSeedOnVisitItem),
            fold::Instruction::Op(fold::Op::VisitItem(fold::VisitItem {
                level_seed: LevelSeed::BlockInProgress {
                    block_serializer_kont: storage::BlockSerializerContinue::Done(..),
                    ..
                },
                ..
            })) =>
                return Err(Error::BuildTreeUnexpectedSerializeDoneOnVisitItem),

            // VisitBlockFinish
            fold::Instruction::Op(fold::Op::VisitBlockFinish(fold::VisitBlockFinish {
                level_seed: LevelSeed::BlockInProgress {
                    block_serializer_kont: storage::BlockSerializerContinue::Done(block_bytes),
                    mut wheel_ref,
                },
                level_index,
                block_index,
                next,
                ..
            })) => {
                if let Some(..) = child_ref {
                    return Err(Error::BuildTreeUnexpectedChildRefOnVisitBlockFinish { level_index, block_index, });
                }

                let block_id = wheel_ref.blockwheel_pid.write_block(block_bytes.freeze()).await
                    .map_err(Error::WriteBlock)?;
                child_ref = Some(BlockRef {
                    blockwheel_filename: wheel_ref.blockwheel_filename,
                    block_id,
                });
                let level_seed = LevelSeed::Empty;
                next.block_flushed(level_seed, &mut fold_ctx)
                    .map_err(Error::BuildTree)?
            },
            fold::Instruction::Op(fold::Op::VisitBlockFinish(fold::VisitBlockFinish { level_seed: LevelSeed::Empty, .. })) =>
                return Err(Error::BuildTreeUnexpectedLevelSeedOnVisitBlockFinish),
            fold::Instruction::Op(fold::Op::VisitBlockFinish(fold::VisitBlockFinish {
                level_seed: LevelSeed::BlockInProgress {
                    block_serializer_kont: storage::BlockSerializerContinue::More(..),
                    ..
                },
                ..
            })) =>
                return Err(Error::BuildTreeUnexpectedSerializeContinueOnVisitBlockFinish),

            // Done
            fold::Instruction::Done =>
                match child_ref {
                    Some(root_block_ref) =>
                        break root_block_ref,
                    None =>
                        return Err(Error::BuildTreeUnexpectedEmptyTree),
                },
        };
    };
    assert_eq!(merger.next().await.map_err(Error::Merger)?, None);

    Ok(Done {
        search_tree_a_ref,
        search_tree_b_ref,
        root_block,
        items_count: tree_items_count,
    })
}

async fn merger_start(
    search_tree_a_ref: Ref,
    search_tree_b_ref: Ref,
    search_tree_a_pid: &mut search_tree::Pid,
    search_tree_b_pid: &mut search_tree::Pid,
    merger_iters_pool: &pool::Pool<Vec<merger::KeyValuesIter>>,
)
    -> Result<merger::ItersMerger<Unique<Vec<merger::KeyValuesIter>>>, Error>
{
    let (items_a_rx, items_b_rx) = futures::future::try_join(
        async {
            let search_tree::SearchTreeIterItemsRx { items_rx, } = search_tree_a_pid.iter(SearchRangeBounds::unbounded()).await
                .map_err(|error| Error::SearchTreeIter { search_tree_ref: search_tree_a_ref.clone(), error, })?;
            Ok(items_rx)
        },
        async {
            let search_tree::SearchTreeIterItemsRx { items_rx, } = search_tree_b_pid.iter(SearchRangeBounds::unbounded()).await
                .map_err(|error| Error::SearchTreeIter { search_tree_ref: search_tree_b_ref.clone(), error, })?;
            Ok(items_rx)
        },
    ).await?;

    let mut iters = merger_iters_pool.lend(Vec::new);
    iters.clear();
    iters.push(merger::KeyValuesIter::new(items_a_rx));
    iters.push(merger::KeyValuesIter::new(items_b_rx));

    Ok(merger::ItersMerger::new(iters))
}
