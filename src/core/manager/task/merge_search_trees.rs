use std::{
    mem,
    cmp::{
        Ordering,
    },
};

use futures::{
    future,
    channel::{
        mpsc,
    },
    StreamExt,
};

use o1::set::Ref;

use bntree::{
    sketch,
    writer::{
        plan,
        fold,
    },
};

use alloc_pool::bytes::{
    BytesPool,
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
    },
    KeyValueStreamItem,
};

pub struct Args {
    pub search_tree_a_ref: Ref,
    pub search_tree_b_ref: Ref,
    pub search_tree_a_pid: search_tree::Pid,
    pub search_tree_b_pid: search_tree::Pid,
    pub blocks_pool: BytesPool,
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
    BackendIterPeerLost,
    Merger(merger::Error),
}

pub async fn run(
    Args {
        search_tree_a_ref,
        search_tree_b_ref,
        mut search_tree_a_pid,
        mut search_tree_b_pid,
        blocks_pool,
        mut wheels_pid,
        tree_block_size,
    }: Args,
)
    -> Result<Done, Error>
{
    let mut tree_items_count = 0;

    let (items_a_rx, items_b_rx) = future::try_join(
        async {
            let search_tree::SearchTreeIterItemsRx { items_rx, } = search_tree_a_pid.iter().await
                .map_err(|error| Error::SearchTreeIter { search_tree_ref: search_tree_a_ref.clone(), error, })?;
            Ok(items_rx)
        },
        async {
            let search_tree::SearchTreeIterItemsRx { items_rx, } = search_tree_b_pid.iter().await
                .map_err(|error| Error::SearchTreeIter { search_tree_ref: search_tree_b_ref.clone(), error, })?;
            Ok(items_rx)
        },
    ).await?;
    let mut merger = merger::ItersMerger::new(vec![
        merger::KeyValuesIter::new(items_a_rx),
        merger::KeyValuesIter::new(items_b_rx),
    ]);
    while let Some(..) = merger.next().await.map_err(Error::Merger)? {
        tree_items_count += 1;
    }

    // let mut merge_iter = MergeIter::new(
    //     search_tree_a_ref,
    //     search_tree_b_ref,
    //     &mut search_tree_a_pid,
    //     &mut search_tree_b_pid,
    // ).await?;
    // while let Some(..) = merge_iter.next().await? {
    //     tree_items_count += 1;
    // }

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

    let mut merge_iter = MergeIter::new(
        search_tree_a_ref,
        search_tree_b_ref,
        &mut search_tree_a_pid,
        &mut search_tree_b_pid,
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
                let kv::KeyValuePair { ref key, ref value_cell, } = merge_iter.next().await?
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
                let block_serializer_kont =
                    block_serializer.entry(
                        &key.key_bytes,
                        value_cell.into(),
                        jump_ref,
                    )
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
    assert_eq!(merge_iter.next().await?, None);

    Ok(Done {
        search_tree_a_ref,
        search_tree_b_ref,
        root_block,
        items_count: tree_items_count,
    })
}

struct MergeIter {
    items_a_rx: mpsc::Receiver<KeyValueStreamItem>,
    items_b_rx: mpsc::Receiver<KeyValueStreamItem>,
    maybe_item_a: Option<kv::KeyValuePair>,
    maybe_item_b: Option<kv::KeyValuePair>,
}

impl MergeIter {
    async fn new(
        search_tree_a_ref: Ref,
        search_tree_b_ref: Ref,
        search_tree_a_pid: &mut search_tree::Pid,
        search_tree_b_pid: &mut search_tree::Pid,
    )
        -> Result<MergeIter, Error>
    {
        let ((items_a_rx, maybe_item_a), (items_b_rx, maybe_item_b)) =
            future::try_join(
                iter_and_fetch(search_tree_a_ref, search_tree_a_pid),
                iter_and_fetch(search_tree_b_ref, search_tree_b_pid),
            )
            .await?;

        Ok(MergeIter {
            items_a_rx,
            items_b_rx,
            maybe_item_a,
            maybe_item_b,
        })
    }

    async fn next(&mut self) -> Result<Option<kv::KeyValuePair>, Error> {
        Ok(match (&mut self.maybe_item_a, &mut self.maybe_item_b) {
            (None, None) =>
                None,
            (item_a @ Some(..), None) => {
                let value = item_a.take();
                self.maybe_item_a = fetch_next(&mut self.items_a_rx).await?;
                value
            },
            (None, item_b @ Some(..)) => {
                let value = item_b.take();
                self.maybe_item_b = fetch_next(&mut self.items_b_rx).await?;
                value
            },
            (
                Some(kv::KeyValuePair { key: key_a, value_cell: value_cell_a, }),
                Some(kv::KeyValuePair { key: key_b, value_cell: value_cell_b, }),
            ) =>
                match key_a.key_bytes.cmp(&key_b.key_bytes) {
                    Ordering::Less => {
                        let next_item_a = fetch_next(&mut self.items_a_rx).await?;
                        mem::replace(&mut self.maybe_item_a, next_item_a)
                    },
                    Ordering::Equal =>
                        match value_cell_a.version.cmp(&value_cell_b.version) {
                            Ordering::Less => {
                                self.maybe_item_a = fetch_next(&mut self.items_a_rx).await?;
                                let next_item_b = fetch_next(&mut self.items_b_rx).await?;
                                mem::replace(&mut self.maybe_item_b, next_item_b)
                            },
                            Ordering::Equal | Ordering::Greater => {
                                self.maybe_item_b = fetch_next(&mut self.items_b_rx).await?;
                                let next_item_a = fetch_next(&mut self.items_a_rx).await?;
                                mem::replace(&mut self.maybe_item_a, next_item_a)
                            },
                        },
                    Ordering::Greater => {
                        let next_item_b = fetch_next(&mut self.items_b_rx).await?;
                        mem::replace(&mut self.maybe_item_b, next_item_b)
                    },
                },
        })
    }
}

async fn iter_and_fetch(
    search_tree_ref: Ref,
    search_tree_pid: &mut search_tree::Pid,
)
    -> Result<(mpsc::Receiver<KeyValueStreamItem>, Option<kv::KeyValuePair>), Error>
{
    let search_tree::SearchTreeIterItemsRx { mut items_rx, } = search_tree_pid.iter().await
        .map_err(|error| Error::SearchTreeIter { search_tree_ref, error, })?;
    let maybe_item = fetch_next(&mut items_rx).await?;
    Ok((items_rx, maybe_item))
}

async fn fetch_next(items_rx: &mut mpsc::Receiver<KeyValueStreamItem>) -> Result<Option<kv::KeyValuePair>, Error> {
    match items_rx.next().await {
        None =>
            Err(Error::BackendIterPeerLost),
        Some(KeyValueStreamItem::NoMore) =>
            Ok(None),
        Some(KeyValueStreamItem::KeyValue(kv)) =>
            Ok(Some(kv)),
    }
}
