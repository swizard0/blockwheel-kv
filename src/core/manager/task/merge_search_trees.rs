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

use crate::{
    kv,
    wheels,
    core::{
        search_tree,
        BlockRef,
    },
};

pub struct Args {
    pub search_tree_a_ref: Ref,
    pub search_tree_b_ref: Ref,
    pub search_tree_a_pid: search_tree::Pid,
    pub search_tree_b_pid: search_tree::Pid,
    pub wheels_pid: wheels::Pid,
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
}

pub async fn run(
    Args {
        search_tree_a_ref,
        search_tree_b_ref,
        mut search_tree_a_pid,
        mut search_tree_b_pid,
        wheels_pid,
    }: Args,
)
    -> Result<Done, Error>
{
    let mut merge_iter = MergeIter::new(
        search_tree_a_ref,
        search_tree_b_ref,
        &mut search_tree_a_pid,
        &mut search_tree_b_pid,
    ).await?;

    let mut items_count = 0;
    while let Some(..) = merge_iter.next().await {
        items_count += 1;
    }

    let root_block = unimplemented!();

    Ok(Done {
        search_tree_a_ref,
        search_tree_b_ref,
        root_block,
        items_count,
    })
}

struct MergeIter {
    items_a_rx: mpsc::Receiver<kv::KeyValuePair>,
    items_b_rx: mpsc::Receiver<kv::KeyValuePair>,
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

    async fn next(&mut self) -> Option<kv::KeyValuePair> {
        match (&mut self.maybe_item_a, &mut self.maybe_item_b) {
            (None, None) =>
                None,
            (item_a @ Some(..), None) => {
                let value = item_a.take();
                self.maybe_item_a = self.items_a_rx.next().await;
                value
            },
            (None, item_b @ Some(..)) => {
                let value = item_b.take();
                self.maybe_item_b = self.items_b_rx.next().await;
                value
            },
            (
                Some(kv::KeyValuePair { key: key_a, value_cell: value_cell_a, }),
                Some(kv::KeyValuePair { key: key_b, value_cell: value_cell_b, }),
            ) =>
                match key_a.key_bytes.cmp(&key_b.key_bytes) {
                    Ordering::Less => {
                        let next_item_a = self.items_a_rx.next().await;
                        mem::replace(&mut self.maybe_item_a, next_item_a)
                    },
                    Ordering::Equal =>
                        match value_cell_a.version.cmp(&value_cell_b.version) {
                            Ordering::Less => {
                                self.maybe_item_a = self.items_a_rx.next().await;
                                let next_item_b = self.items_b_rx.next().await;
                                mem::replace(&mut self.maybe_item_b, next_item_b)
                            },
                            Ordering::Equal | Ordering::Greater => {
                                self.maybe_item_b = self.items_b_rx.next().await;
                                let next_item_a = self.items_a_rx.next().await;
                                mem::replace(&mut self.maybe_item_a, next_item_a)
                            },
                        },
                    Ordering::Greater => {
                        let next_item_b = self.items_b_rx.next().await;
                        mem::replace(&mut self.maybe_item_b, next_item_b)
                    },
                },
        }
    }
}

async fn iter_and_fetch(
    search_tree_ref: Ref,
    search_tree_pid: &mut search_tree::Pid,
)
    -> Result<(mpsc::Receiver<kv::KeyValuePair>, Option<kv::KeyValuePair>), Error>
{
    let search_tree::SearchTreeIterRx { mut items_rx, } = search_tree_pid.iter().await
        .map_err(|error| Error::SearchTreeIter { search_tree_ref, error, })?;
    let maybe_item = items_rx.next().await;
    Ok((items_rx, maybe_item))
}
