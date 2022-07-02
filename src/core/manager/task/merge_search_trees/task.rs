use futures::{
    channel::{
        mpsc,
    },
};

use crate::{
    core::{
        manager::{
            task::{
                merge_search_trees,
            },
        },
        KeyValueRef,
    },
    wheels,
};

pub enum Task {
    MergeCps {
        job_handle: edeltraud::Handle<merge_search_trees::merge_cps::JobOutput>,
    },
    AwaitIterRx {
        await_iter_rx: mpsc::Receiver<KeyValueRef>,
    },
    RemoveValueBlock {
        block_ref: wheels::BlockRef,
    },
}
