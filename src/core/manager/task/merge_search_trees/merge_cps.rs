use futures::{
    channel::{
        mpsc,
    },
};

use o1::{
    set::{
        Ref,
    },
};

use alloc_pool::{
    pool,
    bytes::{
        Bytes,
        BytesPool,
    },
    Unique,
};

use crate::{
    kv,
    storage,
    core::{
        manager::{
            task::{
                merge_search_trees,
            },
        },
        merger,
        search_tree,
        KeyValueRef,
    },
};

pub struct Env {
    pub await_iters: Vec<merger::KeyValuesIterRx>,
    pub await_outcomes: Vec<AwaitOutcome>,
    pub tasks_orders: Vec<merge_search_trees::task::Task>,
    pub conf: Conf,
}

pub struct Conf {
    pub search_tree_a_ref: Ref,
    pub search_tree_b_ref: Ref,
    pub search_tree_a_pid: search_tree::Pid,
    pub search_tree_b_pid: search_tree::Pid,
    pub merger_iters_pool: pool::Pool<Vec<merger::KeyValuesIterRx>>,
}

pub struct JobArgs {
    pub env: Env,
    pub kont: Kont,
}

pub enum Kont {
    StartRemoveAndCount {
        items_a_rx: merger::KeyValuesIterRx,
        items_b_rx: merger::KeyValuesIterRx,
    },
    AwaitReady(KontAwaitReady),
}

pub struct KontAwaitReady {
    pub next: merger::KontAwaitScheduledNext<Unique<Vec<merger::KeyValuesIterRx>>, merger::KeyValuesIterRx>,
}

pub struct JobDone {
    pub env: Env,
    pub done: Done,
}

pub enum Done {
    AwaitIters {
        next: merger::KontAwaitScheduledNext<Unique<Vec<merger::KeyValuesIterRx>>, merger::KeyValuesIterRx>,
    },
    Finish,
}

pub type JobOutput = JobDone;

pub struct AwaitOutcome {
    pub iter: merger::KeyValuesIterRx,
    pub item: KeyValueRef,
}

pub fn job(JobArgs { mut env, kont, }: JobArgs) -> JobOutput {
    assert!(env.await_iters.is_empty());

    let mut merger_kont = match kont {
        Kont::StartRemoveAndCount { items_a_rx, items_b_rx, } => {
            let mut iters = env.conf.merger_iters_pool.lend(Vec::new);
            iters.clear();
            iters.push(items_a_rx);
            iters.push(items_b_rx);
            iters.shrink_to_fit();
            merger::ItersMergerCps::new(iters).step()
        },
        Kont::AwaitReady(KontAwaitReady { next, }) => {
            let await_outcome = env.await_outcomes.pop().unwrap();
            next.proceed_with_item(await_outcome.iter, await_outcome.item)
        },
    };


    todo!()
}
