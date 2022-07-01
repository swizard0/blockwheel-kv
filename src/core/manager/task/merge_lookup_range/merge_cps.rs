use alloc_pool::{
    Unique,
    Shared,
};

use futures::{
    channel::{
        mpsc,
    },
};

use crate::{
    kv,
    storage,
    core::{
        merger,
        KeyValueRef,
    },
};

#[derive(Default)]
pub struct Env {
    pub ready_items: Vec<kv::KeyValuePair<storage::OwnedValueBlockRef>>,
    pub await_butcher_iters: Vec<ButcherJoinedIter>,
    pub await_search_tree_iters: Vec<mpsc::Receiver<KeyValueRef>>,
    pub await_outcomes: Vec<AwaitOutcome>,
}

pub struct JobArgs {
    pub env: Env,
    pub kont: Kont,
}

pub enum Kont {
    Start {
        butcher_iter_items: Shared<Vec<kv::KeyValuePair<kv::Value>>>,
        merger_iters: Unique<Vec<mpsc::Receiver<KeyValueRef>>>,
    },
    AwaitReady(KontAwaitReady),
}

pub struct KontAwaitReady {
    pub next: merger::KontAwaitScheduledNext<JoinedIters, JoinedIter>,
}

pub struct JobDone {
    pub env: Env,
    pub done: Done,
}

pub enum Done {
    AwaitIters {
        next: merger::KontAwaitScheduledNext<JoinedIters, JoinedIter>,
    },
    Finish,
}

pub type JobOutput = JobDone;

pub struct AwaitOutcome {
    pub iter: mpsc::Receiver<KeyValueRef>,
    pub item: KeyValueRef,
}

pub struct JoinedIters {
    iters: Vec<JoinedIter>,
}

pub enum JoinedIter {
    Butcher(ButcherJoinedIter),
    SearchTree(mpsc::Receiver<KeyValueRef>),
}

pub struct ButcherJoinedIter {
    items: Shared<Vec<kv::KeyValuePair<kv::Value>>>,
    index: usize,
}

pub fn job(JobArgs { mut env, kont, }: JobArgs) -> JobOutput {
    assert!(env.ready_items.is_empty());
    assert!(env.await_butcher_iters.is_empty());
    assert!(env.await_search_tree_iters.is_empty());

    let mut merger_kont = match kont {
        Kont::Start { butcher_iter_items, mut merger_iters, } => {
            let mut iters = Vec::with_capacity(1 + merger_iters.len());
            iters.push(JoinedIter::Butcher(ButcherJoinedIter {
                items: butcher_iter_items,
                index: 0,
            }));
            iters.extend(merger_iters.drain(..).map(JoinedIter::SearchTree));
            merger::ItersMergerCps::new(JoinedIters { iters, })
                .step()
        },
        Kont::AwaitReady(KontAwaitReady { next, }) => {
            let await_outcome = env.await_outcomes.pop().unwrap();
            next.proceed_with_item(JoinedIter::SearchTree(await_outcome.iter), await_outcome.item)
        },
    };

    loop {
        merger_kont = match merger_kont {
            merger::Kont::ScheduleIterAwait { await_iter, next, } => {
                match await_iter {
                    JoinedIter::Butcher(await_butcher_iter) =>
                        env.await_butcher_iters.push(await_butcher_iter),
                    JoinedIter::SearchTree(await_search_tree_iter) =>
                        env.await_search_tree_iters.push(await_search_tree_iter),
                }
                next.proceed()
            },
            merger::Kont::AwaitScheduled { next, } => {
                if let Some(await_outcome) = env.await_outcomes.pop() {
                    next.proceed_with_item(JoinedIter::SearchTree(await_outcome.iter), await_outcome.item)
                } else if let Some(mut await_butcher_iter) = env.await_butcher_iters.pop() {
                    let item = if let Some(key_value) = await_butcher_iter.items.get(await_butcher_iter.index) {
                        await_butcher_iter.index += 1;
                        KeyValueRef::Item {
                            key: key_value.key.clone(),
                            value_cell: key_value.value_cell
                                .clone()
                                .into(),
                        }
                    } else {
                        KeyValueRef::NoMore
                    };
                    next.proceed_with_item(JoinedIter::Butcher(await_butcher_iter), item)
                } else {
                    assert!(!env.await_search_tree_iters.is_empty());
                    return JobDone { env, done: Done::AwaitIters { next, }, };
                }
            },
            merger::Kont::Deprecated { next, .. } => {
                assert!(env.await_outcomes.is_empty());
                next.proceed()
            },
            merger::Kont::Item { item, next, } => {
                assert!(env.await_outcomes.is_empty());
                env.ready_items.push(item);
                next.proceed()
            },
            merger::Kont::Done => {
                assert!(env.await_outcomes.is_empty());
                return JobDone { env, done: Done::Finish, };
            },
        }
    }
}

impl std::ops::Deref for JoinedIters {
    type Target = Vec<JoinedIter>;

    fn deref(&self) -> &Self::Target {
        &self.iters
    }
}

impl std::ops::DerefMut for JoinedIters {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.iters
    }
}
