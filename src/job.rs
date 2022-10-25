use std::{
    sync::{
        atomic::{
            Ordering,
            AtomicUsize,
        },
    },
};

use crate::{
    core,
    wheels,
    EchoPolicy,
};

pub enum Job<E> where E: EchoPolicy {
    BlockwheelFs(blockwheel_fs::job::Job<wheels::WheelEchoPolicy<E>>),
    PerformerSklave(core::performer_sklave::SklaveJob<E>),
    LookupRangeMergeSklave(core::performer_sklave::running::lookup_range_merge::SklaveJob<E>),
    FlushButcherSklave(core::performer_sklave::running::flush_butcher::SklaveJob<E>),
    MergeSearchTreesSklave(core::performer_sklave::running::merge_search_trees::SklaveJob<E>),
    DemolishSearchTreeSklave(core::performer_sklave::running::demolish_search_tree::SklaveJob<E>),
}

impl<E> From<blockwheel_fs::job::Job<wheels::WheelEchoPolicy<E>>> for Job<E> where E: EchoPolicy {
    fn from(job: blockwheel_fs::job::Job<wheels::WheelEchoPolicy<E>>) -> Job<E> {
        Job::BlockwheelFs(job)
    }
}

impl<E> From<core::performer_sklave::SklaveJob<E>> for Job<E> where E: EchoPolicy {
    fn from(sklave_job: core::performer_sklave::SklaveJob<E>) -> Job<E> {
        Job::PerformerSklave(sklave_job)
    }
}

impl<E> From<core::performer_sklave::running::lookup_range_merge::SklaveJob<E>> for Job<E> where E: EchoPolicy {
    fn from(sklave_job: core::performer_sklave::running::lookup_range_merge::SklaveJob<E>) -> Job<E> {
        Job::LookupRangeMergeSklave(sklave_job)
    }
}

impl<E> From<core::performer_sklave::running::flush_butcher::SklaveJob<E>> for Job<E> where E: EchoPolicy {
    fn from(sklave_job: core::performer_sklave::running::flush_butcher::SklaveJob<E>) -> Job<E> {
        Job::FlushButcherSklave(sklave_job)
    }
}

impl<E> From<core::performer_sklave::running::merge_search_trees::SklaveJob<E>> for Job<E> where E: EchoPolicy {
    fn from(sklave_job: core::performer_sklave::running::merge_search_trees::SklaveJob<E>) -> Job<E> {
        Job::MergeSearchTreesSklave(sklave_job)
    }
}

impl<E> From<core::performer_sklave::running::demolish_search_tree::SklaveJob<E>> for Job<E> where E: EchoPolicy {
    fn from(sklave_job: core::performer_sklave::running::demolish_search_tree::SklaveJob<E>) -> Job<E> {
        Job::DemolishSearchTreeSklave(sklave_job)
    }
}

pub static JOB_BLOCKWHEEL_FS: AtomicUsize = AtomicUsize::new(0);
pub static JOB_PERFORMER_SKLAVE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_LOOKUP_RANGE_MERGE_SKLAVE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_FLUSH_BUTCHER_SKLAVE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_MERGE_SEARCH_TREES_SKLAVE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_DEMOLISH_SEARCH_TREE_SKLAVE: AtomicUsize = AtomicUsize::new(0);

impl<E> edeltraud::Job for Job<E> where E: EchoPolicy {
    fn run<P>(self, thread_pool: &P) where P: edeltraud::ThreadPool<Self> {
        match self {
            Job::BlockwheelFs(job) => {
                JOB_BLOCKWHEEL_FS.fetch_add(1, Ordering::Relaxed);
                job.run(&edeltraud::ThreadPoolMap::new(thread_pool));
            },
            Job::PerformerSklave(sklave_job) => {
                JOB_PERFORMER_SKLAVE.fetch_add(1, Ordering::Relaxed);
                core::performer_sklave::run_job(sklave_job, thread_pool);
            },
            Job::LookupRangeMergeSklave(sklave_job) => {
                JOB_LOOKUP_RANGE_MERGE_SKLAVE.fetch_add(1, Ordering::Relaxed);
                core::performer_sklave::running::lookup_range_merge::run_job(sklave_job, thread_pool)
            },
            Job::FlushButcherSklave(sklave_job) => {
                JOB_FLUSH_BUTCHER_SKLAVE.fetch_add(1, Ordering::Relaxed);
                core::performer_sklave::running::flush_butcher::run_job(sklave_job, thread_pool)
            },
            Job::MergeSearchTreesSklave(sklave_job) => {
                JOB_MERGE_SEARCH_TREES_SKLAVE.fetch_add(1, Ordering::Relaxed);
                core::performer_sklave::running::merge_search_trees::run_job(sklave_job, thread_pool)
            },
            Job::DemolishSearchTreeSklave(sklave_job) => {
                JOB_DEMOLISH_SEARCH_TREE_SKLAVE.fetch_add(1, Ordering::Relaxed);
                core::performer_sklave::running::demolish_search_tree::run_job(sklave_job, thread_pool)
            },
        }
    }
}
