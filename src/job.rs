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
    AccessPolicy,
};

pub enum Job<A> where A: AccessPolicy {
    BlockwheelFs(blockwheel_fs::job::Job<wheels::WheelAccessPolicy<A>>),
    PerformerSklave(core::performer_sklave::SklaveJob<A>),
    LookupRangeMergeSklave(core::performer_sklave::running::lookup_range_merge::SklaveJob<A>),
    FlushButcherSklave(core::performer_sklave::running::flush_butcher::SklaveJob<A>),
    MergeSearchTreesSklave(core::performer_sklave::running::merge_search_trees::SklaveJob<A>),
    DemolishSearchTreeSklave(core::performer_sklave::running::demolish_search_tree::SklaveJob<A>),
}

impl<A> From<blockwheel_fs::job::Job<wheels::WheelAccessPolicy<A>>> for Job<A> where A: AccessPolicy {
    fn from(job: blockwheel_fs::job::Job<wheels::WheelAccessPolicy<A>>) -> Job<A> {
        Job::BlockwheelFs(job)
    }
}

impl<A> From<core::performer_sklave::SklaveJob<A>> for Job<A> where A: AccessPolicy {
    fn from(sklave_job: core::performer_sklave::SklaveJob<A>) -> Job<A> {
        Job::PerformerSklave(sklave_job)
    }
}

impl<A> From<core::performer_sklave::running::lookup_range_merge::SklaveJob<A>> for Job<A> where A: AccessPolicy {
    fn from(sklave_job: core::performer_sklave::running::lookup_range_merge::SklaveJob<A>) -> Job<A> {
        Job::LookupRangeMergeSklave(sklave_job)
    }
}

impl<A> From<core::performer_sklave::running::flush_butcher::SklaveJob<A>> for Job<A> where A: AccessPolicy {
    fn from(sklave_job: core::performer_sklave::running::flush_butcher::SklaveJob<A>) -> Job<A> {
        Job::FlushButcherSklave(sklave_job)
    }
}

impl<A> From<core::performer_sklave::running::merge_search_trees::SklaveJob<A>> for Job<A> where A: AccessPolicy {
    fn from(sklave_job: core::performer_sklave::running::merge_search_trees::SklaveJob<A>) -> Job<A> {
        Job::MergeSearchTreesSklave(sklave_job)
    }
}

impl<A> From<core::performer_sklave::running::demolish_search_tree::SklaveJob<A>> for Job<A> where A: AccessPolicy {
    fn from(sklave_job: core::performer_sklave::running::demolish_search_tree::SklaveJob<A>) -> Job<A> {
        Job::DemolishSearchTreeSklave(sklave_job)
    }
}

pub static JOB_BLOCKWHEEL_FS: AtomicUsize = AtomicUsize::new(0);
pub static JOB_PERFORMER_SKLAVE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_LOOKUP_RANGE_MERGE_SKLAVE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_FLUSH_BUTCHER_SKLAVE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_MERGE_SEARCH_TREES_SKLAVE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_DEMOLISH_SEARCH_TREE_SKLAVE: AtomicUsize = AtomicUsize::new(0);

impl<A> edeltraud::Job for Job<A> where A: AccessPolicy {
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
