use std::{
    sync::{
        atomic::{
            Ordering,
            AtomicUsize,
        },
    },
};

use crate::{
    core::{
        performer_sklave,
    },
    wheels,
    EchoPolicy,
};

pub enum Job<E> where E: EchoPolicy {
    BlockwheelFs(blockwheel_fs::job::Job<wheels::WheelEchoPolicy<E>>),
    PerformerSklave(performer_sklave::SklaveJob<E>),
    LookupRangeMergeSklave(performer_sklave::running::lookup_range_merge::SklaveJob<E>),
    FlushButcherSklave(performer_sklave::running::flush_butcher::SklaveJob<E>),
    MergeSearchTreesSklave(performer_sklave::running::merge_search_trees::SklaveJob<E>),
    DemolishSearchTreeSklave(performer_sklave::running::demolish_search_tree::SklaveJob<E>),
}

impl<E> From<blockwheel_fs::job::Job<wheels::WheelEchoPolicy<E>>> for Job<E> where E: EchoPolicy {
    fn from(job: blockwheel_fs::job::Job<wheels::WheelEchoPolicy<E>>) -> Job<E> {
        Job::BlockwheelFs(job)
    }
}

impl<E> From<performer_sklave::SklaveJob<E>> for Job<E> where E: EchoPolicy {
    fn from(sklave_job: performer_sklave::SklaveJob<E>) -> Job<E> {
        Job::PerformerSklave(sklave_job)
    }
}

impl<E> From<performer_sklave::running::lookup_range_merge::SklaveJob<E>> for Job<E> where E: EchoPolicy {
    fn from(sklave_job: performer_sklave::running::lookup_range_merge::SklaveJob<E>) -> Job<E> {
        Job::LookupRangeMergeSklave(sklave_job)
    }
}

impl<E> From<performer_sklave::running::flush_butcher::SklaveJob<E>> for Job<E> where E: EchoPolicy {
    fn from(sklave_job: performer_sklave::running::flush_butcher::SklaveJob<E>) -> Job<E> {
        Job::FlushButcherSklave(sklave_job)
    }
}

impl<E> From<performer_sklave::running::merge_search_trees::SklaveJob<E>> for Job<E> where E: EchoPolicy {
    fn from(sklave_job: performer_sklave::running::merge_search_trees::SklaveJob<E>) -> Job<E> {
        Job::MergeSearchTreesSklave(sklave_job)
    }
}

impl<E> From<performer_sklave::running::demolish_search_tree::SklaveJob<E>> for Job<E> where E: EchoPolicy {
    fn from(sklave_job: performer_sklave::running::demolish_search_tree::SklaveJob<E>) -> Job<E> {
        Job::DemolishSearchTreeSklave(sklave_job)
    }
}

pub static JOB_BLOCKWHEEL_FS: AtomicUsize = AtomicUsize::new(0);
pub static JOB_PERFORMER_SKLAVE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_LOOKUP_RANGE_MERGE_SKLAVE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_FLUSH_BUTCHER_SKLAVE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_MERGE_SEARCH_TREES_SKLAVE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_DEMOLISH_SEARCH_TREE_SKLAVE: AtomicUsize = AtomicUsize::new(0);

pub struct JobUnit<E, J>(edeltraud::JobUnit<J, Job<E>>) where E: EchoPolicy;

impl<E, J> From<edeltraud::JobUnit<J, Job<E>>> for JobUnit<E, J> where E: EchoPolicy {
    fn from(job_unit: edeltraud::JobUnit<J, Job<E>>) -> Self {
        Self(job_unit)
    }
}

impl<E, J> edeltraud::Job for JobUnit<E, J>
where E: EchoPolicy,
      J: From<blockwheel_fs::job::SklaveJob<wheels::WheelEchoPolicy<E>>>,
      J: From<blockwheel_fs::job::BlockPrepareWriteJob<wheels::WheelEchoPolicy<E>>>,
      J: From<blockwheel_fs::job::BlockPrepareDeleteJob<wheels::WheelEchoPolicy<E>>>,
      J: From<blockwheel_fs::job::BlockProcessReadJob<wheels::WheelEchoPolicy<E>>>,
      J: From<performer_sklave::running::flush_butcher::SklaveJob<E>>,
      J: From<performer_sklave::running::lookup_range_merge::SklaveJob<E>>,
      J: From<performer_sklave::running::merge_search_trees::SklaveJob<E>>,
      J: From<performer_sklave::running::demolish_search_tree::SklaveJob<E>>,
      J: Send + 'static,
{
    fn run(self) {
        match self.0.job {
            Job::BlockwheelFs(job) => {
                JOB_BLOCKWHEEL_FS.fetch_add(1, Ordering::Relaxed);
                let job_unit = blockwheel_fs::job::JobUnit::from(edeltraud::JobUnit {
                    handle: self.0.handle,
                    job,
                });
                job_unit.run();
            },
            Job::PerformerSklave(sklave_job) => {
                JOB_PERFORMER_SKLAVE.fetch_add(1, Ordering::Relaxed);
                performer_sklave::run_job(sklave_job, &self.0.handle);
            },
            Job::LookupRangeMergeSklave(sklave_job) => {
                JOB_LOOKUP_RANGE_MERGE_SKLAVE.fetch_add(1, Ordering::Relaxed);
                performer_sklave::running::lookup_range_merge::run_job(sklave_job, &self.0.handle)
            },
            Job::FlushButcherSklave(sklave_job) => {
                JOB_FLUSH_BUTCHER_SKLAVE.fetch_add(1, Ordering::Relaxed);
                performer_sklave::running::flush_butcher::run_job(sklave_job, &self.0.handle)
            },
            Job::MergeSearchTreesSklave(sklave_job) => {
                JOB_MERGE_SEARCH_TREES_SKLAVE.fetch_add(1, Ordering::Relaxed);
                performer_sklave::running::merge_search_trees::run_job(sklave_job, &self.0.handle)
            },
            Job::DemolishSearchTreeSklave(sklave_job) => {
                JOB_DEMOLISH_SEARCH_TREE_SKLAVE.fetch_add(1, Ordering::Relaxed);
                performer_sklave::running::demolish_search_tree::run_job(sklave_job, &self.0.handle)
            },
        }
    }
}
