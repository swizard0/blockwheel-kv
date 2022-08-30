use std::{
    sync::{
        atomic::{
            Ordering,
            AtomicUsize,
        },
    },
};

use ero_blockwheel_fs as blockwheel;

use crate::{
    core,
};

pub enum Job {
    BlockwheelFs(blockwheel::job::Job),
    ManagerTaskPerformer(edeltraud::AsyncJob<core::manager::task::performer::Job>),
    ManagerTaskFlushButcher(edeltraud::AsyncJob<core::manager::task::flush_butcher::Job>),
    ManagerTaskLookupRangeMerge(edeltraud::AsyncJob<core::manager::task::lookup_range_merge::Job>),
    ManagerTaskMergeSearchTrees(edeltraud::AsyncJob<core::manager::task::merge_search_trees::Job>),
    ManagerTaskDemolishSearchTree(edeltraud::AsyncJob<core::manager::task::demolish_search_tree::Job>),
}

impl From<blockwheel::job::Job> for Job {
    fn from(job: blockwheel::job::Job) -> Job {
        Job::BlockwheelFs(job)
    }
}

impl From<edeltraud::AsyncJob<core::manager::task::performer::Job>> for Job {
    fn from(job: edeltraud::AsyncJob<core::manager::task::performer::Job>) -> Job {
        Job::ManagerTaskPerformer(job)
    }
}

impl From<edeltraud::AsyncJob<core::manager::task::flush_butcher::Job>> for Job {
    fn from(job: edeltraud::AsyncJob<core::manager::task::flush_butcher::Job>) -> Job {
        Job::ManagerTaskFlushButcher(job)
    }
}

impl From<edeltraud::AsyncJob<core::manager::task::lookup_range_merge::Job>> for Job {
    fn from(job: edeltraud::AsyncJob<core::manager::task::lookup_range_merge::Job>) -> Job {
        Job::ManagerTaskLookupRangeMerge(job)
    }
}

impl From<edeltraud::AsyncJob<core::manager::task::merge_search_trees::Job>> for Job {
    fn from(job: edeltraud::AsyncJob<core::manager::task::merge_search_trees::Job>) -> Job {
        Job::ManagerTaskMergeSearchTrees(job)
    }
}

impl From<edeltraud::AsyncJob<core::manager::task::demolish_search_tree::Job>> for Job {
    fn from(job: edeltraud::AsyncJob<core::manager::task::demolish_search_tree::Job>) -> Job {
        Job::ManagerTaskDemolishSearchTree(job)
    }
}

pub static JOB_BLOCKWHEEL_FS: AtomicUsize = AtomicUsize::new(0);
pub static JOB_MANAGER_TASK_PERFORMER: AtomicUsize = AtomicUsize::new(0);
pub static JOB_MANAGER_TASK_FLUSH_BUTCHER: AtomicUsize = AtomicUsize::new(0);
pub static JOB_MANAGER_TASK_LOOKUP_RANGE_MERGE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_MANAGER_TASK_MERGE_SEARCH_TREES: AtomicUsize = AtomicUsize::new(0);
pub static JOB_MANAGER_TASK_DEMOLISH_SEARCH_TREE: AtomicUsize = AtomicUsize::new(0);

impl edeltraud::Job for Job {
    type Output = ();

    fn run<P>(self, thread_pool: &P) -> Self::Output where P: edeltraud::ThreadPool<Self> {
        match self {
            Job::BlockwheelFs(job) => {
                JOB_BLOCKWHEEL_FS.fetch_add(1, Ordering::Relaxed);
                job.run(&edeltraud::EdeltraudJobMap::new(thread_pool));
            },
            Job::ManagerTaskPerformer(job) => {
                JOB_MANAGER_TASK_PERFORMER.fetch_add(1, Ordering::Relaxed);
                job.run(&edeltraud::EdeltraudJobMap::new(thread_pool));
            },
            Job::ManagerTaskFlushButcher(job) => {
                JOB_MANAGER_TASK_FLUSH_BUTCHER.fetch_add(1, Ordering::Relaxed);
                job.run(&edeltraud::EdeltraudJobMap::new(thread_pool));
            },
            Job::ManagerTaskLookupRangeMerge(job) => {
                JOB_MANAGER_TASK_LOOKUP_RANGE_MERGE.fetch_add(1, Ordering::Relaxed);
                job.run(&edeltraud::EdeltraudJobMap::new(thread_pool));
            },
            Job::ManagerTaskMergeSearchTrees(job) => {
                JOB_MANAGER_TASK_MERGE_SEARCH_TREES.fetch_add(1, Ordering::Relaxed);
                job.run(&edeltraud::EdeltraudJobMap::new(thread_pool));
            },
            Job::ManagerTaskDemolishSearchTree(job) => {
                JOB_MANAGER_TASK_DEMOLISH_SEARCH_TREE.fetch_add(1, Ordering::Relaxed);
                job.run(&edeltraud::EdeltraudJobMap::new(thread_pool));
            },
        }
    }
}
