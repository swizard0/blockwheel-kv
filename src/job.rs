use std::{
    sync::{
        atomic::{
            Ordering,
            AtomicUsize,
        },
    },
};

use blockwheel_fs_ero as blockwheel;

use crate::{
    core,
};

pub enum Job {
    BlockwheelFs(blockwheel::job::Job),
    ManagerTaskPerformer(edeltraud::AsyncJob<core::manager::task::performer::Job>),
    ManagerTaskFlushButcher(edeltraud::AsyncJob<core::manager::task::flush_butcher::JobArgs>),
    ManagerTaskLookupRangeMerge(edeltraud::AsyncJob<core::manager::task::lookup_range_merge::JobArgs>),
    ManagerTaskMergeSearchTrees(edeltraud::AsyncJob<core::manager::task::merge_search_trees::JobArgs>),
    ManagerTaskDemolishSearchTree(edeltraud::AsyncJob<core::manager::task::demolish_search_tree::JobArgs>),
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

impl From<edeltraud::AsyncJob<core::manager::task::flush_butcher::JobArgs>> for Job {
    fn from(job_args: edeltraud::AsyncJob<core::manager::task::flush_butcher::JobArgs>) -> Job {
        Job::ManagerTaskFlushButcher(job_args)
    }
}

impl From<edeltraud::AsyncJob<core::manager::task::lookup_range_merge::JobArgs>> for Job {
    fn from(job_args: edeltraud::AsyncJob<core::manager::task::lookup_range_merge::JobArgs>) -> Job {
        Job::ManagerTaskLookupRangeMerge(job_args)
    }
}

impl From<edeltraud::AsyncJob<core::manager::task::merge_search_trees::JobArgs>> for Job {
    fn from(job_args: edeltraud::AsyncJob<core::manager::task::merge_search_trees::JobArgs>) -> Job {
        Job::ManagerTaskMergeSearchTrees(job_args)
    }
}

impl From<edeltraud::AsyncJob<core::manager::task::demolish_search_tree::JobArgs>> for Job {
    fn from(job_args: edeltraud::AsyncJob<core::manager::task::demolish_search_tree::JobArgs>) -> Job {
        Job::ManagerTaskDemolishSearchTree(job_args)
    }
}

pub static JOB_BLOCKWHEEL_FS: AtomicUsize = AtomicUsize::new(0);
pub static JOB_MANAGER_TASK_PERFORMER: AtomicUsize = AtomicUsize::new(0);
pub static JOB_MANAGER_TASK_FLUSH_BUTCHER: AtomicUsize = AtomicUsize::new(0);
pub static JOB_MANAGER_TASK_LOOKUP_RANGE_MERGE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_MANAGER_TASK_MERGE_SEARCH_TREES: AtomicUsize = AtomicUsize::new(0);
pub static JOB_MANAGER_TASK_DEMOLISH_SEARCH_TREE: AtomicUsize = AtomicUsize::new(0);

impl edeltraud::Job for Job {
    fn run<P>(self, thread_pool: &P) where P: edeltraud::ThreadPool<Self> {
        match self {
            Job::BlockwheelFs(job) => {
                JOB_BLOCKWHEEL_FS.fetch_add(1, Ordering::Relaxed);
                job.run(&edeltraud::ThreadPoolMap::new(thread_pool));
            },
            Job::ManagerTaskPerformer(job) => {
                JOB_MANAGER_TASK_PERFORMER.fetch_add(1, Ordering::Relaxed);
                job.run(&edeltraud::ThreadPoolMap::new(thread_pool));
            },
            Job::ManagerTaskFlushButcher(job) => {
                JOB_MANAGER_TASK_FLUSH_BUTCHER.fetch_add(1, Ordering::Relaxed);
                job.run(&edeltraud::ThreadPoolMap::new(thread_pool));
            },
            Job::ManagerTaskLookupRangeMerge(job) => {
                JOB_MANAGER_TASK_LOOKUP_RANGE_MERGE.fetch_add(1, Ordering::Relaxed);
                job.run(&edeltraud::ThreadPoolMap::new(thread_pool));
            },
            Job::ManagerTaskMergeSearchTrees(job) => {
                JOB_MANAGER_TASK_MERGE_SEARCH_TREES.fetch_add(1, Ordering::Relaxed);
                job.run(&edeltraud::ThreadPoolMap::new(thread_pool));
            },
            Job::ManagerTaskDemolishSearchTree(job) => {
                JOB_MANAGER_TASK_DEMOLISH_SEARCH_TREE.fetch_add(1, Ordering::Relaxed);
                job.run(&edeltraud::ThreadPoolMap::new(thread_pool));
            },
        }
    }
}
