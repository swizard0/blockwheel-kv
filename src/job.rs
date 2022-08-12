use ero_blockwheel_fs as blockwheel;

use crate::{
    core,
};

pub enum Job {
    BlockwheelFs(blockwheel::job::Job),
    ManagerTaskPerformer(core::manager::task::performer::JobArgs),
    ManagerTaskFlushButcher(core::manager::task::flush_butcher::JobArgs),
    ManagerTaskLookupRangeMerge(core::manager::task::lookup_range_merge::JobArgs),
    ManagerTaskMergeSearchTrees(core::manager::task::merge_search_trees::JobArgs),
}

pub enum JobOutput {
    BlockwheelFs(blockwheel::job::JobOutput),
    ManagerTaskPerformer(ManagerTaskPerformerDone),
    ManagerTaskFlushButcher(ManagerTaskFlushButcherDone),
    ManagerTaskLookupRangeMerge(ManagerTaskLookupRangeMergeDone),
    ManagerTaskMergeSearchTrees(ManagerTaskMergeSearchTreesDone),
}

use std::{sync::atomic::{self, AtomicUsize}};

pub static JOB_BLOCKWHEEL_FS: AtomicUsize = AtomicUsize::new(0);
pub static JOB_MANAGER_TASK_PERFORMER: AtomicUsize = AtomicUsize::new(0);
pub static JOB_MANAGER_TASK_FLUSH_BUTCHER: AtomicUsize = AtomicUsize::new(0);
pub static JOB_MANAGER_TASK_LOOKUP_RANGE_MERGE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_MANAGER_TASK_MERGE_SEARCH_TREES: AtomicUsize = AtomicUsize::new(0);

impl edeltraud::Job for Job {
    type Output = JobOutput;

    fn run(self) -> Self::Output {
        match self {
            Job::BlockwheelFs(job) => {
                JOB_BLOCKWHEEL_FS.fetch_add(1, atomic::Ordering::Relaxed);
                JobOutput::BlockwheelFs(job.run())
            },
            Job::ManagerTaskPerformer(args) => {
                JOB_MANAGER_TASK_PERFORMER.fetch_add(1, atomic::Ordering::Relaxed);
                JobOutput::ManagerTaskPerformer(ManagerTaskPerformerDone(core::manager::task::performer::job(args)))
            },
            Job::ManagerTaskFlushButcher(args) => {
                JOB_MANAGER_TASK_FLUSH_BUTCHER.fetch_add(1, atomic::Ordering::Relaxed);
                JobOutput::ManagerTaskFlushButcher(ManagerTaskFlushButcherDone(core::manager::task::flush_butcher::job(args)))
            },
            Job::ManagerTaskLookupRangeMerge(args) => {
                JOB_MANAGER_TASK_LOOKUP_RANGE_MERGE.fetch_add(1, atomic::Ordering::Relaxed);
                JobOutput::ManagerTaskLookupRangeMerge(ManagerTaskLookupRangeMergeDone(core::manager::task::lookup_range_merge::job(args)))
            },
            Job::ManagerTaskMergeSearchTrees(args) => {
                JOB_MANAGER_TASK_MERGE_SEARCH_TREES.fetch_add(1, atomic::Ordering::Relaxed);
                JobOutput::ManagerTaskMergeSearchTrees(ManagerTaskMergeSearchTreesDone(core::manager::task::merge_search_trees::job(args)))
            },
        }
    }
}

impl From<blockwheel::job::Job> for Job {
    fn from(job: blockwheel::job::Job) -> Job {
        Job::BlockwheelFs(job)
    }
}

impl From<blockwheel::job::JobOutput> for JobOutput {
    fn from(output: blockwheel::job::JobOutput) -> JobOutput {
        JobOutput::BlockwheelFs(output)
    }
}

impl From<JobOutput> for blockwheel::job::JobOutput {
    fn from(output: JobOutput) -> blockwheel::job::JobOutput {
        match output {
            JobOutput::BlockwheelFs(done) =>
                done,
            _other =>
                panic!("expected JobOutput::BlockwheelFs but got other"),
        }
    }
}

pub struct ManagerTaskPerformerDone(pub core::manager::task::performer::Output);

impl From<JobOutput> for ManagerTaskPerformerDone {
    fn from(output: JobOutput) -> Self {
        match output {
            JobOutput::ManagerTaskPerformer(done) =>
                done,
            _other =>
                panic!("expected JobOutput::ManagerTaskPerformer but got other"),
        }
    }
}

pub struct ManagerTaskFlushButcherDone(pub core::manager::task::flush_butcher::Output);

impl From<JobOutput> for ManagerTaskFlushButcherDone {
    fn from(output: JobOutput) -> Self {
        match output {
            JobOutput::ManagerTaskFlushButcher(done) =>
                done,
            _other =>
                panic!("expected JobOutput::ManagerTaskFlushButcher but got other"),
        }
    }
}

pub struct ManagerTaskLookupRangeMergeDone(pub core::manager::task::lookup_range_merge::Output);

impl From<JobOutput> for ManagerTaskLookupRangeMergeDone {
    fn from(output: JobOutput) -> Self {
        match output {
            JobOutput::ManagerTaskLookupRangeMerge(done) =>
                done,
            _other =>
                panic!("expected JobOutput::ManagerTaskLookupRangeMerge but got other"),
        }
    }
}

pub struct ManagerTaskMergeSearchTreesDone(pub core::manager::task::merge_search_trees::Output);

impl From<JobOutput> for ManagerTaskMergeSearchTreesDone {
    fn from(output: JobOutput) -> Self {
        match output {
            JobOutput::ManagerTaskMergeSearchTrees(done) =>
                done,
            _other =>
                panic!("expected JobOutput::ManagerTaskMergeSearchTrees but got other"),
        }
    }
}
