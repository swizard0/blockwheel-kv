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

impl edeltraud::Job for Job {
    type Output = JobOutput;

    fn run(self) -> Self::Output {
        match self {
            Job::BlockwheelFs(job) =>
                JobOutput::BlockwheelFs(job.run()),
            Job::ManagerTaskPerformer(args) =>
                JobOutput::ManagerTaskPerformer(ManagerTaskPerformerDone(core::manager::task::performer::job(args))),
            Job::ManagerTaskFlushButcher(args) =>
                JobOutput::ManagerTaskFlushButcher(ManagerTaskFlushButcherDone(core::manager::task::flush_butcher::job(args))),
            Job::ManagerTaskLookupRangeMerge(args) =>
                JobOutput::ManagerTaskLookupRangeMerge(ManagerTaskLookupRangeMergeDone(core::manager::task::lookup_range_merge::job(args))),
            Job::ManagerTaskMergeSearchTrees(args) =>
                JobOutput::ManagerTaskMergeSearchTrees(ManagerTaskMergeSearchTreesDone(core::manager::task::merge_search_trees::job(args))),
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
