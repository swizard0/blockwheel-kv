use ero_blockwheel_fs as blockwheel;

use crate::{
    core,
};

pub enum Job {
    BlockwheelFs(blockwheel::job::Job),
    SearchTreeBootstrap(core::search_tree::task::bootstrap::JobArgs),
    SearchTreeSearchBlock(core::search_tree::task::search_block::JobArgs),
    SearchTreeIterBlock(core::search_tree::task::iter_block::JobArgs),
}

pub enum JobOutput {
    BlockwheelFs(blockwheel::job::JobOutput),
    SearchTreeBootstrap(SearchTreeBootstrapDone),
    SearchTreeSearchBlock(SearchTreeSearchBlockDone),
    SearchTreeIterBlock(SearchTreeIterBlockDone),
}

impl edeltraud::Job for Job {
    type Output = JobOutput;

    fn run(self) -> Self::Output {
        match self {
            Job::BlockwheelFs(job) =>
                JobOutput::BlockwheelFs(job.run()),
            Job::SearchTreeBootstrap(args) =>
                JobOutput::SearchTreeBootstrap(SearchTreeBootstrapDone(
                    core::search_tree::task::bootstrap::job(args),
                )),
            Job::SearchTreeSearchBlock(args) =>
                JobOutput::SearchTreeSearchBlock(SearchTreeSearchBlockDone(
                    core::search_tree::task::search_block::job(args),
                )),
            Job::SearchTreeIterBlock(args) =>
                JobOutput::SearchTreeIterBlock(SearchTreeIterBlockDone(
                    core::search_tree::task::iter_block::job(args),
                )),
        }
    }
}

pub struct SearchTreeBootstrapDone(
    pub core::search_tree::task::bootstrap::JobOutput,
);

impl From<JobOutput> for SearchTreeBootstrapDone {
    fn from(output: JobOutput) -> Self {
        match output {
            JobOutput::SearchTreeBootstrap(done) =>
                done,
            _other =>
                panic!("expected JobOutput::SearchTreeBootstrap but got other"),
        }
    }
}

pub struct SearchTreeSearchBlockDone(
    pub core::search_tree::task::search_block::JobOutput,
);

impl From<JobOutput> for SearchTreeSearchBlockDone {
    fn from(output: JobOutput) -> Self {
        match output {
            JobOutput::SearchTreeSearchBlock(done) =>
                done,
            _other =>
                panic!("expected JobOutput::SearchTreeSearchBlock but got other"),
        }
    }
}

pub struct SearchTreeIterBlockDone(
    pub core::search_tree::task::iter_block::JobOutput,
);

impl From<JobOutput> for SearchTreeIterBlockDone {
    fn from(output: JobOutput) -> Self {
        match output {
            JobOutput::SearchTreeIterBlock(done) =>
                done,
            _other =>
                panic!("expected JobOutput::SearchTreeIterBlock but got other"),
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
