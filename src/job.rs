use ero_blockwheel_fs as blockwheel;

use crate::{
    core,
};

pub enum Job {
    BlockwheelFs(blockwheel::job::Job),
    SearchTreeBootstrapBlock(core::search_tree::task::bootstrap::BlockJobArgs),
    SearchTreeBootstrapLayout(core::search_tree::task::bootstrap::LayoutJobArgs),
    SearchTreeSearchBlock(core::search_tree::task::search_block::JobArgs),
    SearchTreeIterCache(core::search_tree::task::iter_cache::JobArgs),
    SearchTreeIterBlock(core::search_tree::task::iter_block::JobArgs),
}

pub enum JobOutput {
    BlockwheelFs(blockwheel::job::JobOutput),
    SearchTreeBootstrapBlock(SearchTreeBootstrapBlockDone),
    SearchTreeBootstrapLayout(SearchTreeBootstrapLayoutDone),
    SearchTreeSearchBlock(SearchTreeSearchBlockDone),
    SearchTreeIterCache(SearchTreeIterCacheDone),
    SearchTreeIterBlock(SearchTreeIterBlockDone),
}

impl edeltraud::Job for Job {
    type Output = JobOutput;

    fn run(self) -> Self::Output {
        match self {
            Job::BlockwheelFs(job) =>
                JobOutput::BlockwheelFs(job.run()),
            Job::SearchTreeBootstrapBlock(args) =>
                JobOutput::SearchTreeBootstrapBlock(SearchTreeBootstrapBlockDone(
                    core::search_tree::task::bootstrap::block_job(args),
                )),
            Job::SearchTreeBootstrapLayout(args) =>
                JobOutput::SearchTreeBootstrapLayout(SearchTreeBootstrapLayoutDone(
                    core::search_tree::task::bootstrap::layout_job(args),
                )),
            Job::SearchTreeSearchBlock(args) =>
                JobOutput::SearchTreeSearchBlock(SearchTreeSearchBlockDone(
                    core::search_tree::task::search_block::job(args),
                )),
            Job::SearchTreeIterCache(args) =>
                JobOutput::SearchTreeIterCache(SearchTreeIterCacheDone(
                    core::search_tree::task::iter_cache::job(args),
                )),
            Job::SearchTreeIterBlock(args) =>
                JobOutput::SearchTreeIterBlock(SearchTreeIterBlockDone(
                    core::search_tree::task::iter_block::job(args),
                )),
        }
    }
}

pub struct SearchTreeBootstrapBlockDone(
    pub core::search_tree::task::bootstrap::BlockJobOutput,
);

impl From<JobOutput> for SearchTreeBootstrapBlockDone {
    fn from(output: JobOutput) -> Self {
        match output {
            JobOutput::SearchTreeBootstrapBlock(done) =>
                done,
            _other =>
                panic!("expected JobOutput::SearchTreeBootstrapBlock but got other"),
        }
    }
}

pub struct SearchTreeBootstrapLayoutDone(
    pub core::search_tree::task::bootstrap::LayoutJobOutput,
);

impl From<JobOutput> for SearchTreeBootstrapLayoutDone {
    fn from(output: JobOutput) -> Self {
        match output {
            JobOutput::SearchTreeBootstrapLayout(done) =>
                done,
            _other =>
                panic!("expected JobOutput::SearchTreeBootstrapLayout but got other"),
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

pub struct SearchTreeIterCacheDone(
    pub core::search_tree::task::iter_cache::JobOutput,
);

impl From<JobOutput> for SearchTreeIterCacheDone {
    fn from(output: JobOutput) -> Self {
        match output {
            JobOutput::SearchTreeIterCache(done) =>
                done,
            _other =>
                panic!("expected JobOutput::SearchTreeIterCache but got other"),
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
