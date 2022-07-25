use ero_blockwheel_fs as blockwheel;

use crate::{
    core,
};

pub enum Job {
    BlockwheelFs(blockwheel::job::Job),
}

pub enum JobOutput {
    BlockwheelFs(blockwheel::job::JobOutput),
}

impl edeltraud::Job for Job {
    type Output = JobOutput;

    fn run(self) -> Self::Output {
        match self {
            Job::BlockwheelFs(job) =>
                JobOutput::BlockwheelFs(job.run()),
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
