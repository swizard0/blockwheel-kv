use crate::{
    job,
    core::{
        performer,
    },
};

pub struct Args<J> where J: edeltraud::Job {
    pub job_args: JobArgs,
    pub thread_pool: edeltraud::Edeltraud<J>,
}

pub struct Done {
    pub env: Env,
    pub next: Next,
}

#[derive(Debug)]
pub enum Error {
    ThreadPoolGone,
}

pub type Output = Result<Done, Error>;

pub async fn run<J>(args: Args<J>) -> Output
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    let job = job::Job::ManagerTaskPerformer(args.job_args);
    let job_output = args.thread_pool.spawn(job).await
        .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
    let job_output: job::JobOutput = job_output.into();
    let job::ManagerTaskPerformerDone(job_done) = job_output.into();
    job_done
}

pub struct JobArgs {
    pub env: Env,
    pub kont: Kont,
}

#[derive(Default)]
pub struct Env {
}

pub enum Kont {
    Start {
        performer: performer::Performer,
    },
}

pub enum Next {
    Poll {
        next: performer::KontPollNext,
    },
}

pub fn job(JobArgs { mut env, kont, }: JobArgs) -> Output {
    let mut performer_kont = match kont {
        Kont::Start { performer, } =>
            performer.step(),
    };

    loop {
        performer_kont = match performer_kont {
            performer::Kont::Poll(kont_poll) =>
                todo!(),
            performer::Kont::Inserted(kont_inserted) =>
                todo!(),
            performer::Kont::FlushButcher(kont_flush_butcher) =>
                todo!(),
        };
    }
}
