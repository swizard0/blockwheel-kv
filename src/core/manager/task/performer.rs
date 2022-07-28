use crate::{
    job,
    core::{
        performer,
        Context as C,
        RequestInsert,
    },
    Inserted,
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
    pub incoming: Incoming,
}

#[derive(Default)]
pub struct Incoming {
    pub request_insert: Vec<RequestInsert>,
}

pub enum Kont {
    Start {
        performer: performer::Performer<C>,
    },
    StepPoll {
        next: performer::KontPollNext<C>,
    },
}

pub enum Next {
    Poll {
        next: performer::KontPollNext<C>,
    },
}

pub fn job(JobArgs { mut env, mut kont, }: JobArgs) -> Output {
    loop {
        todo!();

        // let performer_kont = match kont {
        //     Kont::Start { performer, } =>
        //         performer.step(),
        //     Kont::StepPoll { next, } =>
        //         if let Some(RequestInsert { key, value, reply_tx, }) = env.incoming.request_insert.pop() {
        //             let performer_kont = next.incoming_insert(key, value); ?????
        //             if let Err(_send_error) = reply_tx.send(Inserted) {
        //                 log::warn!("client canceled insert request");
        //             }
        //             performer_kont
        //         },
        // };

        // performer_kont = match performer_kont {
        //     performer::Kont::Poll(kont_poll) =>
        //         todo!(),
        //     performer::Kont::Inserted(kont_inserted) =>
        //         todo!(),
        //     performer::Kont::FlushButcher(kont_flush_butcher) =>
        //         todo!(),
        // };
    }
}
