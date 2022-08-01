use crate::{
    job,
};

pub mod performer;
pub mod flush_butcher;

pub enum TaskArgs<J> where J: edeltraud::Job {
    Performer(performer::Args<J>),
    FlushButcher(flush_butcher::Args<J>),
}

pub enum TaskDone {
    Performer(performer::Done),
    FlushButcher(flush_butcher::Done),
}

#[derive(Debug)]
pub enum Error {
    Performer(performer::Error),
    FlushButcher(flush_butcher::Error),
}

pub async fn run_args<J>(args: TaskArgs<J>) -> Result<TaskDone, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    Ok(match args {
        TaskArgs::Performer(args) =>
            TaskDone::Performer(
                performer::run(args).await
                    .map_err(Error::Performer)?,
            ),
        TaskArgs::FlushButcher(args) =>
            TaskDone::FlushButcher(
                flush_butcher::run(args).await
                    .map_err(Error::FlushButcher)?,
            ),
    })
}
