use crate::{
    job,
};

pub mod performer;

pub enum TaskArgs<J> where J: edeltraud::Job {
    Performer(performer::Args<J>),
}

pub enum TaskDone {
    Performer(performer::Done),
}

#[derive(Debug)]
pub enum Error {
    Performer(performer::Error),
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
    })
}
