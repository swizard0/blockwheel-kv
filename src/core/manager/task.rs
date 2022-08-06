use crate::{
    job,
};

pub mod performer;
pub mod flush_butcher;
pub mod lookup_range_merge;

pub enum TaskArgs<J> where J: edeltraud::Job {
    Performer(performer::Args<J>),
    FlushButcher(flush_butcher::Args<J>),
    LookupRangeMerge(lookup_range_merge::Args<J>),
}

pub enum TaskDone {
    Performer(performer::Done),
    FlushButcher(flush_butcher::Done),
    LookupRangeMerge(lookup_range_merge::Done),
}

#[derive(Debug)]
pub enum Error {
    Performer(performer::Error),
    FlushButcher(flush_butcher::Error),
    LookupRangeMerge(lookup_range_merge::Error),
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
        TaskArgs::LookupRangeMerge(args) =>
            TaskDone::LookupRangeMerge(
                lookup_range_merge::run(args).await
                    .map_err(Error::LookupRangeMerge)?,
            ),
    })
}
