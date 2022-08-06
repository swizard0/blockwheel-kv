
use crate::{
    job,
    core::{
        performer,
        RequestLookupKind,
    },
};

pub struct Args<J> where J: edeltraud::Job {
    pub ranges_merger: performer::LookupRangesMerger,
    pub lookup_context: RequestLookupKind,
    pub thread_pool: edeltraud::Edeltraud<J>,
}

pub struct Done {
}

#[derive(Debug)]
pub enum Error {
    ThreadPoolGone,
}

pub type Output = Result<Done, Error>;

pub async fn run<J>(
    Args {
        ranges_merger,
        lookup_context,
        thread_pool,
    }: Args<J>,
)
    -> Result<Done, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    inner_run(
        ranges_merger,
        lookup_context,
        thread_pool,
    ).await
}

async fn inner_run<J>(
    ranges_merger: performer::LookupRangesMerger,
    lookup_context: RequestLookupKind,
    thread_pool: edeltraud::Edeltraud<J>,
)
    -> Result<Done, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{

    todo!()
}
