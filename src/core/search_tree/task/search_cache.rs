use std::{
    sync::Arc,
};

use crate::{
    kv,
    job,
    storage,
    core::{
        MemCache,
        search_tree::task::{
            LookupRequest,
        },
    },
};

pub struct Args<J> where J: edeltraud::Job {
    pub lookup_request: LookupRequest,
    pub cache: Arc<MemCache>,
    pub thread_pool: edeltraud::Edeltraud<J>,
}

pub struct Done;

#[derive(Debug)]
pub enum Error {
    ThreadPoolGone,
}

pub type JobOutput = Result<JobDone, Error>;

pub struct JobArgs {
    key: kv::Key,
    cache: Arc<MemCache>,
}

pub struct JobDone {
    outcome: Option<kv::ValueCell<storage::OwnedValueBlockRef>>,
}

pub fn job(JobArgs { key, cache, }: JobArgs) -> JobOutput {
    let result = cache.get(&*key.key_bytes)
        .cloned();
    Ok(JobDone { outcome: result.map(From::from), })
}

pub async fn run<J>(
    Args {
        lookup_request,
        cache,
        thread_pool,
    }: Args<J>,
)
    -> Result<Done, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    let LookupRequest { key, reply_tx, } = lookup_request;
    let job_output = thread_pool.spawn(job::Job::SearchTreeSearchCache(JobArgs { key, cache, })).await
        .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
    let job_output: job::JobOutput = job_output.into();
    let job::SearchTreeSearchCacheDone(job_result) = job_output.into();
    let JobDone { outcome, } = job_result?;

    if let Err(_send_error) = reply_tx.send(Ok(outcome.map(From::from))) {
        log::warn!("client canceled lookup request");
    }
    Ok(Done)
}
