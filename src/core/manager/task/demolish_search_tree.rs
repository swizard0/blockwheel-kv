
use crate::{
    job,
    wheels,
    core::{
        manager::{
            task::{
                Wheels,
            },
        },
        performer,
        search_tree_walker,
    },
};

pub struct Args<J> where J: edeltraud::Job {
    pub order: performer::DemolishOrder,
    pub wheels: wheels::Wheels,
    pub thread_pool: edeltraud::Edeltraud<J>,
}

pub struct Done;

#[derive(Debug)]
pub enum Error {
    ThreadPoolGone,
    WheelNotFound {
        blockwheel_filename: wheels::WheelFilename,
    },
}

pub async fn run<J>(
    Args {
        order,
        wheels,
        thread_pool,
    }: Args<J>,
)
    -> Result<Done, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    inner_run(
        order,
        Wheels::Regular(wheels),
        thread_pool,
    ).await
}

async fn inner_run<J>(
    order: performer::DemolishOrder,
    wheels: Wheels,
    thread_pool: edeltraud::Edeltraud<J>,
)
    -> Result<Done, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{

    todo!()
}
