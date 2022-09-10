use alloc_pool::{
    Unique,
};

use arbeitssklave::{
    komm,
};

use crate::{
    job,
    core::{
        performer,
        performer_sklave,
        search_ranges_merge,
    },
    AccessPolicy,
};

pub enum Order<A> where A: AccessPolicy {
    Terminate,
    Dummy(A),
}

pub struct Welt<A> where A: AccessPolicy {
    pub kont: Option<Kont>,
    pub lookup_context: komm::Rueckkopplung<A::Order, A::LookupRange>,
    pub drop_bomb: komm::Rueckkopplung<performer_sklave::Order<A>, performer_sklave::LookupRangeMergeDrop>,
}

pub type Meister<A> = arbeitssklave::Meister<Welt<A>, Order<A>>;
pub type SklaveJob<A> = arbeitssklave::SklaveJob<Welt<A>, Order<A>>;

type SearchRangesMergeCps =
    search_ranges_merge::RangesMergeCps<Unique<Vec<performer::LookupRangeSource>>, performer::LookupRangeSource>;
type SearchRangesMergeKont =
    search_ranges_merge::Kont<Unique<Vec<performer::LookupRangeSource>>, performer::LookupRangeSource>;
type SearchRangesMergeBlockNext =
    search_ranges_merge::KontAwaitBlocksNext<Unique<Vec<performer::LookupRangeSource>>, performer::LookupRangeSource>;
type SearchRangesMergeItemNext =
    search_ranges_merge::KontEmitItemNext<Unique<Vec<performer::LookupRangeSource>>, performer::LookupRangeSource>;

pub enum Kont {
    Start {
        merger: SearchRangesMergeCps,
    },
    ProceedAwaitBlocks {
        next: SearchRangesMergeBlockNext,
    },
    ProceedItem {
        next: SearchRangesMergeItemNext,
    },
}

#[derive(Debug)]
pub enum Error {
    SearchRangesMerge(search_ranges_merge::Error),
}

pub fn run_job<A, P>(sklave_job: SklaveJob<A>, thread_pool: &P)
where A: AccessPolicy,
      P: edeltraud::ThreadPool<job::Job<A>>,
{
    if let Err(error) = job(sklave_job, thread_pool) {
        log::error!("terminated with an error: {error:?}");
    }
}

fn job<A, P>(mut sklave_job: SklaveJob<A>, thread_pool: &P) -> Result<(), Error>
where A: AccessPolicy,
      P: edeltraud::ThreadPool<job::Job<A>>,
{
    loop {
        let mut merger_kont = match sklave_job.sklavenwelt_mut().kont.take().unwrap() {
            Kont::Start { merger, } => {
                merger.step()
                    .map_err(Error::SearchRangesMerge)?
            },
            Kont::ProceedAwaitBlocks { next, } => {

                todo!();
                // if let Some(ReceivedBlockTask { async_token, block_bytes, }) = env.incoming.received_block_tasks.pop() {
                //     next.block_arrived(async_token, block_bytes)
                //         .map_err(Error::SearchRangesMerge)?
                // } else {
                //     return Ok(JobDone::AwaitRetrieveBlockTasks { env, next, });
                // },
            },
            Kont::ProceedItem { next, } =>
                next.proceed().map_err(Error::SearchRangesMerge)?,
        };

        loop {
            match merger_kont {
                search_ranges_merge::Kont::RequireBlockAsync(
                    search_ranges_merge::KontRequireBlockAsync { block_ref, async_token, next, },
                ) => {

                    todo!()
                    // let wheel_ref = env.wheels.get(block_ref.blockwheel_filename.clone())
                    //     .ok_or_else(|| Error::WheelNotFound {
                    //         blockwheel_filename: block_ref.blockwheel_filename.clone(),
                    //     })?;
                    // env.outgoing.retrieve_block_tasks.push(RetrieveBlockTask { wheel_ref, block_ref, async_token, });
                    // merger_kont = next.scheduled()
                    //     .map_err(Error::SearchRangesMerge)?;
                },
                search_ranges_merge::Kont::AwaitBlocks(search_ranges_merge::KontAwaitBlocks { next, }) => {
                    sklave_job.sklavenwelt_mut().kont =
                        Some(Kont::ProceedAwaitBlocks { next, });
                    break;
                },
                search_ranges_merge::Kont::BlockFinished(search_ranges_merge::KontBlockFinished { next, .. }) => {
                    merger_kont = next.proceed()
                        .map_err(Error::SearchRangesMerge)?;
                },
                search_ranges_merge::Kont::EmitDeprecated(search_ranges_merge::KontEmitDeprecated { next, .. }) => {
                    merger_kont = next.proceed()
                        .map_err(Error::SearchRangesMerge)?;
                },
                search_ranges_merge::Kont::EmitItem(
                    search_ranges_merge::KontEmitItem { item, next, },
                ) => {

                    todo!()
                    // return Ok(JobDone::ItemArrived { item, env, next, });
                },
                search_ranges_merge::Kont::Finished => {

                    todo!()
                    // assert!(env.outgoing.is_empty());
                    // assert!(env.incoming.is_empty());
                    // return Ok(JobDone::Finished);
                },
            }
        }
    }

}
