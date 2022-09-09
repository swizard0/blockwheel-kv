use crate::{
    job,
    core::{
        performer,
        performer_sklave::{
            Welt,
            Error,
            Context,
        },
    },
    AccessPolicy,
};

pub struct WeltState<A> where A: AccessPolicy {
    kont: Kont<A>,
}

impl<A> WeltState<A> where A: AccessPolicy {
    pub fn new(performer: performer::Performer<Context<A>>) -> Self {
        WeltState {
            kont: Kont::Initialize { performer, },
        }
    }
}

pub enum Outcome<A> where A: AccessPolicy {
    Rasten { running: WeltState<A>, },
}

enum Kont<A> where A: AccessPolicy {
    Initialize { performer: performer::Performer<Context<A>>, },
}

pub fn job<A, P>(
    mut welt_state: WeltState<A>,
    sklavenwelt: &mut Welt<A>,
    thread_pool: &P,
)
    -> Result<Outcome<A>, Error>
where A: AccessPolicy,
      P: edeltraud::ThreadPool<job::Job<A>>,
{

    todo!();
}
