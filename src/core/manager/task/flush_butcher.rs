
use o1::set::Ref;

use crate::{
    core::{
        butcher,
    },
    Flushed,
};

pub struct Args {
    pub request_ref: Ref,
    pub butcher_pid: butcher::Pid,
}

pub struct Done {
    pub request_ref: Ref,
}

#[derive(Debug)]
pub enum Error {
    ButcherFlush(ero::NoProcError),
}

pub async fn run(Args { request_ref, mut butcher_pid, }: Args) -> Result<Done, Error> {
    let Flushed = butcher_pid.flush().await
        .map_err(Error::ButcherFlush)?;
    Ok(Done { request_ref, })
}
