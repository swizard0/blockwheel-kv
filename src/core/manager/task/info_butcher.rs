
use o1::set::Ref;

use crate::{
    Info,
    core::{
        butcher,
    },
};

pub struct Args {
    pub request_ref: Ref,
    pub butcher_pid: butcher::Pid,
}

pub struct Done {
    pub request_ref: Ref,
    pub info: Info,
}

#[derive(Debug)]
pub enum Error {
    ButcherInfo(ero::NoProcError),
}

pub async fn run(Args { request_ref, mut butcher_pid, }: Args) -> Result<Done, Error> {
    let info = butcher_pid.info().await
        .map_err(Error::ButcherInfo)?;
    Ok(Done { request_ref, info, })
}
