
use o1::set::Ref;

use crate::{
    kv,
    core::{
        butcher,
    },
};

pub struct Args {
    pub key: kv::Key,
    pub request_ref: Ref,
    pub butcher_pid: butcher::Pid,
}

pub struct Done {
    pub request_ref: Ref,
    pub found: Option<kv::ValueCell>,
}

#[derive(Debug)]
pub enum Error {
    ButcherLookup(ero::NoProcError),
}

pub async fn run(Args { request_ref, key, mut butcher_pid, }: Args) -> Result<Done, Error> {
    let maybe_value_cell = butcher_pid.lookup(key).await
        .map_err(Error::ButcherLookup)?;
    Ok(Done { request_ref, found: maybe_value_cell, })
}
