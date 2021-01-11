
use o1::set::Ref;

use crate::{
    kv,
    storage,
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
    pub found: Option<kv::ValueCell<storage::OwnedValueBlockRef>>,
}

#[derive(Debug)]
pub enum Error {
    ButcherLookup(ero::NoProcError),
}

pub async fn run(Args { request_ref, key, mut butcher_pid, }: Args) -> Result<Done, Error> {
    match butcher_pid.lookup(key).await {
        Ok(None) =>
            Ok(Done { request_ref, found: None, }),
        Ok(Some(value_cell)) =>
            Ok(Done { request_ref, found: Some(value_cell.into()), }),
        Err(error) =>
            Err(Error::ButcherLookup(error)),
    }
}
