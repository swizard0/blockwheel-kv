use crate::{
    core::{
        butcher,
    },
    RequestRemove,
};

pub struct Args {
    pub request: RequestRemove,
    pub butcher_pid: butcher::Pid,
}

pub struct Done;

#[derive(Debug)]
pub enum Error {
    ButcherRemove(ero::NoProcError),
}

pub async fn run(Args { request: RequestRemove { key, reply_tx, }, mut butcher_pid, }: Args) -> Result<Done, Error> {
    let removed = butcher_pid.remove(key).await
        .map_err(Error::ButcherRemove)?;
    if let Err(_send_error) = reply_tx.send(removed) {
        log::warn!("client canceled remove request");
    }
    Ok(Done)
}
