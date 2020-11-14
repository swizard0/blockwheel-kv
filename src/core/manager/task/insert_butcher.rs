use crate::{
    core::{
        butcher,
        RequestInsert,
    },
};

pub struct Args {
    pub request: RequestInsert,
    pub butcher_pid: butcher::Pid,
}

pub struct Done;

#[derive(Debug)]
pub enum Error {
    ButcherInsert(ero::NoProcError),
}

pub async fn run(Args { request: RequestInsert { key, value, reply_tx, }, mut butcher_pid, }: Args) -> Result<Done, Error> {
    let inserted = butcher_pid.insert(key, value).await
        .map_err(Error::ButcherInsert)?;
    if let Err(_send_error) = reply_tx.send(inserted) {
        log::warn!("client canceled insert request");
    }
    Ok(Done)
}
