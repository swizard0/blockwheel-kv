use futures::{
    channel::{
        oneshot,
    },
};

use ero_blockwheel_fs as blockwheel;

use crate::{
    kv,
    wheels,
    storage,
};

pub struct Args {
    pub found_fold: Option<kv::ValueCell<storage::OwnedValueBlockRef>>,
    pub reply_tx: oneshot::Sender<Option<kv::ValueCell<kv::Value>>>,
    pub wheels_pid: wheels::Pid,
}

pub struct Done;

#[derive(Debug)]
pub enum Error {
    WheelsGone,
    WheelNotFound {
        blockwheel_filename: wheels::WheelFilename,
    },
    ReadBlock(blockwheel::ReadBlockError),
    ValueDeserialize(storage::Error),
}

pub async fn run(Args { found_fold, reply_tx, mut wheels_pid, }: Args) -> Result<Done, Error> {
    let lookup_result = match found_fold {
        None =>
            None,
        Some(kv::ValueCell { version, cell: kv::Cell::Value(storage::OwnedValueBlockRef::Inline(value)), }) =>
            Some(kv::ValueCell { version, cell: kv::Cell::Value(value), }),
        Some(kv::ValueCell { version, cell: kv::Cell::Value(storage::OwnedValueBlockRef::Ref(block_ref)), }) => {
            let mut wheel_ref = wheels_pid.get(block_ref.blockwheel_filename.clone()).await
                .map_err(|ero::NoProcError| Error::WheelsGone)?
                .ok_or_else(|| Error::WheelNotFound {
                    blockwheel_filename: block_ref.blockwheel_filename.clone(),
                })?;
            match wheel_ref.blockwheel_pid.read_block(block_ref.block_id.clone()).await {
                Ok(block_bytes) => {
                    let value_bytes = storage::value_block_deserialize(&block_bytes)
                        .map_err(Error::ValueDeserialize)?;
                    Some(kv::ValueCell {
                        version,
                        cell: kv::Cell::Value(value_bytes.into()),
                    })
                },
                Err(blockwheel::ReadBlockError::NotFound) => {
                    log::debug!("externally refereced block {:?} but blockwheel read returns none", block_ref);
                    // value is already gone: assume kv pair has been deleted
                    None
                },
                Err(error) =>
                    return Err(Error::ReadBlock(error)),
            }
        },
        Some(kv::ValueCell { version, cell: kv::Cell::Tombstone, }) =>
            Some(kv::ValueCell { version, cell: kv::Cell::Tombstone, }),
    };
    if let Err(_send_error) = reply_tx.send(lookup_result) {
        log::warn!("client canceled lookup request");
    }
    Ok(Done)
}
