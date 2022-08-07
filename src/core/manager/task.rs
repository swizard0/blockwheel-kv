use crate::{
    job,
};

use alloc_pool::{
    bytes::{
        Bytes,
    },
};

use ero_blockwheel_fs as blockwheel;

use crate::{
    wheels,
};

pub mod performer;
pub mod flush_butcher;
pub mod lookup_range_merge;

pub enum TaskArgs<J> where J: edeltraud::Job {
    Performer(performer::Args<J>),
    FlushButcher(flush_butcher::Args<J>),
    LookupRangeMerge(lookup_range_merge::Args<J>),
}

pub enum TaskDone {
    Performer(performer::Done),
    FlushButcher(flush_butcher::Done),
    LookupRangeMerge(lookup_range_merge::Done),
}

#[derive(Debug)]
pub enum Error {
    Performer(performer::Error),
    FlushButcher(flush_butcher::Error),
    LookupRangeMerge(lookup_range_merge::Error),
}

pub async fn run_args<J>(args: TaskArgs<J>) -> Result<TaskDone, Error>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    Ok(match args {
        TaskArgs::Performer(args) =>
            TaskDone::Performer(
                performer::run(args).await
                    .map_err(Error::Performer)?,
            ),
        TaskArgs::FlushButcher(args) =>
            TaskDone::FlushButcher(
                flush_butcher::run(args).await
                    .map_err(Error::FlushButcher)?,
            ),
        TaskArgs::LookupRangeMerge(args) =>
            TaskDone::LookupRangeMerge(
                lookup_range_merge::run(args).await
                    .map_err(Error::LookupRangeMerge)?,
            ),
    })
}

#[cfg(test)]
use std::{
    sync::{
        Arc,
        Mutex,
    },
};

#[derive(Clone)]
enum BlockwheelPid {
    Regular(blockwheel::Pid),
    #[cfg(test)]
    Custom {
        write_block: Arc<Mutex<dyn FnMut(Bytes) -> blockwheel::block::Id + Send + 'static>>,
        read_block: Arc<Mutex<dyn FnMut(blockwheel::block::Id) -> Bytes + Send + 'static>>,
    },
}

#[derive(Clone)]
enum WheelsPid {
    Regular(wheels::Pid),
    #[cfg(test)]
    Custom {
        acquire: Arc<Mutex<dyn FnMut() -> WheelRef + Send + 'static>>,
        get: Arc<Mutex<dyn FnMut(wheels::WheelFilename) -> WheelRef + Send + 'static>>,
    },
}

struct WheelRef {
    blockwheel_filename: wheels::WheelFilename,
    blockwheel_pid: BlockwheelPid,
}

impl BlockwheelPid {
    async fn write_block(&mut self, block_bytes: Bytes) -> Result<blockwheel::block::Id, blockwheel::WriteBlockError> {
        match self {
            BlockwheelPid::Regular(blockwheel_pid) =>
                blockwheel_pid.write_block(block_bytes).await,
            #[cfg(test)]
            BlockwheelPid::Custom { write_block: custom_write_fn, .. } => {
                let mut fn_lock = custom_write_fn.lock().unwrap();
                Ok(fn_lock(block_bytes))
            },
        }
    }

    async fn read_block(&mut self, block_id: blockwheel::block::Id) -> Result<Bytes, blockwheel::ReadBlockError> {
        match self {
            BlockwheelPid::Regular(blockwheel_pid) =>
                blockwheel_pid.read_block(block_id).await,
            #[cfg(test)]
            BlockwheelPid::Custom { read_block: custom_write_fn, .. } => {
                let mut fn_lock = custom_write_fn.lock().unwrap();
                Ok(fn_lock(block_id))
            },
        }
    }
}

impl WheelsPid {
    async fn acquire(&mut self) -> Result<Option<WheelRef>, ero::NoProcError> {
        match self {
            WheelsPid::Regular(wheels_pid) =>
                match wheels_pid.acquire().await? {
                    None =>
                        Ok(None),
                    Some(wheel_ref) =>
                        Ok(Some(WheelRef {
                            blockwheel_filename: wheel_ref.blockwheel_filename,
                            blockwheel_pid: BlockwheelPid::Regular(wheel_ref.blockwheel_pid),
                        })),
                },
            #[cfg(test)]
            WheelsPid::Custom { acquire: custom_acquire_fn, .. } => {
                let mut fn_lock = custom_acquire_fn.lock().unwrap();
                Ok(Some(fn_lock()))
            },
        }
    }

    pub async fn get(&mut self, blockwheel_filename: wheels::WheelFilename) -> Result<Option<WheelRef>, ero::NoProcError> {
        match self {
            WheelsPid::Regular(wheels_pid) =>
                match wheels_pid.get(blockwheel_filename).await? {
                    None =>
                        Ok(None),
                    Some(wheel_ref) =>
                        Ok(Some(WheelRef {
                            blockwheel_filename: wheel_ref.blockwheel_filename,
                            blockwheel_pid: BlockwheelPid::Regular(wheel_ref.blockwheel_pid),
                        })),
                },
            #[cfg(test)]
            WheelsPid::Custom { get: custom_acquire_fn, .. } => {
                let mut fn_lock = custom_acquire_fn.lock().unwrap();
                Ok(Some(fn_lock(blockwheel_filename)))
            },
        }
    }
}
