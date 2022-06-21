use std::{
    fmt,
    str,
    path,
    ops::Deref,
    time::Duration,
    collections::{
        HashMap,
        hash_map,
    },
};

use futures::{
    channel::{
        mpsc,
        oneshot,
    },
    stream::{
        FuturesUnordered,
    },
    StreamExt,
    SinkExt,
};

use rand::Rng;

use alloc_pool::{
    bytes::{
        Bytes,
        BytesPool,
    },
};

use ero::{
    restart,
    ErrorSeverity,
    RestartStrategy,
};

use ero_blockwheel_fs::{
    self as blockwheel,
    block,
};

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct WheelFilename {
    filename_bytes: Bytes,
}

impl From<Bytes> for WheelFilename {
    fn from(filename_bytes: Bytes) -> WheelFilename {
        WheelFilename { filename_bytes, }
    }
}

impl WheelFilename {
    pub fn from_str(filename: &str, blocks_pool: &BytesPool) -> WheelFilename {
        let mut block_bytes = blocks_pool.lend();
        block_bytes.extend_from_slice(filename.as_bytes());
        WheelFilename {
            filename_bytes: block_bytes.freeze(),
        }
    }

    pub fn from_path<P>(filename: P, blocks_pool: &BytesPool) -> WheelFilename where P: AsRef<path::Path> {
        WheelFilename::from_str(&filename.as_ref().to_string_lossy(), blocks_pool)
    }
}

impl fmt::Debug for WheelFilename {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_tuple("WheelFilename")
            .field(&str::from_utf8(&self.filename_bytes).unwrap_or("<invalid utf>"))
            .finish()
    }
}

impl Deref for WheelFilename {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl AsRef<[u8]> for WheelFilename {
    fn as_ref(&self) -> &[u8] {
        &self.filename_bytes
    }
}

#[derive(Clone)]
pub struct WheelRef {
    pub blockwheel_filename: WheelFilename,
    pub blockwheel_pid: blockwheel::Pid,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct BlockRef {
    pub blockwheel_filename: WheelFilename,
    pub block_id: block::Id,
}

#[derive(Clone, Debug)]
pub struct Params {
    pub task_restart_sec: usize,
}

impl Default for Params {
    fn default() -> Params {
        Params {
            task_restart_sec: 1,
        }
    }
}

pub struct GenServer {
    request_tx: mpsc::Sender<Request>,
    request_rx: mpsc::Receiver<Request>,
}

#[derive(Clone)]
pub struct Pid {
    request_tx: mpsc::Sender<Request>,
}

impl GenServer {
    pub fn new() -> GenServer {
        let (request_tx, request_rx) = mpsc::channel(0);
        GenServer { request_tx, request_rx, }
    }

    pub fn pid(&self) -> Pid {
        Pid {
            request_tx: self.request_tx.clone(),
        }
    }

    pub async fn run<I>(
        self,
        wheels_iter: I,
        params: Params,
    ) where I: IntoIterator<Item = WheelRef>
    {
        let mut wheels = Vec::new();
        let mut index = HashMap::new();
        for wheel_ref in wheels_iter {
            let offset = wheels.len();
            let filename = wheel_ref.blockwheel_filename.clone();
            match index.entry(filename.clone()) {
                hash_map::Entry::Vacant(ve) => {
                    ve.insert(offset);
                    wheels.push(wheel_ref);
                },
                hash_map::Entry::Occupied(..) =>
                    (),
            }
        }

        let terminate_result = restart::restartable(
            ero::Params {
                name: "ero-blockwheel-kv wheels task",
                restart_strategy: RestartStrategy::Delay {
                    restart_after: Duration::from_secs(params.task_restart_sec as u64),
                },
            },
            State {
                request_rx: self.request_rx,
                wheels,
                index,
            },
            |state| busyloop(state),
        ).await;
        if let Err(error) = terminate_result {
            log::error!("fatal error: {:?}", error);
        }
    }
}

struct State {
    request_rx: mpsc::Receiver<Request>,
    wheels: Vec<WheelRef>,
    index: HashMap<WheelFilename, usize>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Flushed;

#[derive(Debug)]
pub enum IterBlocksError {
    GenServer(ero::NoProcError),
}

pub struct IterBlocks {
    pub block_refs_rx: mpsc::Receiver<IterBlocksItem>,
}

pub enum IterBlocksItem {
    Block { block_ref: BlockRef, block_bytes: Bytes, },
    NoMoreBlocks,
}

impl Pid {
    pub async fn acquire(&mut self) -> Result<Option<WheelRef>, ero::NoProcError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx.send(Request::Acquire { reply_tx, }).await
                .map_err(|_send_error| ero::NoProcError)?;
            match reply_rx.await {
                Ok(maybe_wheel_ref) =>
                    return Ok(maybe_wheel_ref),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn get(&mut self, blockwheel_filename: WheelFilename) -> Result<Option<WheelRef>, ero::NoProcError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx.send(Request::Get { blockwheel_filename: blockwheel_filename.clone(), reply_tx, }).await
                .map_err(|_send_error| ero::NoProcError)?;
            match reply_rx.await {
                Ok(maybe_wheel_ref) =>
                    return Ok(maybe_wheel_ref),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn flush(&mut self) -> Result<Flushed, ero::NoProcError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx.send(Request::Flush { reply_tx, }).await
                .map_err(|_send_error| ero::NoProcError)?;
            match reply_rx.await {
                Ok(Flushed) =>
                    return Ok(Flushed),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn iter_blocks(&mut self) -> Result<IterBlocks, IterBlocksError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx.send(Request::IterBlocks { reply_tx, }).await
                .map_err(|_send_error| IterBlocksError::GenServer(ero::NoProcError))?;
            match reply_rx.await {
                Ok(iter_blocks) =>
                    return Ok(iter_blocks),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }
}

enum Request {
    Acquire { reply_tx: oneshot::Sender<Option<WheelRef>>, },
    Get { blockwheel_filename: WheelFilename, reply_tx: oneshot::Sender<Option<WheelRef>>, },
    Flush { reply_tx: oneshot::Sender<Flushed>, },
    IterBlocks { reply_tx: oneshot::Sender<IterBlocks>, },
}

#[derive(Debug)]
pub enum Error {
    WheelGoneDuringFlush {
        blockwheel_filename: WheelFilename,
    },
    WheelIterBlocks {
        blockwheel_filename: WheelFilename,
        error: blockwheel::IterBlocksError,
    },
    WheelIterBlocksRxDropped {
        blockwheel_filename: WheelFilename,
    },
}

async fn busyloop(mut state: State) -> Result<(), ErrorSeverity<State, Error>> {
    while let Some(request) = state.request_rx.next().await {
        match request {
            Request::Acquire { reply_tx, } => {
                let maybe_wheel_ref = if state.wheels.is_empty() {
                    None
                } else {
                    let mut rng = rand::thread_rng();
                    let offset = rng.gen_range(0 .. state.wheels.len());
                    Some(state.wheels[offset].clone())
                };
                if let Err(_send_error) = reply_tx.send(maybe_wheel_ref) {
                    log::warn!("client canceled add request");
                }
            },

            Request::Get { blockwheel_filename, reply_tx, } => {
                let maybe_wheel_ref = state.index.get(&blockwheel_filename)
                    .map(|&offset| state.wheels[offset].clone());
                if let Err(_send_error) = reply_tx.send(maybe_wheel_ref) {
                    log::warn!("client canceled get request");
                }
            },

            Request::Flush { reply_tx, } => {
                let mut flush_tasks = FuturesUnordered::new();
                for WheelRef { blockwheel_pid, blockwheel_filename, } in &state.wheels {
                    let mut blockwheel_pid = blockwheel_pid.clone();
                    let blockwheel_filename = blockwheel_filename.clone();
                    flush_tasks.push(async move {
                        let status = blockwheel_pid.flush().await;
                        (blockwheel_filename, status)
                    });
                }
                while let Some((blockwheel_filename, flush_status)) = flush_tasks.next().await {
                    match flush_status {
                        Ok(blockwheel::Flushed) =>
                            (),
                        Err(ero::NoProcError) =>
                            return Err(ErrorSeverity::Fatal(Error::WheelGoneDuringFlush { blockwheel_filename, })),
                    }
                }
                if let Err(_send_error) = reply_tx.send(Flushed) {
                    log::warn!("client canceled flush request");
                }
            },

            Request::IterBlocks { reply_tx, } => {
                let (mut block_refs_tx, block_refs_rx) = mpsc::channel(0);
                if let Err(_send_error) = reply_tx.send(IterBlocks { block_refs_rx, }) {
                    log::warn!("client canceled iter request");
                    continue;
                }

                enum TaskDone { Finished, Canceled, }
                let mut iter_tasks = FuturesUnordered::new();
                for WheelRef { blockwheel_pid, blockwheel_filename, } in &state.wheels {
                    let mut blockwheel_pid = blockwheel_pid.clone();
                    let blockwheel_filename = blockwheel_filename.clone();
                    let mut block_refs_tx = block_refs_tx.clone();
                    iter_tasks.push(async move {
                        let mut iter_blocks = blockwheel_pid.iter_blocks().await
                            .map_err(|error| Error::WheelIterBlocks {
                                blockwheel_filename: blockwheel_filename.clone(),
                                error,
                            })?;
                        loop {
                            match iter_blocks.blocks_rx.next().await {
                                None =>
                                    return Err(Error::WheelIterBlocksRxDropped {
                                        blockwheel_filename,
                                    }),
                                Some(blockwheel::IterBlocksItem::Block { block_id, block_bytes, }) => {
                                    let block_ref = BlockRef {
                                        blockwheel_filename: blockwheel_filename.clone(),
                                        block_id,
                                    };
                                    let item = IterBlocksItem::Block { block_ref, block_bytes, };
                                    if let Err(_send_error) = block_refs_tx.send(item).await {
                                        return Ok(TaskDone::Canceled);
                                    }
                                },
                                Some(blockwheel::IterBlocksItem::NoMoreBlocks) =>
                                    break,
                            }
                        }
                        Ok(TaskDone::Finished)
                    });
                }
                loop {
                    match iter_tasks.next().await {
                        None => {
                            if let Err(_send_error) = block_refs_tx.send(IterBlocksItem::NoMoreBlocks).await {
                                log::warn!("client canceled iter request");
                            }
                            break;
                        },
                        Some(Err(error)) =>
                            return Err(ErrorSeverity::Fatal(error)),
                        Some(Ok(TaskDone::Canceled)) => {
                            log::warn!("client canceled iter request");
                            break;
                        },
                        Some(Ok(TaskDone::Finished)) =>
                            (),
                    }
                }
            },
        }
    }

    Ok(())
}
