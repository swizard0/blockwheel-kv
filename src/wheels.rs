use std::{
    path,
    sync::Arc,
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

use ero::{
    restart,
    ErrorSeverity,
    RestartStrategy,
};

use super::{
    blockwheel,
};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct WheelFilename {
    pub filename: Arc<String>,
}

impl<'a> From<&'a str> for WheelFilename {
    fn from(filename: &'a str) -> WheelFilename {
        WheelFilename { filename: Arc::new(filename.to_string()), }
    }
}

impl<'a> From<&'a path::Path> for WheelFilename {
    fn from(filename: &'a path::Path) -> WheelFilename {
        WheelFilename {
            filename: Arc::new(filename.to_string_lossy().to_string()),
        }
    }
}

impl From<path::PathBuf> for WheelFilename {
    fn from(filename: path::PathBuf) -> WheelFilename {
        WheelFilename {
            filename: Arc::new(filename.to_string_lossy().to_string()),
        }
    }
}

impl Deref for WheelFilename {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &*self.filename
    }
}

#[derive(Clone)]
pub struct WheelRef {
    pub blockwheel_filename: WheelFilename,
    pub blockwheel_pid: blockwheel::Pid,
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

    pub async fn run(self, params: Params) {
        let terminate_result = restart::restartable(
            ero::Params {
                name: "ero-blockwheel-kv wheels task",
                restart_strategy: RestartStrategy::Delay {
                    restart_after: Duration::from_secs(params.task_restart_sec as u64),
                },
            },
            State {
                request_rx: self.request_rx,
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
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Flushed;

impl Pid {
    pub async fn add(&mut self, wheel_ref: WheelRef) -> Result<bool, ero::NoProcError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx.send(Request::Add { wheel_ref: wheel_ref.clone(), reply_tx, }).await
                .map_err(|_send_error| ero::NoProcError)?;
            match reply_rx.await {
                Ok(has_added) =>
                    return Ok(has_added),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

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
}

enum Request {
    Add { wheel_ref: WheelRef, reply_tx: oneshot::Sender<bool>, },
    Acquire { reply_tx: oneshot::Sender<Option<WheelRef>>, },
    Get { blockwheel_filename: WheelFilename, reply_tx: oneshot::Sender<Option<WheelRef>>, },
    Flush { reply_tx: oneshot::Sender<Flushed>, },
}

#[derive(Debug)]
enum Error {
    WheelGoneDuringFlush { blockwheel_filename: WheelFilename, },
}

async fn busyloop(mut state: State) -> Result<(), ErrorSeverity<State, Error>> {
    let mut wheels = Vec::new();
    let mut index = HashMap::new();

    while let Some(request) = state.request_rx.next().await {
        match request {
            Request::Add { wheel_ref, reply_tx, } => {
                let offset = wheels.len();
                let filename = wheel_ref.blockwheel_filename.clone();
                let has_added = match index.entry(filename.clone()) {
                    hash_map::Entry::Vacant(ve) => {
                        ve.insert(offset);
                        wheels.push(wheel_ref);
                        true
                    },
                    hash_map::Entry::Occupied(..) =>
                        false,
                };
                if let Err(_send_error) = reply_tx.send(has_added) {
                    log::warn!("client canceled add request");
                    index.remove(&filename);
                    wheels.pop();
                }
            },

            Request::Acquire { reply_tx, } => {
                let maybe_wheel_ref = if wheels.is_empty() {
                    None
                } else {
                    let mut rng = rand::thread_rng();
                    let offset = rng.gen_range(0, wheels.len());
                    Some(wheels[offset].clone())
                };
                if let Err(_send_error) = reply_tx.send(maybe_wheel_ref) {
                    log::warn!("client canceled add request");
                }
            },

            Request::Get { blockwheel_filename, reply_tx, } => {
                let maybe_wheel_ref = index.get(&blockwheel_filename)
                    .map(|&offset| wheels[offset].clone());
                if let Err(_send_error) = reply_tx.send(maybe_wheel_ref) {
                    log::warn!("client canceled get request");
                }
            },

            Request::Flush { reply_tx, } => {
                let mut flush_tasks = FuturesUnordered::new();
                for WheelRef { blockwheel_pid, blockwheel_filename, } in &wheels {
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
        }
    }

    Ok(())
}
