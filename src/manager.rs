use std::{
    sync::Arc,
    time::Duration,
};

use futures::{
    stream,
    channel::{
        mpsc,
        oneshot,
    },
    StreamExt,
    SinkExt,
};

use ero::{
    restart,
    ErrorSeverity,
    RestartStrategy,
};

use alloc_pool::bytes::BytesPool;

use ero_blockwheel_fs as blockwheel;

use super::{
    memcache,
};

#[derive(Clone, Debug)]
pub struct Params {
    pub task_restart_sec: usize,
}

impl Default for Params {
    fn default() -> Params {
        Params {
            task_restart_sec: 4,
        }
    }
}

pub struct GenServer {
    request_tx: mpsc::Sender<Request>,
    fused_request_rx: stream::Fuse<mpsc::Receiver<Request>>,
}

#[derive(Clone)]
pub struct Pid {
    request_tx: mpsc::Sender<Request>,
}

impl GenServer {
    pub fn new() -> GenServer {
        let (request_tx, request_rx) = mpsc::channel(0);
        GenServer {
            request_tx,
            fused_request_rx: request_rx.fuse(),
        }
    }

    pub fn pid(&self) -> Pid {
        Pid {
            request_tx: self.request_tx.clone(),
        }
    }

    pub async fn run(
        self,
        blocks_pool: BytesPool,
        blockwheel_pid: blockwheel::Pid,
        params: Params,
    )
    {
        let terminate_result = restart::restartable(
            ero::Params {
                name: "ero-blockwheel-kv manager task",
                restart_strategy: RestartStrategy::Delay {
                    restart_after: Duration::from_secs(params.task_restart_sec as u64),
                },
            },
            State {
                fused_request_rx: self.fused_request_rx,
                blocks_pool,
                params,
            },
            |mut state| busyloop(state),
        ).await;
        if let Err(error) = terminate_result {
            log::error!("fatal error: {:?}", error);
        }
    }
}

enum Request {
    MemcacheFlush {
        cache: Arc<memcache::Cache>,
        current_block_size: usize,
    },
}

struct State {
    fused_request_rx: stream::Fuse<mpsc::Receiver<Request>>,
    blocks_pool: BytesPool,
    params: Params,
}

#[derive(Debug)]
enum Error {
}

async fn busyloop(mut state: State) -> Result<(), ErrorSeverity<State, Error>> {
    while let Some(request) = state.fused_request_rx.next().await {
        match request {
            Request::MemcacheFlush { cache, current_block_size, } =>
                unimplemented!(),
        }
    }
    Ok(())
}
