#![forbid(unsafe_code)]

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
    RestartStrategy,
    supervisor::SupervisorPid,
};

use ero_blockwheel as blockwheel;

mod proto;
mod context;

#[derive(Clone, Debug)]
pub struct Params {
    pub kv_task_restart_sec: usize,
}

impl Default for Params {
    fn default() -> Params {
        Params {
            kv_task_restart_sec: 4,
        }
    }
}

type Request = proto::Request<kv_context::Context>;

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
        parent_supervisor: SupervisorPid,
        blockwheel_pid: blockwheel::Pid,
        params: Params,
    )
    {

        unimplemented!()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, Debug)]
pub struct Info {
}

impl Pid {
    pub async fn info(&mut self) -> Result<Info, ero::NoProcError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx.send(Request::Info(proto::RequestInfo { context: reply_tx, })).await
                .map_err(|_send_error| ero::NoProcError)?;
            match reply_rx.await {
                Ok(info) =>
                    return Ok(info),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }
}

mod kv_context {
    use futures::{
        channel::{
            oneshot,
        },
        future,
    };

    use super::{
        context,
        Info,
    };

    pub struct Context;

    impl context::Context for Context {
        type Info = oneshot::Sender<Info>;
    }
}
