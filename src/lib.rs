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

use ero_blockwheel_fs as blockwheel;

pub mod kv;

mod proto;
mod context;
mod memcache;

#[derive(Clone, Debug)]
pub struct Params {
    pub kv_task_restart_sec: usize,
    pub memcache_task_restart_sec: usize,
}

impl Default for Params {
    fn default() -> Params {
        Params {
            kv_task_restart_sec: 4,
            memcache_task_restart_sec: 1,
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
        mut parent_supervisor: SupervisorPid,
        blockwheel_pid: blockwheel::Pid,
        params: Params,
    )
    {
        let memcache_gen_server = memcache::GenServer::new();
        let memcache_pid = memcache_gen_server.pid();
        let memcache_params = memcache::Params {
            task_restart_sec: params.memcache_task_restart_sec,
        };
        parent_supervisor.spawn_link_permanent(
            memcache_gen_server.run(memcache_params),
        );

        unimplemented!()
    }
}


#[derive(Debug)]
pub enum InsertError {
    GenServer(ero::NoProcError),
    NoSpaceLeft,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Inserted;

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

    pub async fn insert(&mut self, key_value: kv::KeyValue) -> Result<Inserted, InsertError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(proto::Request::Insert(proto::RequestInsert {
                    key_value: key_value.clone(),
                    context: reply_tx,
                }))
                .await
                .map_err(|_send_error| InsertError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(Ok(Inserted)) =>
                    return Ok(Inserted),
                Ok(Err(kv_context::RequestInsertError::NoSpaceLeft)) =>
                    return Err(InsertError::NoSpaceLeft),
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
        Inserted,
    };

    pub struct Context;

    impl context::Context for Context {
        type Info = oneshot::Sender<Info>;
        type Insert = oneshot::Sender<Result<Inserted, RequestInsertError>>;
    }

    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum RequestInsertError {
        NoSpaceLeft,
    }

}
