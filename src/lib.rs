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

use alloc_pool::bytes::BytesPool;

use ero_blockwheel_fs as blockwheel;

pub mod kv;

mod proto;
mod context;
mod storage;
mod manager;
mod memcache;

#[derive(Clone, Debug)]
pub struct Params {
    pub tree_block_size: usize,
    pub kv_task_restart_sec: usize,
    pub memcache_task_restart_sec: usize,
    pub manager_task_restart_sec: usize,
}

impl Default for Params {
    fn default() -> Params {
        Params {
            tree_block_size: 32,
            kv_task_restart_sec: 4,
            memcache_task_restart_sec: 1,
            manager_task_restart_sec: 1,
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
        blocks_pool: BytesPool,
        blockwheel_pid: blockwheel::Pid,
        params: Params,
    )
    {
        let memcache_gen_server = memcache::GenServer::new();
        let memcache_pid = memcache_gen_server.pid();
        let memcache_params = memcache::Params {
            tree_block_size: params.tree_block_size,
            task_restart_sec: params.memcache_task_restart_sec,
        };

        let manager_gen_server = manager::GenServer::new();
        let manager_pid = manager_gen_server.pid();
        let manager_params = manager::Params {
            task_restart_sec: params.manager_task_restart_sec,
        };

        parent_supervisor.spawn_link_permanent(
            memcache_gen_server.run(manager_pid.clone(), memcache_params),
        );
        parent_supervisor.spawn_link_permanent(
            manager_gen_server.run(
                blocks_pool.clone(),
                blockwheel_pid.clone(),
                manager_params,
            ),
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
