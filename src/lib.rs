#![forbid(unsafe_code)]

use std::time::Duration;

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
    supervisor::SupervisorPid,
};

use alloc_pool::bytes::BytesPool;

use ero_blockwheel_fs as blockwheel;

pub mod kv;
pub mod wheels;

mod core;
mod proto;
mod context;
mod storage;

#[cfg(test)]
mod tests;

#[derive(Clone, Debug)]
pub struct Params {
    pub tree_block_size: usize,
    pub main_task_restart_sec: usize,
    pub kv_task_restart_sec: usize,
    pub butcher_task_restart_sec: usize,
    pub manager_task_restart_sec: usize,
    pub search_tree_task_restart_sec: usize,
}

impl Default for Params {
    fn default() -> Params {
        Params {
            tree_block_size: 32,
            main_task_restart_sec: 4,
            kv_task_restart_sec: 2,
            butcher_task_restart_sec: 1,
            manager_task_restart_sec: 1,
            search_tree_task_restart_sec: 1,
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
        blocks_pool: BytesPool,
        wheels_pid: wheels::Pid,
        params: Params,
    )
    {
        let terminate_result = restart::restartable(
            ero::Params {
                name: "ero-blockwheel-kv main task",
                restart_strategy: RestartStrategy::Delay {
                    restart_after: Duration::from_secs(params.main_task_restart_sec as u64),
                },
            },
            State {
                parent_supervisor,
                blocks_pool,
                wheels_pid,
                fused_request_rx: self.fused_request_rx,
                params,
            },
            |mut state| async move {
                let butcher_gen_server = core::butcher::GenServer::new();
                let butcher_pid = butcher_gen_server.pid();
                let butcher_params = core::butcher::Params {
                    tree_block_size: state.params.tree_block_size,
                    task_restart_sec: state.params.butcher_task_restart_sec,
                };

                let manager_gen_server = core::manager::GenServer::new();
                let manager_pid = manager_gen_server.pid();
                let manager_params = core::manager::Params {
                    task_restart_sec: state.params.manager_task_restart_sec,
                    search_tree_params: core::search_tree::Params {
                        task_restart_sec: state.params.search_tree_task_restart_sec,
                        tree_block_size: state.params.tree_block_size,
                    },
                };

                let child_supervisor_gen_server = state.parent_supervisor.child_supevisor();
                let mut child_supervisor_pid = child_supervisor_gen_server.pid();
                state.parent_supervisor.spawn_link_temporary(
                    child_supervisor_gen_server.run(),
                );
                child_supervisor_pid.spawn_link_permanent(
                    butcher_gen_server.run(manager_pid.clone(), butcher_params),
                );
                child_supervisor_pid.spawn_link_permanent(
                    manager_gen_server.run(
                        child_supervisor_pid.clone(),
                        state.blocks_pool.clone(),
                        butcher_pid.clone(),
                        state.wheels_pid.clone(),
                        manager_params,
                    ),
                );

                busyloop(child_supervisor_pid, butcher_pid, manager_pid, state).await
            },
        ).await;
        if let Err(error) = terminate_result {
            log::error!("fatal error: {:?}", error);
        }
    }
}

#[derive(Debug)]
pub enum InsertError {
    GenServer(ero::NoProcError),
}

#[derive(Debug)]
pub enum LookupError {
    GenServer(ero::NoProcError),
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

    pub async fn insert(&mut self, key: kv::Key, value: kv::Value) -> Result<Inserted, InsertError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(proto::Request::Insert(proto::RequestInsert {
                    key: key.clone(),
                    value: value.clone(),
                    context: reply_tx,
                }))
                .await
                .map_err(|_send_error| InsertError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(Ok(Inserted)) =>
                    return Ok(Inserted),
                Ok(Err(..)) =>
                    unreachable!(),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn lookup(&mut self, key: kv::Key) -> Result<Option<kv::Value>, LookupError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(proto::Request::Lookup(proto::RequestLookup {
                    key: key.clone(),
                    context: reply_tx,
                }))
                .await
                .map_err(|_send_error| LookupError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(Ok(result)) =>
                    return Ok(result),
                Ok(Err(..)) =>
                    unreachable!(),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }
}

struct State {
    parent_supervisor: SupervisorPid,
    blocks_pool: BytesPool,
    wheels_pid: wheels::Pid,
    fused_request_rx: stream::Fuse<mpsc::Receiver<Request>>,
    params: Params,
}

#[derive(Debug)]
enum Error {
}

async fn busyloop(
    _child_supervisor_pid: SupervisorPid,
    mut butcher_pid: core::butcher::Pid,
    mut manager_pid: core::manager::Pid,
    mut state: State,
)
    -> Result<(), ErrorSeverity<State, Error>>
{
    while let Some(request) = state.fused_request_rx.next().await {
        match request {
            proto::Request::Info(proto::RequestInfo { context: reply_tx, }) => {
                let info = match butcher_pid.info().await {
                    Ok(info) =>
                        info,
                    Err(ero::NoProcError) => {
                        log::warn!("butcher has gone during info, terminating");
                        break;
                    },
                };
                if let Err(_send_error) = reply_tx.send(info) {
                    log::warn!("client canceled info request");
                }

                // more info from manager?
                unimplemented!()
            },

            proto::Request::Insert(proto::RequestInsert { key, value, context: reply_tx, }) => {
                let status = match butcher_pid.insert(key, value).await {
                    Ok(Inserted) =>
                        Ok(Inserted),
                    Err(ero::NoProcError) => {
                        log::warn!("butcher has gone during flush, terminating");
                        break;
                    },
                };
                if let Err(_send_error) = reply_tx.send(status) {
                    log::warn!("client canceled insert request");
                }
            },

            proto::Request::Lookup(proto::RequestLookup { key, context: reply_tx, }) => {
                let status = match manager_pid.lookup(key).await {
                    Ok(result) =>
                        Ok(result),
                    Err(core::manager::LookupError::GenServer(ero::NoProcError)) => {
                        log::warn!("manager has gone during flush, terminating");
                        break;
                    },
                };
                if let Err(_send_error) = reply_tx.send(status) {
                    log::warn!("client canceled lookup request");
                }
            },
        }
    }
    Ok(())
}

mod kv_context {
    use futures::{
        channel::{
            oneshot,
        },
    };

    use super::{
        kv,
        context,
        Info,
        Inserted,
    };

    pub struct Context;

    impl context::Context for Context {
        type Info = oneshot::Sender<Info>;
        type Insert = oneshot::Sender<Result<Inserted, RequestInsertError>>;
        type Lookup = oneshot::Sender<Result<Option<kv::Value>, RequestLookupError>>;
    }

    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum RequestInsertError {
    }

    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum RequestLookupError {
    }
}
