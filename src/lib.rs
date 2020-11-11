#![forbid(unsafe_code)]

use std::{
    ops::AddAssign,
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
    supervisor::SupervisorPid,
};

use alloc_pool::bytes::BytesPool;

use ero_blockwheel_fs as blockwheel;

pub mod kv;
pub mod wheels;
pub mod version;

mod core;
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
    pub search_tree_remove_tasks_limit: usize,
    pub search_tree_iter_send_buffer: usize,
    pub standalone_search_trees_count: usize,
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
            search_tree_remove_tasks_limit: 64,
            search_tree_iter_send_buffer: 4,
            standalone_search_trees_count: 2,
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
        parent_supervisor: SupervisorPid,
        blocks_pool: BytesPool,
        version_provider: version::Provider,
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
                version_provider,
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
                        remove_tasks_limit: state.params.search_tree_remove_tasks_limit,
                        iter_send_buffer: state.params.search_tree_iter_send_buffer,
                    },
                    standalone_search_trees_count: state.params.standalone_search_trees_count,
                };

                let child_supervisor_gen_server = state.parent_supervisor.child_supevisor();
                let mut child_supervisor_pid = child_supervisor_gen_server.pid();
                state.parent_supervisor.spawn_link_temporary(
                    child_supervisor_gen_server.run(),
                );
                child_supervisor_pid.spawn_link_permanent(
                    butcher_gen_server.run(
                        state.version_provider.clone(),
                        manager_pid.clone(),
                        butcher_params,
                    ),
                );
                child_supervisor_pid.spawn_link_permanent(
                    manager_gen_server.run(
                        child_supervisor_pid.clone(),
                        state.blocks_pool.clone(),
                        butcher_pid,
                        state.wheels_pid.clone(),
                        manager_params,
                    ),
                );

                busyloop(child_supervisor_pid, manager_pid, state).await
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

#[derive(Debug)]
pub enum RemoveError {
    GenServer(ero::NoProcError),
}

#[derive(Debug)]
pub enum FlushError {
    GenServer(ero::NoProcError),
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Inserted {
    pub version: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Removed {
    pub version: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Flushed;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, Debug)]
pub struct Info {
    pub alive_cells_count: usize,
    pub tombstones_count: usize,
}

impl Pid {
    pub async fn info(&mut self) -> Result<Info, ero::NoProcError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx.send(Request::Info(RequestInfo { reply_tx, })).await
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
                .send(Request::Insert(RequestInsert {
                    key: key.clone(),
                    value: value.clone(),
                    reply_tx,
                }))
                .await
                .map_err(|_send_error| InsertError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(inserted) =>
                    return Ok(inserted),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn lookup(&mut self, key: kv::Key) -> Result<Option<kv::ValueCell>, LookupError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(Request::Lookup(RequestLookup {
                    key: key.clone(),
                    reply_tx,
                }))
                .await
                .map_err(|_send_error| LookupError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(result) =>
                    return Ok(result),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn remove(&mut self, key: kv::Key) -> Result<Removed, RemoveError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(Request::Remove(RequestRemove {
                    key: key.clone(),
                    reply_tx,
                }))
                .await
                .map_err(|_send_error| RemoveError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(result) =>
                    return Ok(result),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn flush(&mut self) -> Result<Flushed, FlushError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx.send(Request::Flush(RequestFlush { reply_tx, })).await
                .map_err(|_send_error| FlushError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(result) =>
                    return Ok(result),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }
}

struct State {
    parent_supervisor: SupervisorPid,
    blocks_pool: BytesPool,
    version_provider: version::Provider,
    wheels_pid: wheels::Pid,
    fused_request_rx: stream::Fuse<mpsc::Receiver<Request>>,
    params: Params,
}

#[derive(Debug)]
pub enum Request {
    Info(RequestInfo),
    Insert(RequestInsert),
    Lookup(RequestLookup),
    Remove(RequestRemove),
    Flush(RequestFlush),
}

#[derive(Debug)]
pub struct RequestInfo {
    pub reply_tx: oneshot::Sender<Info>,
}

#[derive(Debug)]
pub struct RequestInsert {
    pub key: kv::Key,
    pub value: kv::Value,
    pub reply_tx: oneshot::Sender<Inserted>,
}

#[derive(Debug)]
pub struct RequestLookup {
    pub key: kv::Key,
    pub reply_tx: oneshot::Sender<Option<kv::ValueCell>>,
}

#[derive(Debug)]
pub struct RequestRemove {
    pub key: kv::Key,
    pub reply_tx: oneshot::Sender<Removed>,
}

#[derive(Debug)]
pub struct RequestFlush {
    pub reply_tx: oneshot::Sender<Flushed>,
}


#[derive(Debug)]
enum Error {
}

async fn busyloop(
    _child_supervisor_pid: SupervisorPid,
    mut manager_pid: core::manager::Pid,
    mut state: State,
)
    -> Result<(), ErrorSeverity<State, Error>>
{
    while let Some(request) = state.fused_request_rx.next().await {
        match request {
            Request::Info(RequestInfo { reply_tx, }) => {
                let info = match manager_pid.info().await {
                    Ok(info) =>
                        info,
                    Err(ero::NoProcError) => {
                        log::warn!("manager has gone during info, terminating");
                        break;
                    },
                };
                if let Err(_send_error) = reply_tx.send(info) {
                    log::warn!("client canceled info request");
                }
            },

            Request::Insert(RequestInsert { key, value, reply_tx, }) => {
                let status = match manager_pid.insert(key, value).await {
                    Ok(inserted) =>
                        inserted,
                    Err(core::manager::InsertError::GenServer(ero::NoProcError)) => {
                        log::warn!("manager has gone during insert, terminating");
                        break;
                    },
                };
                if let Err(_send_error) = reply_tx.send(status) {
                    log::warn!("client canceled insert request");
                }
            },

            Request::Lookup(RequestLookup { key, reply_tx, }) => {
                let status = match manager_pid.lookup(key).await {
                    Ok(result) =>
                        result,
                    Err(core::manager::LookupError::GenServer(ero::NoProcError)) => {
                        log::warn!("manager has gone during lookup, terminating");
                        break;
                    },
                };
                if let Err(_send_error) = reply_tx.send(status) {
                    log::warn!("client canceled lookup request");
                }
            },

            Request::Remove(RequestRemove { key, reply_tx, }) => {
                let status = match manager_pid.remove(key).await {
                    Ok(removed) =>
                        removed,
                    Err(core::manager::RemoveError::GenServer(ero::NoProcError)) => {
                        log::warn!("manager has gone during remove, terminating");
                        break;
                    },
                };
                if let Err(_send_error) = reply_tx.send(status) {
                    log::warn!("client canceled remove request");
                }
            },

            Request::Flush(RequestFlush { reply_tx, }) => {
                let status = match manager_pid.flush_all().await {
                    Ok(Flushed) =>
                        Flushed,
                    Err(core::manager::FlushError::GenServer(ero::NoProcError)) => {
                        log::warn!("manager has gone during flush, terminating");
                        break;
                    },
                };
                if let Err(_send_error) = reply_tx.send(status) {
                    log::warn!("client canceled flush request");
                }
            },
        }
    }
    Ok(())
}

impl AddAssign for Info {
    fn add_assign(&mut self, rhs: Info) {
        self.alive_cells_count += rhs.alive_cells_count;
        self.tombstones_count += rhs.tombstones_count;
    }
}

impl Info {
    pub fn reset(&mut self) {
        self.alive_cells_count = 0;
        self.tombstones_count = 0;
    }
}
