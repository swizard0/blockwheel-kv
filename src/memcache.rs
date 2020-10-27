use std::{
    cmp,
    collections::{
        BTreeMap,
    },
    ops::Deref,
    time::Duration,
    borrow::Borrow,
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

use super::{
    kv,
    proto,
    storage,
    manager,
    kv_context,
    Inserted,
};

#[derive(Clone, Debug)]
pub struct Params {
    pub task_restart_sec: usize,
    pub tree_block_size: usize,
}

impl Default for Params {
    fn default() -> Params {
        Params {
            task_restart_sec: 4,
            tree_block_size: 32,
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
        manager_pid: manager::Pid,
        params: Params,
    )
    {
        let terminate_result = restart::restartable(
            ero::Params {
                name: "ero-blockwheel-kv memcache task",
                restart_strategy: RestartStrategy::Delay {
                    restart_after: Duration::from_secs(params.task_restart_sec as u64),
                },
            },
            State {
                fused_request_rx: self.fused_request_rx,
                manager_pid,
                params,
            },
            |mut state| busyloop(state),
        ).await;
        if let Err(error) = terminate_result {
            log::error!("fatal error: {:?}", error);
        }
    }
}

type Request = proto::Request<kv_context::Context>;

struct State {
    fused_request_rx: stream::Fuse<mpsc::Receiver<Request>>,
    manager_pid: manager::Pid,
    params: Params,
}

#[derive(Debug)]
enum Error {
    Storage(storage::Error),
}

pub type Cache = BTreeMap<Key, ()>;

async fn busyloop(mut state: State) -> Result<(), ErrorSeverity<State, Error>> {
    let mut memcache = Cache::new();
    let mut work_block = Vec::new();
    let mut current_block_size = 0;

    while let Some(request) = state.fused_request_rx.next().await {
        match request {
            proto::Request::Info(..) =>
                unimplemented!(),

            proto::Request::Insert(proto::RequestInsert { key_value, context: reply_tx, }) => {
                work_block.clear();
                storage::serialize_key_value(&key_value, storage::JumpRef::None, &mut work_block)
                    .map_err(Error::Storage)
                    .map_err(ErrorSeverity::Fatal)?;
                current_block_size += work_block.len();
                memcache.insert(Key::new(key_value.clone()), ());
                if let Err(_send_error) = reply_tx.send(Ok(Inserted)) {
                    log::warn!("client canceled insert request");
                    current_block_size -= work_block.len();
                    memcache.remove(key_value.key_data());
                }
            },
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct Key {
    kv: kv::KeyValue,
}

impl Key {
    fn new(kv: kv::KeyValue) -> Key {
        Key { kv, }
    }
}

impl AsRef<kv::KeyValue> for Key {
    #[inline]
    fn as_ref(&self) -> &kv::KeyValue {
        &self.kv
    }
}

impl Deref for Key {
    type Target = kv::KeyValue;

    #[inline]
    fn deref(&self) -> &kv::KeyValue {
        self.as_ref()
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Key) -> bool {
        self.kv.key_data() == other.kv.key_data()
    }
}

impl Eq for Key { }

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Key) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Key) -> cmp::Ordering {
        self.kv.key_data().cmp(other.kv.key_data())
    }
}

impl Borrow<[u8]> for Key {
    fn borrow(&self) -> &[u8] {
        self.kv.key_data()
    }
}
