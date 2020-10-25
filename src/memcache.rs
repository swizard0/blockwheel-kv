use std::{
    cmp,
    collections::{
        BTreeMap,
    },
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

use super::{
    kv,
    proto,
    kv_context,
    Inserted,
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
    params: Params,
}

#[derive(Debug)]
enum Error {

}

async fn busyloop(mut state: State) -> Result<(), ErrorSeverity<State, Error>> {
    let mut memcache = BTreeMap::new();

    while let Some(request) = state.fused_request_rx.next().await {
        match request {
            proto::Request::Info(..) =>
                unimplemented!(),

            proto::Request::Insert(proto::RequestInsert { key_value, context: reply_tx, }) => {
                // reply first to check canceling scenario
                if let Err(_send_error) = reply_tx.send(Ok(Inserted)) {
                    log::warn!("client canceled insert request");
                } else {
                    memcache.insert(Key::new(key_value.clone()), key_value);
                }
            },
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct Key {
    kv: kv::KeyValue,
}

impl Key {
    fn new(kv: kv::KeyValue) -> Key {
        Key { kv, }
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
