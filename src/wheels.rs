use std::{
    str,
    fmt,
    path,
    pin::{
        Pin,
    },
    ops::{
        Deref,
    },
    sync::{
        Arc,
    },
    collections::{
        hash_map,
        HashMap,
    },
};

use futures::{
    future::{
        try_join_all,
    },
    stream::{
        FuturesUnordered,
    },
    channel::{
        mpsc,
    },
    task::{
        Context,
        Poll,
    },
    Stream,
    Future,
    StreamExt,
};

use rand::{
    Rng,
};

use alloc_pool::{
    bytes::{
        Bytes,
        BytesPool,
    },
};

use ero_blockwheel_fs as blockwheel_fs;

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
    pub blockwheel_pid: blockwheel_fs::Pid,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct BlockRef {
    pub blockwheel_filename: WheelFilename,
    pub block_id: blockwheel_fs::block::Id,
}

#[derive(Clone)]
pub struct Wheels {
    inner: Arc<Inner>,
}

struct Inner {
    wheels: Vec<WheelRef>,
    index: HashMap<WheelFilename, usize>,
}

pub struct WheelsBuilder {
    inner: Inner,
}

#[derive(Debug)]
pub enum BuilderError {
    NoWheelRefs,
}

impl WheelsBuilder {
    pub fn new() -> Self {
        Self {
            inner: Inner {
                wheels: Vec::new(),
                index: HashMap::new(),
            },
        }
    }

    pub fn add_wheel_ref(&mut self, wheel_ref: WheelRef) -> &mut Self {
        let offset = self.inner.wheels.len();
        let filename = wheel_ref.blockwheel_filename.clone();
        match self.inner.index.entry(filename.clone()) {
            hash_map::Entry::Vacant(ve) => {
                ve.insert(offset);
                self.inner.wheels.push(wheel_ref);
            },
            hash_map::Entry::Occupied(..) =>
                (),
        }
        self
    }

    pub fn build(self) -> Result<Wheels, BuilderError> {
        if self.inner.wheels.is_empty() {
            return Err(BuilderError::NoWheelRefs);
        }

        Ok(Wheels { inner: Arc::new(self.inner), })
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Flushed;

#[derive(Debug)]
pub enum FlushError {
    WheelGone {
        blockwheel_filename: WheelFilename,
    },
}

pub enum IterBlocksItem {
    Block {
        block_ref: BlockRef,
        block_bytes: Bytes,
    },
    NoMoreBlocks,
}

#[derive(Debug)]
pub enum IterBlocksItemError {
    WheelIterBlocks {
        blockwheel_filename: WheelFilename,
        error: blockwheel_fs::IterBlocksError,
    },
    WheelIterBlocksRxDropped,
}

impl Wheels {
    pub fn acquire(&self) -> WheelRef {
        let mut rng = rand::thread_rng();
        let offset = rng.gen_range(0 .. self.inner.wheels.len());
        self.inner.wheels[offset].clone()
    }

    pub fn get(&self, blockwheel_filename: &WheelFilename) -> Option<WheelRef> {
        self.inner.index.get(blockwheel_filename)
            .and_then(|&offset| self.inner.wheels.get(offset))
            .map(Clone::clone)
    }

    pub async fn flush(&self) -> Result<Flushed, FlushError> {
        let mut flush_tasks = FuturesUnordered::new();
        for WheelRef { blockwheel_pid, blockwheel_filename, } in &self.inner.wheels {
            let mut blockwheel_pid = blockwheel_pid.clone();
            flush_tasks.push(async move {
                let status = blockwheel_pid.flush().await;
                (blockwheel_filename, status)
            });
        }
        while let Some((blockwheel_filename, flush_status)) = flush_tasks.next().await {
            match flush_status {
                Ok(blockwheel_fs::Flushed) =>
                    (),
                Err(ero::NoProcError) =>
                    return Err(FlushError::WheelGone { blockwheel_filename: blockwheel_filename.clone(), }),
            }
        }
        Ok(Flushed)
    }

    pub fn iter_blocks(&self) -> IterBlocks<impl Future<Output = Result<Vec<Rx>, IterBlocksItemError>> + '_> {
        make_iter_blocks(self.inner.wheels.iter().cloned())
    }
}

pub struct IterBlocks<F> {
    state: IterBlocksState<F>,
    rxs_ready: Vec<Rx>,
    committed: bool,
}

enum IterBlocksState<F> {
    AwaitRxs { rxs_future: F, },
    RxsReady,
}

pub struct Rx {
    rx: mpsc::Receiver<blockwheel_fs::IterBlocksItem>,
    blockwheel_filename: WheelFilename,
}

fn make_iter_blocks<I>(
    wheels_refs: I
)
    -> IterBlocks<impl Future<Output = Result<Vec<Rx>, IterBlocksItemError>>>
where I: IntoIterator<Item = WheelRef>
{
    let rxs_future = try_join_all(
        wheels_refs
            .into_iter()
            .map(|WheelRef { mut blockwheel_pid, blockwheel_filename, }| {
                async move {
                    let iter_blocks = blockwheel_pid.iter_blocks().await
                        .map_err(|error| IterBlocksItemError::WheelIterBlocks {
                            blockwheel_filename: blockwheel_filename.clone(),
                            error,
                        })?;
                    Ok(Rx { rx: iter_blocks.blocks_rx, blockwheel_filename, })
                }
            })
    );

    IterBlocks {
        state: IterBlocksState::AwaitRxs { rxs_future, },
        rxs_ready: Vec::new(),
        committed: false,
    }
}

impl<F> Stream for IterBlocks<F>
where F: Future<Output = Result<Vec<Rx>, IterBlocksItemError>> + Unpin
{
    type Item = Result<IterBlocksItem, IterBlocksItemError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match &mut this.state {
            IterBlocksState::AwaitRxs { rxs_future, } => {
                let rxs_future_pin = Pin::new(rxs_future);
                match rxs_future_pin.poll(cx) {
                    Poll::Ready(Ok(rxs)) => {
                        this.rxs_ready.extend(rxs);
                        this.state = IterBlocksState::RxsReady;
                    },
                    Poll::Ready(Err(error)) =>
                        return Poll::Ready(Some(Err(error))),
                    Poll::Pending =>
                        (),
                }
            },
            IterBlocksState::RxsReady =>
                (),
        }

        let mut i = 0;
        while i < this.rxs_ready.len() {
            let rx_pin = Pin::new(&mut this.rxs_ready[i].rx);
            match rx_pin.poll_next(cx) {
                Poll::Ready(None) =>
                    return Poll::Ready(Some(Err(IterBlocksItemError::WheelIterBlocksRxDropped))),
                Poll::Ready(Some(blockwheel_fs::IterBlocksItem::NoMoreBlocks)) => {
                    this.rxs_ready.swap_remove(i);
                },
                Poll::Ready(Some(blockwheel_fs::IterBlocksItem::Block { block_id, block_bytes, })) => {
                    let block_ref = BlockRef {
                        blockwheel_filename: this.rxs_ready[i].blockwheel_filename.clone(),
                        block_id,
                    };
                    return Poll::Ready(Some(Ok(IterBlocksItem::Block { block_ref, block_bytes, })));
                },
                Poll::Pending =>
                    i += 1,
            }
        }

        if this.rxs_ready.is_empty() {
            if this.committed {
                Poll::Ready(None)
            } else {
                Poll::Ready(Some(Ok(IterBlocksItem::NoMoreBlocks)))
            }
        } else {
            Poll::Pending
        }
    }
}
