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
    marker::{
        PhantomData,
    },
    collections::{
        hash_map,
        HashMap,
    },
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

use crate::{
    core::{
        performer_sklave,
    },
    AccessPolicy,
};

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
    pub fn from_bytes(bytes: &[u8], blocks_pool: &BytesPool) -> WheelFilename {
        let mut block_bytes = blocks_pool.lend();
        block_bytes.extend_from_slice(bytes);
        WheelFilename {
            filename_bytes: block_bytes.freeze(),
        }
    }

    pub fn from_str(filename: &str, blocks_pool: &BytesPool) -> WheelFilename {
        WheelFilename::from_bytes(filename.as_bytes(), blocks_pool)
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

pub struct WheelRef<A> where A: AccessPolicy {
    pub blockwheel_filename: WheelFilename,
    pub meister: WheelMeister<A>,
}

impl<A> Clone for WheelRef<A> where A: AccessPolicy {
    fn clone(&self) -> Self {
        WheelRef {
            blockwheel_filename: self.blockwheel_filename.clone(),
            meister: self.meister.clone(),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct BlockRef {
    pub blockwheel_filename: WheelFilename,
    pub block_id: blockwheel_fs::block::Id,
}

pub struct Wheels<A> where A: AccessPolicy {
    inner: Arc<Inner<A>>,
}

impl<A> Clone for Wheels<A> where A: AccessPolicy {
    fn clone(&self) -> Self {
        Wheels { inner: self.inner.clone(), }
    }
}

pub struct WheelAccessPolicy<A>(PhantomData<A>);

impl<A> blockwheel_fs::AccessPolicy for WheelAccessPolicy<A> where A: AccessPolicy {
    type Order = performer_sklave::Order<A>;
    type Info = performer_sklave::WheelRouteInfo;
    type Flush = performer_sklave::WheelRouteFlush;
    type WriteBlock = performer_sklave::WheelRouteWriteBlock;
    type ReadBlock = performer_sklave::WheelRouteReadBlock;
    type DeleteBlock = performer_sklave::WheelRouteDeleteBlock;
    type IterBlocksInit = performer_sklave::WheelRouteIterBlocksInit;
    type IterBlocksNext = performer_sklave::WheelRouteIterBlocksNext;
}

pub type WheelMeister<A> = blockwheel_fs::Meister<WheelAccessPolicy<A>>;

struct Inner<A> where A: AccessPolicy {
    wheels: Vec<WheelRef<A>>,
    index: HashMap<WheelFilename, usize>,
}

pub struct WheelsBuilder<A> where A: AccessPolicy {
    inner: Inner<A>,
}

#[derive(Debug)]
pub enum BuilderError {
    NoWheelRefs,
}

impl<A> WheelsBuilder<A> where A: AccessPolicy {
    pub fn new() -> Self {
        Self {
            inner: Inner {
                wheels: Vec::new(),
                index: HashMap::new(),
            },
        }
    }

    pub fn add_wheel_ref(mut self, wheel_ref: WheelRef<A>) -> Self {
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

    pub fn build(self) -> Result<Wheels<A>, BuilderError> {
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
        error: arbeitssklave::Error,
    },
    WheelIterBlocksRxDropped {
        blockwheel_filename: WheelFilename,
    },
    DuplicateNoMoreBlocks {
        blockwheel_filename: WheelFilename,
    },
    UnexpectedBlockAfterShutdown {
        blockwheel_filename: WheelFilename,
    },
}

impl<A> Wheels<A> where A: AccessPolicy {
    pub fn acquire(&self) -> WheelRef<A> {
        let mut rng = rand::thread_rng();
        let offset = rng.gen_range(0 .. self.inner.wheels.len());
        self.inner.wheels[offset].clone()
    }

    pub fn get(&self, blockwheel_filename: &WheelFilename) -> Option<WheelRef<A>> {
        self.inner.index.get(blockwheel_filename)
            .and_then(|&offset| self.inner.wheels.get(offset))
            .map(Clone::clone)
    }

    pub fn iter(&self) -> impl Iterator<Item = &'_ WheelRef<A>> {
        self.inner.wheels.iter()
    }
}
