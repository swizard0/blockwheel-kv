use std::{
    str,
    fmt,
    path,
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

use arbeitssklave::{
    komm,
};

use crate::{
    core::{
        performer_sklave,
    },
    EchoPolicy,
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

pub struct WheelRef<E> where E: EchoPolicy {
    pub blockwheel_filename: WheelFilename,
    pub meister: WheelMeister<E>,
}

impl<E> Clone for WheelRef<E> where E: EchoPolicy {
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

pub struct Wheels<E> where E: EchoPolicy {
    inner: Arc<Inner<E>>,
}

impl<E> Clone for Wheels<E> where E: EchoPolicy {
    fn clone(&self) -> Self {
        Wheels { inner: self.inner.clone(), }
    }
}

pub struct WheelEchoPolicy<E>(PhantomData<E>);

impl<E> blockwheel_fs::EchoPolicy for WheelEchoPolicy<E> where E: EchoPolicy {
    type Info = komm::Rueckkopplung<performer_sklave::Order<E>, performer_sklave::WheelRouteInfo>;
    type Flush = komm::Rueckkopplung<performer_sklave::Order<E>, performer_sklave::WheelRouteFlush>;
    type WriteBlock = komm::Rueckkopplung<performer_sklave::Order<E>, performer_sklave::WheelRouteWriteBlock>;
    type ReadBlock = komm::Rueckkopplung<performer_sklave::Order<E>, performer_sklave::WheelRouteReadBlock>;
    type DeleteBlock = komm::Rueckkopplung<performer_sklave::Order<E>, performer_sklave::WheelRouteDeleteBlock>;
    type IterBlocksInit = komm::Rueckkopplung<performer_sklave::Order<E>, performer_sklave::WheelRouteIterBlocksInit>;
    type IterBlocksNext = komm::Rueckkopplung<performer_sklave::Order<E>, performer_sklave::WheelRouteIterBlocksNext>;
}

pub type WheelMeister<E> = blockwheel_fs::Meister<WheelEchoPolicy<E>>;

struct Inner<E> where E: EchoPolicy {
    wheels: Vec<WheelRef<E>>,
    index: HashMap<WheelFilename, usize>,
}

pub struct WheelsBuilder<E> where E: EchoPolicy {
    inner: Inner<E>,
}

#[derive(Debug)]
pub enum BuilderError {
    NoWheelRefs,
}

impl<E> Default for WheelsBuilder<E> where E: EchoPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> WheelsBuilder<E> where E: EchoPolicy {
    pub fn new() -> Self {
        Self {
            inner: Inner {
                wheels: Vec::new(),
                index: HashMap::new(),
            },
        }
    }

    pub fn add_wheel_ref(mut self, wheel_ref: WheelRef<E>) -> Self {
        let offset = self.inner.wheels.len();
        let filename = wheel_ref.blockwheel_filename.clone();
        match self.inner.index.entry(filename) {
            hash_map::Entry::Vacant(ve) => {
                ve.insert(offset);
                self.inner.wheels.push(wheel_ref);
            },
            hash_map::Entry::Occupied(..) =>
                (),
        }
        self
    }

    pub fn build(self) -> Result<Wheels<E>, BuilderError> {
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

impl<E> Wheels<E> where E: EchoPolicy {
    pub fn acquire(&self) -> WheelRef<E> {
        let mut rng = rand::thread_rng();
        let offset = rng.gen_range(0 .. self.inner.wheels.len());
        self.inner.wheels[offset].clone()
    }

    pub fn get(&self, blockwheel_filename: &WheelFilename) -> Option<WheelRef<E>> {
        self.inner.index.get(blockwheel_filename)
            .and_then(|&offset| self.inner.wheels.get(offset))
            .map(Clone::clone)
    }

    pub fn iter(&self) -> impl Iterator<Item = &'_ WheelRef<E>> {
        self.inner.wheels.iter()
    }
}
