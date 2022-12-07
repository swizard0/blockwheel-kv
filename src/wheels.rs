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

use alloc_pool_pack::{
    Source,
    Target,
    ReadFromSource,
    WriteToBytesMut,
};

use arbeitssklave::{
    komm,
};

use crate::{
    core::{
        performer_sklave,
        FsInfo,
        FsFlush,
        FsReadBlock,
        FsWriteBlock,
        FsDeleteBlock,
    },
    EchoPolicy,
};

// WheelFilename

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct WheelFilename {
    filename_bytes: Bytes,
}

impl WriteToBytesMut for WheelFilename {
    fn write_to_bytes_mut<T>(&self, target: &mut T) where T: Target {
        self.filename_bytes.write_to_bytes_mut(target);
    }
}

#[derive(Debug)]
pub enum ReadWheelFilenameError {
    FilenameBytes(alloc_pool_pack::bytes::ReadBytesError),
}

impl ReadFromSource for WheelFilename {
    type Error = ReadWheelFilenameError;

    fn read_from_source<S>(source: &mut S) -> Result<Self, Self::Error> where S: Source {
        let filename_bytes = Bytes::read_from_source(source)
            .map_err(Self::Error::FilenameBytes)?;
        Ok(WheelFilename { filename_bytes, })
    }
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

// WheelRef

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

// BlockRef

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct BlockRef {
    pub blockwheel_filename: WheelFilename,
    pub block_id: blockwheel_fs::block::Id,
}

impl WriteToBytesMut for BlockRef {
    fn write_to_bytes_mut<T>(&self, target: &mut T) where T: Target {
        self.blockwheel_filename.write_to_bytes_mut(target);
        self.block_id.write_to_bytes_mut(target);
    }
}

#[derive(Debug)]
pub enum ReadBlockRefError {
    BlockwheelFilename(ReadWheelFilenameError),
    BlockId(blockwheel_fs::block::ReadIdError),
}

impl ReadFromSource for BlockRef {
    type Error = ReadBlockRefError;

    fn read_from_source<S>(source: &mut S) -> Result<Self, Self::Error> where S: Source {
        let blockwheel_filename = WheelFilename::read_from_source(source)
            .map_err(Self::Error::BlockwheelFilename)?;
        let block_id = blockwheel_fs::block::Id::read_from_source(source)
            .map_err(Self::Error::BlockId)?;
        Ok(BlockRef { blockwheel_filename, block_id, })
    }
}

// Wheels

pub struct Wheels<E> where E: EchoPolicy {
    inner: Arc<Inner<E>>,
}

impl<E> Clone for Wheels<E> where E: EchoPolicy {
    fn clone(&self) -> Self {
        Wheels { inner: self.inner.clone(), }
    }
}

// WheelEchoPolicy

pub struct WheelEchoPolicy<E>(PhantomData<E>);

impl<E> blockwheel_fs::EchoPolicy for WheelEchoPolicy<E> where E: EchoPolicy {
    type Info = FsInfo<E>;
    type Flush = FsFlush<E>;
    type WriteBlock = FsWriteBlock;
    type ReadBlock = FsReadBlock<E>;
    type DeleteBlock = FsDeleteBlock;
    type IterBlocksInit = komm::Rueckkopplung<performer_sklave::Order<E>, performer_sklave::WheelRouteIterBlocksInit>;
    type IterBlocksNext = komm::Rueckkopplung<performer_sklave::Order<E>, performer_sklave::WheelRouteIterBlocksNext>;
}

// WheelMeister

pub type WheelMeister<E> = blockwheel_fs::Meister<WheelEchoPolicy<E>>;

// Inner

struct Inner<E> where E: EchoPolicy {
    wheels: Vec<WheelRef<E>>,
    index: HashMap<WheelFilename, usize>,
}

// WheelsBuilder

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
