use alloc_pool::{
    bytes::{
        Bytes,
        BytesMut,
    },
};

use alloc_pool_pack::{
    integer,
    Source,
    Target,
    TargetCounter,
    SourceBytesRef,
    ReadFromSource,
    WriteToBytesMut,
};

use blockwheel_fs::{
    block,
};

use crate::{
    kv,
    wheels::{
        BlockRef,
        WheelFilename,
        ReadBlockRefError,
    },
};

// NodeType

#[derive(Clone, Copy, Debug)]
pub enum NodeType {
    Root { tree_entries_count: usize, },
    Leaf,
}

const TAG_NODE_TYPE_ROOT: u8 = 1;
const TAG_NODE_TYPE_LEAF: u8 = 2;

impl WriteToBytesMut for NodeType {
    fn write_to_bytes_mut<T>(&self, target: &mut T) where T: Target {
        match self {
            &NodeType::Root { tree_entries_count, } => {
                TAG_NODE_TYPE_ROOT.write_to_bytes_mut(target);
                let value = tree_entries_count as u64;
                value.write_to_bytes_mut(target);
            },
            NodeType::Leaf =>
                TAG_NODE_TYPE_LEAF.write_to_bytes_mut(target),
        }
    }
}

#[derive(Debug)]
pub enum ReadNodeTypeError {
    Tag(integer::ReadIntegerError),
    InvalidTag(u8),
    TreeEntriesCount(integer::ReadIntegerError),
}

impl ReadFromSource for NodeType {
    type Error = ReadNodeTypeError;

    fn read_from_source<S>(source: &mut S) -> Result<Self, Self::Error> where S: Source {
        let tag = u8::read_from_source(source)
            .map_err(Self::Error::Tag)?;
        match tag {
            TAG_NODE_TYPE_ROOT => {
                let value = u64::read_from_source(source)
                    .map_err(Self::Error::TreeEntriesCount)?;
                Ok(NodeType::Root { tree_entries_count: value as usize, })
            },
            TAG_NODE_TYPE_LEAF =>
                Ok(NodeType::Leaf),
            _ =>
                Err(Self::Error::InvalidTag(tag)),
        }
    }
}

// BlockHeader

const BLOCK_MAGIC: u64 = 0xbde78ba3966ca503;

#[derive(Clone, Debug)]
pub struct BlockHeader {
    pub node_type: NodeType,
    pub entries_count: usize,
}

impl WriteToBytesMut for BlockHeader {
    fn write_to_bytes_mut<T>(&self, target: &mut T) where T: Target {
        BLOCK_MAGIC.write_to_bytes_mut(target);
        self.node_type.write_to_bytes_mut(target);
        (self.entries_count as u32).write_to_bytes_mut(target);
    }
}

#[derive(Debug)]
pub enum ReadBlockHeaderError {
    Magic(integer::ReadIntegerError),
    InvalidMagic { expected: u64, provided: u64, },
    NodeType(ReadNodeTypeError),
    EntriesCount(integer::ReadIntegerError),
    EntriesCountInto(std::num::TryFromIntError),
}

impl ReadFromSource for BlockHeader {
    type Error = ReadBlockHeaderError;

    fn read_from_source<S>(source: &mut S) -> Result<Self, Self::Error> where S: Source {
        let magic = u64::read_from_source(source)
            .map_err(Self::Error::Magic)?;
        if magic != BLOCK_MAGIC {
            return Err(Self::Error::InvalidMagic {
                expected: BLOCK_MAGIC,
                provided: magic,
            });
        }
        let node_type = NodeType::read_from_source(source)
            .map_err(Self::Error::NodeType)?;
        let entries_count = u32::read_from_source(source)
            .map_err(Self::Error::EntriesCount)?
            .try_into()
            .map_err(Self::Error::EntriesCountInto)?;
        Ok(BlockHeader { node_type, entries_count, })
    }
}

// LocalRef

#[derive(Clone, Debug)]
pub struct LocalRef {
    pub block_id: block::Id,
}

impl WriteToBytesMut for LocalRef {
    fn write_to_bytes_mut<T>(&self, target: &mut T) where T: Target {
        self.block_id.write_to_bytes_mut(target);
    }
}

#[derive(Debug)]
pub enum ReadLocalRefError {
    BlockId(block::ReadIdError),
}

impl ReadFromSource for LocalRef {
    type Error = ReadLocalRefError;

    fn read_from_source<S>(source: &mut S) -> Result<Self, Self::Error> where S: Source {
        let block_id = block::Id::read_from_source(source)
            .map_err(Self::Error::BlockId)?;
        Ok(LocalRef { block_id, })
    }
}

// JumpRef

#[derive(Clone, Debug)]
pub enum JumpRef {
    None,
    Local(LocalRef),
    External(BlockRef),
}

const TAG_JUMP_REF_NONE: u8 = 1;
const TAG_JUMP_REF_LOCAL: u8 = 2;
const TAG_JUMP_REF_EXTERNAL: u8 = 3;

impl WriteToBytesMut for JumpRef {
    fn write_to_bytes_mut<T>(&self, target: &mut T) where T: Target {
        match self {
            JumpRef::None =>
                TAG_JUMP_REF_NONE.write_to_bytes_mut(target),
            JumpRef::Local(local_ref) => {
                TAG_JUMP_REF_LOCAL.write_to_bytes_mut(target);
                local_ref.write_to_bytes_mut(target);
            },
            JumpRef::External(block_ref) => {
                TAG_JUMP_REF_EXTERNAL.write_to_bytes_mut(target);
                block_ref.write_to_bytes_mut(target);
            },
        }
    }
}

#[derive(Debug)]
pub enum ReadJumpRefError {
    Tag(integer::ReadIntegerError),
    InvalidTag(u8),
    Local(ReadLocalRefError),
    External(ReadBlockRefError),
}

impl ReadFromSource for JumpRef {
    type Error = ReadJumpRefError;

    fn read_from_source<S>(source: &mut S) -> Result<Self, Self::Error> where S: Source {
        let tag = u8::read_from_source(source)
            .map_err(Self::Error::Tag)?;
        match tag {
            TAG_JUMP_REF_NONE =>
                Ok(JumpRef::None),
            TAG_JUMP_REF_LOCAL => {
                let local_ref = LocalRef::read_from_source(source)
                    .map_err(Self::Error::Local)?;
                Ok(JumpRef::Local(local_ref))
            },
            TAG_JUMP_REF_EXTERNAL => {
                let block_ref = BlockRef::read_from_source(source)
                    .map_err(Self::Error::External)?;
                Ok(JumpRef::External(block_ref))
            },
            _ =>
                Err(Self::Error::InvalidTag(tag)),
        }
    }
}

impl JumpRef {
    pub fn from_maybe_block_ref(maybe_block_ref: &Option<BlockRef>, current_blockwheel_filename: &WheelFilename) -> JumpRef {
        match maybe_block_ref {
            None =>
                JumpRef::None,
            Some(block_ref) if &block_ref.blockwheel_filename == current_blockwheel_filename =>
                JumpRef::Local(LocalRef {
                    block_id: block_ref.block_id.clone(),
                }),
            Some(block_ref) =>
                JumpRef::External(block_ref.clone()),
        }
    }
}

// ValueRef

#[derive(Clone, Debug)]
pub enum ValueRef {
    Inline(Bytes),
    Local(LocalRef),
    External(BlockRef),
}

const TAG_VALUE_REF_INLINE: u8 = 1;
const TAG_VALUE_REF_LOCAL: u8 = 2;
const TAG_VALUE_REF_EXTERNAL: u8 = 3;

impl WriteToBytesMut for ValueRef {
    fn write_to_bytes_mut<T>(&self, target: &mut T) where T: Target {
        match self {
            ValueRef::Inline(bytes) => {
                TAG_VALUE_REF_INLINE.write_to_bytes_mut(target);
                bytes.write_to_bytes_mut(target);
            },
            ValueRef::Local(local_ref) => {
                TAG_VALUE_REF_LOCAL.write_to_bytes_mut(target);
                local_ref.write_to_bytes_mut(target);
            },
            ValueRef::External(block_ref) => {
                TAG_VALUE_REF_EXTERNAL.write_to_bytes_mut(target);
                block_ref.write_to_bytes_mut(target);
            },
        }
    }
}

#[derive(Debug)]
pub enum ReadValueRefError {
    Tag(integer::ReadIntegerError),
    InvalidTag(u8),
    Inline(alloc_pool_pack::bytes::ReadBytesError),
    Local(ReadLocalRefError),
    External(ReadBlockRefError),
}

impl ReadFromSource for ValueRef {
    type Error = ReadValueRefError;

    fn read_from_source<S>(source: &mut S) -> Result<Self, Self::Error> where S: Source {
        let tag = u8::read_from_source(source)
            .map_err(Self::Error::Tag)?;
        match tag {
            TAG_VALUE_REF_INLINE => {
                let bytes = Bytes::read_from_source(source)
                    .map_err(Self::Error::Inline)?;
                Ok(ValueRef::Inline(bytes))
            },
            TAG_VALUE_REF_LOCAL => {
                let local_ref = LocalRef::read_from_source(source)
                    .map_err(Self::Error::Local)?;
                Ok(ValueRef::Local(local_ref))
            },
            TAG_VALUE_REF_EXTERNAL => {
                let block_ref = BlockRef::read_from_source(source)
                    .map_err(Self::Error::External)?;
                Ok(ValueRef::External(block_ref))
            },
            _ =>
                Err(Self::Error::InvalidTag(tag)),
        }
    }
}

impl ValueRef {
    pub fn maybe_collapse(&mut self, current_blockwheel_filename: &WheelFilename) {
        match self {
            ValueRef::External(block_ref) if &block_ref.blockwheel_filename == current_blockwheel_filename =>
                *self = ValueRef::Local(LocalRef {
                    block_id: block_ref.block_id.clone(),
                }),
            ValueRef::Inline(..) | ValueRef::Local(..) | ValueRef::External(..) =>
                (),
        }
    }

    pub fn maybe_lift(&mut self, current_blockwheel_filename: &WheelFilename) {
        match self {
            ValueRef::Local(LocalRef { block_id, }) =>
                *self = ValueRef::External(BlockRef {
                    blockwheel_filename: current_blockwheel_filename.clone(),
                    block_id: block_id.clone(),
                }),
            ValueRef::Inline(..) | ValueRef::External(..) =>
                (),
        }
    }
}

impl From<kv::Value> for ValueRef {
    fn from(value: kv::Value) -> ValueRef {
        ValueRef::Inline(value.value_bytes)
    }
}

// Cell

const TAG_CELL_VALUE: u8 = 1;
const TAG_CELL_TOMBSTONE: u8 = 2;

impl WriteToBytesMut for kv::Cell<ValueRef> {
    fn write_to_bytes_mut<T>(&self, target: &mut T) where T: Target {
        match self {
            kv::Cell::Value(value_ref) => {
                TAG_CELL_VALUE.write_to_bytes_mut(target);
                value_ref.write_to_bytes_mut(target);
            },
            kv::Cell::Tombstone =>
                TAG_CELL_TOMBSTONE.write_to_bytes_mut(target),
        }
    }
}

#[derive(Debug)]
pub enum ReadCellError {
    Tag(integer::ReadIntegerError),
    InvalidTag(u8),
    Value(ReadValueRefError),
}

impl ReadFromSource for kv::Cell<ValueRef> {
    type Error = ReadCellError;

    fn read_from_source<S>(source: &mut S) -> Result<Self, Self::Error> where S: Source {
        let tag = u8::read_from_source(source)
            .map_err(Self::Error::Tag)?;
        match tag {
            TAG_CELL_VALUE => {
                let value_ref = ValueRef::read_from_source(source)
                    .map_err(Self::Error::Value)?;
                Ok(kv::Cell::Value(value_ref))
            },
            TAG_CELL_TOMBSTONE =>
                Ok(kv::Cell::Tombstone),
            _ =>
                Err(Self::Error::InvalidTag(tag)),
        }
    }
}

impl kv::Cell<ValueRef> {
    pub fn maybe_collapse(&mut self, current_blockwheel_filename: &WheelFilename) {
        match self {
            kv::Cell::Value(value) =>
                value.maybe_collapse(current_blockwheel_filename),
            kv::Cell::Tombstone =>
                (),
        }
    }

    pub fn maybe_lift(&mut self, current_blockwheel_filename: &WheelFilename) {
        match self {
            kv::Cell::Value(value) =>
                value.maybe_lift(current_blockwheel_filename),
            kv::Cell::Tombstone =>
                (),
        }
    }
}

impl From<kv::Cell<kv::Value>> for kv::Cell<ValueRef> {
    fn from(cell: kv::Cell<kv::Value>) -> kv::Cell<ValueRef> {
        match cell {
            kv::Cell::Value(value) =>
                kv::Cell::Value(value.into()),
            kv::Cell::Tombstone =>
                kv::Cell::Tombstone,
        }
    }
}

// ValueCell

impl WriteToBytesMut for kv::ValueCell<ValueRef> {
    fn write_to_bytes_mut<T>(&self, target: &mut T) where T: Target {
        self.version.write_to_bytes_mut(target);
        self.cell.write_to_bytes_mut(target);
    }
}

#[derive(Debug)]
pub enum ReadValueCellError {
    Version(integer::ReadIntegerError),
    Cell(ReadCellError),
}

impl ReadFromSource for kv::ValueCell<ValueRef> {
    type Error = ReadValueCellError;

    fn read_from_source<S>(source: &mut S) -> Result<Self, Self::Error> where S: Source {
        let version = u64::read_from_source(source)
            .map_err(Self::Error::Version)?;
        let cell = kv::Cell::read_from_source(source)
            .map_err(Self::Error::Cell)?;
        Ok(kv::ValueCell { version, cell, })
    }
}

impl kv::ValueCell<ValueRef> {
    pub fn maybe_collapse(&mut self, current_blockwheel_filename: &WheelFilename) {
        self.cell.maybe_collapse(current_blockwheel_filename);
    }

    pub fn maybe_lift(&mut self, current_blockwheel_filename: &WheelFilename) {
        self.cell.maybe_lift(current_blockwheel_filename);
    }
}

impl From<kv::ValueCell<kv::Value>> for kv::ValueCell<ValueRef> {
    fn from(value_cell: kv::ValueCell<kv::Value>) -> kv::ValueCell<ValueRef> {
        kv::ValueCell {
            version: value_cell.version,
            cell: value_cell.cell.into(),
        }
    }
}

// Entry

#[derive(Clone, Debug)]
pub struct Entry {
    pub jump_ref: JumpRef,
    pub key: kv::Key,
    pub value_cell: kv::ValueCell<ValueRef>,
}

impl WriteToBytesMut for Entry {
    fn write_to_bytes_mut<T>(&self, target: &mut T) where T: Target {
        self.key.key_bytes.write_to_bytes_mut(target);
        self.jump_ref.write_to_bytes_mut(target);
        self.value_cell.write_to_bytes_mut(target);
    }
}

#[derive(Debug)]
pub enum ReadEntryError {
    JumpRef(ReadJumpRefError),
    Key(alloc_pool_pack::bytes::ReadBytesError),
    ValueCell(ReadValueCellError),
}

impl ReadFromSource for Entry {
    type Error = ReadEntryError;

    fn read_from_source<S>(source: &mut S) -> Result<Self, Self::Error> where S: Source {
        let key_bytes = Bytes::read_from_source(source)
            .map_err(Self::Error::Key)?;
        let jump_ref = JumpRef::read_from_source(source)
            .map_err(Self::Error::JumpRef)?;
        let value_cell = kv::ValueCell::read_from_source(source)
            .map_err(Self::Error::ValueCell)?;
        Ok(Entry { jump_ref, key: kv::Key { key_bytes, }, value_cell, })
    }
}

// EntryContainer

#[derive(Clone, Debug)]
pub struct EntryContainer {
    pub entry: Entry,
}

impl WriteToBytesMut for EntryContainer {
    fn write_to_bytes_mut<T>(&self, target: &mut T) where T: Target {
        let mut target_counter = TargetCounter::default();
        self.entry.write_to_bytes_mut(&mut target_counter);
        let entry_size_bytes: u32 = target_counter.bytes_written()
            .try_into()
            .unwrap();
        entry_size_bytes.write_to_bytes_mut(target);
        self.entry.write_to_bytes_mut(target);
    }
}

#[derive(Debug)]
pub enum ReadEntryContainerError {
    EntrySizeBytes(integer::ReadIntegerError),
    Entry(ReadEntryError),
}

impl ReadFromSource for EntryContainer {
    type Error = ReadEntryContainerError;

    fn read_from_source<S>(source: &mut S) -> Result<Self, Self::Error> where S: Source {
        let _entry_size_bytes = u32::read_from_source(source)
            .map_err(Self::Error::EntrySizeBytes)?;
        let entry = Entry::read_from_source(source)
            .map_err(Self::Error::Entry)?;
        Ok(EntryContainer { entry, })
    }
}

// ValueBlock

const VALUE_BLOCK_MAGIC: u64 = 0x5df58182f2741b7a;

#[derive(Clone, Debug)]
pub struct ValueBlock {
    value_block: Bytes,
}

impl WriteToBytesMut for ValueBlock {
    fn write_to_bytes_mut<T>(&self, target: &mut T) where T: Target {
        VALUE_BLOCK_MAGIC.write_to_bytes_mut(target);
        self.value_block.write_to_bytes_mut(target);
    }
}

#[derive(Debug)]
pub enum ReadValueBlockError {
    Magic(integer::ReadIntegerError),
    InvalidMagic { expected: u64, provided: u64, },
    ValueBlock(alloc_pool_pack::bytes::ReadBytesError),
}

impl ReadFromSource for ValueBlock {
    type Error = ReadValueBlockError;

    fn read_from_source<S>(source: &mut S) -> Result<Self, Self::Error> where S: Source {
        let magic = u64::read_from_source(source)
            .map_err(Self::Error::Magic)?;
        if magic != VALUE_BLOCK_MAGIC {
            return Err(Self::Error::InvalidMagic {
                expected: VALUE_BLOCK_MAGIC,
                provided: magic,
            });
        }
        let value_block = Bytes::read_from_source(source)
            .map_err(Self::Error::ValueBlock)?;
        Ok(ValueBlock { value_block, })
    }
}

impl From<kv::Value> for ValueBlock {
    fn from(value: kv::Value) -> ValueBlock {
        ValueBlock {
            value_block: value.value_bytes,
        }
    }
}

impl From<ValueBlock> for Bytes {
    fn from(value_block: ValueBlock) -> Bytes {
        value_block.value_block
    }
}

// BlockSerializer

pub struct BlockSerializer {
    block_bytes: BytesMut,
    entries_left: usize,
}

impl BlockSerializer {
    pub fn start(node_type: NodeType, entries_count: usize, mut block_bytes: BytesMut) -> BlockSerializerContinue {
        block_bytes.clear();
        let block_header = BlockHeader { node_type, entries_count, };
        block_header.write_to_bytes_mut(&mut block_bytes);
        if entries_count == 0 {
            BlockSerializerContinue::Done(block_bytes)
        } else {
            BlockSerializerContinue::More(BlockSerializer { block_bytes, entries_left: entries_count, })
        }
    }

    pub fn entry(mut self, entry: Entry) -> BlockSerializerContinue {
        let entry_container = EntryContainer { entry, };
        entry_container.write_to_bytes_mut(&mut self.block_bytes);
        self.entries_left -= 1;
        if self.entries_left == 0 {
            BlockSerializerContinue::Done(self.block_bytes)
        } else {
            BlockSerializerContinue::More(self)
        }
    }
}

pub enum BlockSerializerContinue {
    Done(BytesMut),
    More(BlockSerializer),
}

// BlockDeserializeIter

#[derive(Debug)]
pub enum Error {
    BlockHeader(ReadBlockHeaderError),
    EntryContainer(ReadEntryContainerError),
    ValueBlock(ReadValueBlockError),
    EntrySnapshotEntryContainerSizeBytes(integer::ReadIntegerError),
    EntrySnapshotKeyLength(integer::ReadIntegerError),
    EntrySnapshotJumpRef(ReadJumpRefError),
    EntrySnapshotValueCell(ReadValueCellError),
}

pub struct BlockDeserializeIter<'a> {
    source: SourceBytesRef<'a>,
    block_header: BlockHeader,
    entries_read: usize,
}

pub fn block_deserialize_iter(block_bytes: &Bytes) -> Result<BlockDeserializeIter<'_>, Error> {
    let mut source = SourceBytesRef::from(block_bytes);
    let block_header = BlockHeader::read_from_source(&mut source)
        .map_err(Error::BlockHeader)?;
    Ok(BlockDeserializeIter {
        source,
        block_header,
        entries_read: 0,
    })
}

impl<'a> BlockDeserializeIter<'a> {
    pub fn block_header(&self) -> &BlockHeader {
        &self.block_header
    }

    pub fn entry_snapshot(&mut self) -> Result<Option<BlockDeserializeEntrySnapshotKey<'a, '_>>, Error> {
        if self.entries_read >= self.block_header.entries_count {
            return Ok(None);
        }
        let entry_size_bytes = u32::read_from_source(&mut self.source)
            .map_err(Error::EntrySnapshotEntryContainerSizeBytes)?;
        let start_source = self.source.clone();
        let key_length = u32::read_from_source(&mut self.source)
            .map_err(Error::EntrySnapshotKeyLength)?;
        self.entries_read += 1;
        Ok(Some(BlockDeserializeEntrySnapshotKey {
            start_source,
            source: &mut self.source,
            entry_size_bytes,
            key_length,
        }))
    }
}

pub struct BlockDeserializeEntrySnapshotKey<'a, 'b> {
    start_source: SourceBytesRef<'a>,
    source: &'b mut SourceBytesRef<'a>,
    entry_size_bytes: u32,
    key_length: u32,
}

impl<'a, 'b> BlockDeserializeEntrySnapshotKey<'a, 'b> {
    pub fn peek_key_slice(&mut self) -> &[u8] {
        &self.source.slice()[.. self.key_length as usize]
    }

    pub fn proceed_to_jump_ref(self) -> Result<(JumpRef, BlockDeserializeEntrySnapshotJumpRef<'a, 'b>), Error> {
        let key_source = self.source.clone();
        self.source.advance(self.key_length as usize);
        let jump_ref = JumpRef::read_from_source(self.source)
            .map_err(Error::EntrySnapshotJumpRef)?;
        Ok((
            jump_ref,
            BlockDeserializeEntrySnapshotJumpRef {
                start_source: self.start_source,
                key_source,
                source: self.source,
                entry_size_bytes: self.entry_size_bytes,
                key_length: self.key_length,
            },
        ))
    }

    pub fn skip_entry(self) {
        *self.source = self.start_source;
        self.source.advance(self.entry_size_bytes as usize);
    }
}

pub struct BlockDeserializeEntrySnapshotJumpRef<'a, 'b> {
    start_source: SourceBytesRef<'a>,
    key_source: SourceBytesRef<'a>,
    source: &'b mut SourceBytesRef<'a>,
    entry_size_bytes: u32,
    key_length: u32,
}

impl<'a, 'b> BlockDeserializeEntrySnapshotJumpRef<'a, 'b> {
    pub fn peek_key_slice(&mut self) -> &[u8] {
        &self.key_source.slice()[.. self.key_length as usize]
    }

    pub fn proceed_to_value_cell(self) -> Result<kv::ValueCell<ValueRef>, Error> {
        let value_cell = kv::ValueCell::read_from_source(self.source)
            .map_err(Error::EntrySnapshotValueCell)?;
        Ok(value_cell)
    }

    pub fn skip_entry(self) {
        *self.source = self.start_source;
        self.source.advance(self.entry_size_bytes as usize);
    }
}

impl<'a> Iterator for BlockDeserializeIter<'a> {
    type Item = Result<Entry, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.entries_read >= self.block_header.entries_count {
            None
        } else {
            let maybe_entry_container = EntryContainer::read_from_source(&mut self.source)
                .map_err(Error::EntryContainer);
            let maybe_entry = maybe_entry_container
                .map(|EntryContainer { entry, }| entry);
            self.entries_read += 1;
            Some(maybe_entry)
        }
    }
}
