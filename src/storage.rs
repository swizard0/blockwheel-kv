use std::{
    marker::{
        PhantomData,
    },
};

use serde_derive::{
    Serialize,
    Deserialize,
};

use bincode::Options;

use alloc_pool::{
    bytes::Bytes,
};

use blockwheel_fs_ero::{
    block,
};

use crate::{
    kv,
    wheels::{
        BlockRef,
        WheelFilename,
    },
};

pub const BLOCK_MAGIC: u64 = 0xbde78ba3966ca503;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct BlockHeader {
    pub node_type: NodeType,
    pub entries_count: usize,
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub enum NodeType {
    Root { tree_entries_count: usize, },
    Leaf,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Entry<'a> {
    #[serde(borrow)]
    pub jump_ref: JumpRef<'a>,
    pub key: &'a [u8],
    #[serde(borrow)]
    pub value_cell: ValueCell<'a>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ValueCell<'a> {
    pub version: u64,
    #[serde(borrow)]
    pub cell: Cell<'a>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Cell<'a> {
    #[serde(borrow)]
    Value(ValueRef<'a>),
    Tombstone,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum JumpRef<'a> {
    None,
    Local(LocalRef),
    #[serde(borrow)]
    External(ExternalRef<'a>),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum ValueRef<'a> {
    Inline(&'a [u8]),
    Local(LocalRef),
    #[serde(borrow)]
    External(ExternalRef<'a>),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct LocalRef {
    pub block_id: block::Id,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ExternalRef<'a> {
    pub filename: &'a [u8],
    pub block_id: block::Id,
}

impl<'a> JumpRef<'a> {
    pub fn from_maybe_block_ref(maybe_block_ref: &'a Option<BlockRef>, current_blockwheel_filename: &WheelFilename) -> JumpRef<'a> {
        match maybe_block_ref {
            None =>
                JumpRef::None,
            Some(block_ref) if &block_ref.blockwheel_filename == current_blockwheel_filename =>
                JumpRef::Local(LocalRef {
                    block_id: block_ref.block_id.clone(),
                }),
            Some(block_ref) =>
                JumpRef::External(ExternalRef {
                    filename: &block_ref.blockwheel_filename,
                    block_id: block_ref.block_id.clone(),
                }),
        }
    }
}

impl<'a> From<&'a kv::ValueCell<kv::Value>> for ValueCell<'a> {
    fn from(value_cell: &'a kv::ValueCell<kv::Value>) -> ValueCell<'a> {
        ValueCell {
            version: value_cell.version,
            cell: match value_cell.cell {
                kv::Cell::Tombstone =>
                    Cell::Tombstone,
                kv::Cell::Value(kv::Value { ref value_bytes, }) =>
                    Cell::Value(ValueRef::Inline(value_bytes)),
            },
        }
    }
}

#[derive(Debug)]
pub enum Error {
    BlockMagicSerialize(bincode::Error),
    BlockHeaderSerialize(bincode::Error),
    EntrySerialize(bincode::Error),
    ValueBlockSerialize(bincode::Error),
    BlockMagicDeserialize(bincode::Error),
    InvalidBlockMagic { expected: u64, provided: u64, },
    BlockHeaderDeserialize(bincode::Error),
    EntryDeserialize(bincode::Error),
    ValueBlockDeserialize(bincode::Error),
}

pub struct BlockSerializer<B> {
    block_bytes: B,
    entries_left: usize,
}

impl<B> BlockSerializer<B> where B: AsMut<Vec<u8>> {
    pub fn start(node_type: NodeType, entries_count: usize, mut block_bytes: B) -> Result<BlockSerializerContinue<B>, Error> {
        block_bytes.as_mut().clear();
        bincode_options()
            .serialize_into(block_bytes.as_mut(), &BLOCK_MAGIC)
            .map_err(Error::BlockMagicSerialize)?;
        bincode_options()
            .serialize_into(block_bytes.as_mut(), &BlockHeader { node_type, entries_count, })
            .map_err(Error::BlockHeaderSerialize)?;
        Ok(if entries_count == 0 {
            BlockSerializerContinue::Done(block_bytes)
        } else {
            BlockSerializerContinue::More(BlockSerializer { block_bytes, entries_left: entries_count, })
        })
    }

    pub fn entry(mut self, entry: Entry) -> Result<BlockSerializerContinue<B>, Error> {
        bincode_options()
            .serialize_into(self.block_bytes.as_mut(), &entry)
            .map_err(Error::EntrySerialize)?;
        self.entries_left -= 1;
        Ok(if self.entries_left == 0 {
            BlockSerializerContinue::Done(self.block_bytes)
        } else {
            BlockSerializerContinue::More(self)
        })
    }
}

pub enum BlockSerializerContinue<B> {
    Done(B),
    More(BlockSerializer<B>),
}

pub struct BlockDeserializeIter<'a, R, O> where O: Options {
    deserializer: bincode::Deserializer<R, O>,
    block_header: BlockHeader,
    entries_read: usize,
    _marker: PhantomData<&'a ()>,
}

pub fn block_deserialize_iter<'a>(
    block_bytes: &'a Bytes,
)
    -> Result<BlockDeserializeIter<'a, impl bincode::BincodeRead<'a>, impl Options>, Error>
{
    let mut deserializer = bincode::Deserializer::from_slice(block_bytes, bincode_options());
    let magic: u64 = serde::Deserialize::deserialize(&mut deserializer)
        .map_err(Error::BlockMagicDeserialize)?;
    if magic != BLOCK_MAGIC {
        return Err(Error::InvalidBlockMagic { expected: BLOCK_MAGIC, provided: magic, });
    }
    let block_header: BlockHeader = serde::Deserialize::deserialize(&mut deserializer)
        .map_err(Error::BlockHeaderDeserialize)?;
    Ok(BlockDeserializeIter {
        deserializer,
        block_header,
        entries_read: 0,
        _marker: PhantomData,
    })
}

impl<'a, R, O> BlockDeserializeIter<'a, R, O> where O: Options {
    pub fn block_header(&self) -> &BlockHeader {
        &self.block_header
    }
}

#[derive(Clone, Debug)]
pub struct OwnedEntry {
    pub jump_ref: OwnedJumpRef,
    pub key: kv::Key,
    pub value_cell: kv::ValueCell<OwnedValueRef>,
}

impl OwnedEntry {
    pub fn from_entry<'a>(entry: &Entry<'a>, block_bytes: &'a Bytes) -> OwnedEntry {
        OwnedEntry {
            jump_ref: OwnedJumpRef::from_jump_ref(&entry.jump_ref, block_bytes),
            key: kv::Key {
                key_bytes: block_bytes.clone_subslice(&entry.key),
            },
            value_cell: kv::ValueCell {
                version: entry.value_cell.version,
                cell: match &entry.value_cell.cell {
                    Cell::Tombstone =>
                        kv::Cell::Tombstone,
                    Cell::Value(ValueRef::Inline(value)) => {
                        kv::Cell::Value(OwnedValueRef::Inline(kv::Value {
                            value_bytes: block_bytes.clone_subslice(value),
                        }))
                    },
                    Cell::Value(ValueRef::Local(local_ref)) =>
                        kv::Cell::Value(OwnedValueRef::Local(local_ref.clone())),
                    Cell::Value(ValueRef::External(ExternalRef { filename, block_id, })) =>
                        kv::Cell::Value(OwnedValueRef::External(BlockRef {
                            blockwheel_filename: block_bytes.clone_subslice(filename).into(),
                            block_id: block_id.clone(),
                        })),
                },
            },
        }
    }
}

impl<'a> From<&'a OwnedEntry> for Entry<'a> {
    fn from(entry: &'a OwnedEntry) -> Entry<'a> {
        Entry {
            jump_ref: (&entry.jump_ref).into(),
            key: &entry.key.key_bytes,
            value_cell: (&entry.value_cell).into(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum OwnedJumpRef {
    None,
    Local(LocalRef),
    External(BlockRef),
}

impl OwnedJumpRef {
    pub fn from_jump_ref<'a>(jump_ref: &JumpRef<'a>, block_bytes: &'a Bytes) -> OwnedJumpRef {
        match jump_ref {
            JumpRef::None =>
                OwnedJumpRef::None,
            JumpRef::Local(local_ref) =>
                OwnedJumpRef::Local(local_ref.clone()),
            JumpRef::External(ExternalRef { filename, block_id, }) =>
                OwnedJumpRef::External(BlockRef {
                    blockwheel_filename: block_bytes.clone_subslice(filename).into(),
                    block_id: block_id.clone(),
                }),
        }
    }
}

impl<'a> From<&'a OwnedJumpRef> for JumpRef<'a> {
    fn from(jump_ref: &'a OwnedJumpRef) -> JumpRef<'a> {
        match jump_ref {
            OwnedJumpRef::None =>
                JumpRef::None,
            OwnedJumpRef::Local(local_ref) =>
                JumpRef::Local(local_ref.clone()),
            OwnedJumpRef::External(BlockRef { blockwheel_filename, block_id, }) =>
                JumpRef::External(ExternalRef {
                    filename: &*blockwheel_filename,
                    block_id: block_id.clone(),
                }),
        }
    }
}

impl<'a> From<&'a kv::ValueCell<OwnedValueRef>> for ValueCell<'a> {
    fn from(value_cell: &'a kv::ValueCell<OwnedValueRef>) -> ValueCell<'a> {
        ValueCell {
            version: value_cell.version,
            cell: (&value_cell.cell).into(),
        }
    }
}

impl From<kv::ValueCell<kv::Value>> for kv::ValueCell<OwnedValueRef> {
    fn from(value_cell: kv::ValueCell<kv::Value>) -> kv::ValueCell<OwnedValueRef> {
        kv::ValueCell {
            version: value_cell.version,
            cell: value_cell.cell.into(),
        }
    }
}

impl<'a> From<&'a kv::Cell<OwnedValueRef>> for Cell<'a> {
    fn from(cell: &'a kv::Cell<OwnedValueRef>) -> Cell<'a> {
        match cell {
            kv::Cell::Value(value_ref) =>
                Cell::Value(value_ref.into()),
            kv::Cell::Tombstone =>
                Cell::Tombstone,
        }
    }
}

impl From<kv::Cell<kv::Value>> for kv::Cell<OwnedValueRef> {
    fn from(cell: kv::Cell<kv::Value>) -> kv::Cell<OwnedValueRef> {
        match cell {
            kv::Cell::Value(value) =>
                kv::Cell::Value(OwnedValueRef::Inline(value)),
            kv::Cell::Tombstone =>
                kv::Cell::Tombstone,
        }
    }
}

#[derive(Clone, Debug)]
pub enum OwnedValueRef {
    Inline(kv::Value),
    Local(LocalRef),
    External(BlockRef),
}

impl<'a> From<&'a OwnedValueRef> for ValueRef<'a> {
    fn from(value_ref: &'a OwnedValueRef) -> ValueRef<'a> {
        match value_ref {
            OwnedValueRef::Inline(value) =>
                ValueRef::Inline(&value.value_bytes),
            OwnedValueRef::Local(local_ref) =>
                ValueRef::Local(local_ref.clone()),
            OwnedValueRef::External(block_ref) =>
                ValueRef::External(ExternalRef {
                    filename: &block_ref.blockwheel_filename,
                    block_id: block_ref.block_id.clone(),
                }),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum OwnedValueBlockRef {
    Inline(kv::Value),
    Ref(BlockRef),
}

impl OwnedValueBlockRef {
    pub fn from_owned_value_ref(value_ref: OwnedValueRef, current_blockwheel_filename: &WheelFilename) -> OwnedValueBlockRef {
        match value_ref {
            OwnedValueRef::Inline(value) =>
                OwnedValueBlockRef::Inline(value),
            OwnedValueRef::Local(LocalRef { block_id, }) =>
                OwnedValueBlockRef::Ref(BlockRef {
                    blockwheel_filename: current_blockwheel_filename.clone(),
                    block_id,
                }),
            OwnedValueRef::External(block_ref) =>
                OwnedValueBlockRef::Ref(block_ref),
        }
    }
}

impl kv::ValueCell<OwnedValueBlockRef> {
    pub fn into_owned_value_ref(self, current_blockwheel_filename: &WheelFilename) -> kv::ValueCell<OwnedValueRef> {
        kv::ValueCell {
            version: self.version,
            cell: match self.cell {
                kv::Cell::Value(OwnedValueBlockRef::Inline(value)) =>
                    kv::Cell::Value(OwnedValueRef::Inline(value)),
                kv::Cell::Value(OwnedValueBlockRef::Ref(BlockRef { blockwheel_filename, block_id, }))
                    if &blockwheel_filename == current_blockwheel_filename =>
                    kv::Cell::Value(OwnedValueRef::Local(LocalRef { block_id, })),
                kv::Cell::Value(OwnedValueBlockRef::Ref(block_ref)) =>
                    kv::Cell::Value(OwnedValueRef::External(block_ref)),
                kv::Cell::Tombstone =>
                    kv::Cell::Tombstone,
            },
        }
    }
}

impl From<kv::KeyValuePair<kv::Value>> for kv::KeyValuePair<OwnedValueBlockRef> {
    fn from(kv_pair: kv::KeyValuePair<kv::Value>) -> kv::KeyValuePair<OwnedValueBlockRef> {
        kv::KeyValuePair {
            key: kv_pair.key,
            value_cell: kv_pair.value_cell.into(),
        }
    }
}

impl From<kv::ValueCell<kv::Value>> for kv::ValueCell<OwnedValueBlockRef> {
    fn from(value_cell: kv::ValueCell<kv::Value>) -> kv::ValueCell<OwnedValueBlockRef> {
        kv::ValueCell {
            version: value_cell.version,
            cell: value_cell.cell.into(),
        }
    }
}

impl From<kv::Cell<kv::Value>> for kv::Cell<OwnedValueBlockRef> {
    fn from(cell: kv::Cell<kv::Value>) -> kv::Cell<OwnedValueBlockRef> {
        match cell {
            kv::Cell::Value(value) =>
                kv::Cell::Value(OwnedValueBlockRef::Inline(value)),
            kv::Cell::Tombstone =>
                kv::Cell::Tombstone,
        }
    }
}

impl<'a, R, O> Iterator for BlockDeserializeIter<'a, R, O> where R: bincode::BincodeRead<'a>, O: Options {
    type Item = Result<Entry<'a>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.entries_read >= self.block_header.entries_count {
            None
        } else {
            self.entries_read += 1;
            let maybe_entry: Result<Entry<'_>, Error> = serde::Deserialize::deserialize(&mut self.deserializer)
                .map_err(Error::EntryDeserialize);
            match maybe_entry {
                Ok(entry) =>
                    Some(Ok(entry)),
                Err(error) =>
                    Some(Err(error)),
            }
        }
    }
}

pub const VALUE_BLOCK_MAGIC: u64 = 0x5df58182f2741b7a;

#[derive(Clone, Serialize, Deserialize, Debug)]
struct ValueBlock<'a> {
    value_block: &'a [u8],
}

pub fn value_block_serialize<B>(value_block: &[u8], mut block_bytes: B) -> Result<(), Error> where B: AsMut<Vec<u8>> {
    block_bytes.as_mut().clear();
    bincode_options()
        .serialize_into(block_bytes.as_mut(), &VALUE_BLOCK_MAGIC)
        .map_err(Error::BlockMagicSerialize)?;
    bincode_options()
        .serialize_into(block_bytes.as_mut(), &ValueBlock {
            value_block,
        })
        .map_err(Error::ValueBlockSerialize)?;
    Ok(())
}

pub fn value_block_deserialize(block_bytes: &Bytes) -> Result<Bytes, Error> {
    let mut deserializer = bincode::Deserializer::from_slice(block_bytes, bincode_options());
    let magic: u64 = serde::Deserialize::deserialize(&mut deserializer)
        .map_err(Error::BlockMagicDeserialize)?;
    if magic != VALUE_BLOCK_MAGIC {
        return Err(Error::InvalidBlockMagic { expected: VALUE_BLOCK_MAGIC, provided: magic, });
    }
    let value_block: ValueBlock<'_> = serde::Deserialize::deserialize(&mut deserializer)
        .map_err(Error::ValueBlockDeserialize)?;
    Ok(block_bytes.clone_subslice(value_block.value_block))
}

fn bincode_options() -> impl Options {
    bincode::DefaultOptions::new()
        .with_no_limit()
        .with_big_endian()
        .with_fixint_encoding()
        .allow_trailing_bytes()
}
