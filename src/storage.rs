use std::ops::Deref;

use serde_derive::{
    Serialize,
    Deserialize,
};

use bincode::Options;

use alloc_pool::{
    bytes::Bytes,
};

use crate::{
    kv,
    blockwheel::block,
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
    Inline { value: &'a [u8], },
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
    pub filename: &'a str,
    pub block_id: block::Id,
}

impl<'a> From<&'a kv::ValueCell> for ValueCell<'a> {
    fn from(value_cell: &'a kv::ValueCell) -> ValueCell<'a> {
        ValueCell {
            version: value_cell.version,
            cell: match value_cell.cell {
                kv::Cell::Tombstone =>
                    Cell::Tombstone,
                kv::Cell::Value(kv::Value { ref value_bytes, }) =>
                    Cell::Value(ValueRef::Inline { value: value_bytes, }),
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

    pub fn entry(mut self, key: &kv::Key, value_cell: ValueCell, jump_ref: JumpRef) -> Result<BlockSerializerContinue<B>, Error> {
        let entry = Entry {
            key: &key.key_bytes,
            value_cell,
            jump_ref,
        };
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
    block_bytes: &'a Bytes,
    block_header: BlockHeader,
    entries_read: usize,
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
        block_bytes,
        block_header,
        entries_read: 0,
    })
}

impl<'a, R, O> BlockDeserializeIter<'a, R, O> where O: Options {
    pub fn block_header(&self) -> &BlockHeader {
        &self.block_header
    }
}

pub struct OwnedEntry<P> {
    pub jump_ref: OwnedJumpRef<P>,
    pub key: kv::Key,
    pub value_cell: OwnedValueCell<P>,
}

pub enum OwnedJumpRef<P> {
    None,
    Local {
        block_id: block::Id,
    },
    External {
        filename: P,
        block_id: block::Id,
    },
}

pub struct OwnedValueCell<P> {
    pub version: u64,
    pub cell: OwnedCell<P>,
}

pub enum OwnedCell<P> {
    Value(OwnedValueRef<P>),
    Tombstone,
}

pub enum OwnedValueRef<P> {
    Inline(kv::Value),
    Local {
        block_id: block::Id,
    },
    External {
        filename: P,
        block_id: block::Id,
    },
}

impl<P> OwnedJumpRef<P> {
    pub fn as_ref<'a>(&'a self) -> JumpRef<'a> where P: Deref<Target = str> {
        match self {
            OwnedJumpRef::None =>
                JumpRef::None,
            OwnedJumpRef::Local { block_id, } =>
                JumpRef::Local(LocalRef {
                    block_id: block_id.clone(),
                }),
            OwnedJumpRef::External { filename, block_id, } =>
                JumpRef::External(ExternalRef {
                    filename: filename.deref(),
                    block_id: block_id.clone(),
                }),
        }
    }
}

impl<P> OwnedValueCell<P> {
    pub fn as_ref<'a>(&'a self) -> ValueCell<'a> where P: Deref<Target = str> {
        ValueCell {
            version: self.version,
            cell: match &self.cell {
                OwnedCell::Value(OwnedValueRef::Inline(value)) =>
                    Cell::Value(ValueRef::Inline { value: &value.value_bytes, }),
                OwnedCell::Value(OwnedValueRef::Local { block_id, }) =>
                    Cell::Value(ValueRef::Local(LocalRef {
                        block_id: block_id.clone(),
                    })),
                OwnedCell::Value(OwnedValueRef::External { filename, block_id, }) =>
                    Cell::Value(ValueRef::External(ExternalRef {
                        filename: filename.deref(),
                        block_id: block_id.clone(),
                    })),
                OwnedCell::Tombstone =>
                    Cell::Tombstone,
            },
        }
    }
}

impl<P> From<kv::ValueCell> for OwnedValueCell<P> {
    fn from(value_cell: kv::ValueCell) -> OwnedValueCell<P> {
        OwnedValueCell {
            version: value_cell.version,
            cell: match value_cell.cell {
                kv::Cell::Value(value) =>
                    OwnedCell::Value(OwnedValueRef::Inline(value)),
                kv::Cell::Tombstone =>
                    OwnedCell::Tombstone,
            },
        }
    }
}

impl<'a, R, O> Iterator for BlockDeserializeIter<'a, R, O> where R: bincode::BincodeRead<'a>, O: Options {
    type Item = Result<OwnedEntry<&'a str>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.entries_read >= self.block_header.entries_count {
            None
        } else {
            self.entries_read += 1;
            let maybe_entry: Result<Entry<'_>, Error> = serde::Deserialize::deserialize(&mut self.deserializer)
                .map_err(Error::EntryDeserialize);
            match maybe_entry {
                Ok(entry) =>
                    Some(Ok(OwnedEntry {
                        jump_ref: match entry.jump_ref {
                            JumpRef::None =>
                                OwnedJumpRef::None,
                            JumpRef::Local(LocalRef { block_id, }) =>
                                OwnedJumpRef::Local { block_id, },
                            JumpRef::External(ExternalRef { filename, block_id, }) =>
                                OwnedJumpRef::External { filename, block_id, },
                        },
                        key: kv::Key {
                            key_bytes: self.block_bytes.clone_subslice(&entry.key),
                        },
                        value_cell: OwnedValueCell {
                            version: entry.value_cell.version,
                            cell: match entry.value_cell.cell {
                                Cell::Tombstone =>
                                    OwnedCell::Tombstone,
                                Cell::Value(ValueRef::Inline { value, }) => {
                                    OwnedCell::Value(OwnedValueRef::Inline(kv::Value {
                                        value_bytes: self.block_bytes.clone_subslice(value),
                                    }))
                                },
                                Cell::Value(ValueRef::Local(LocalRef { block_id, })) =>
                                    OwnedCell::Value(OwnedValueRef::Local { block_id, }),
                                Cell::Value(ValueRef::External(ExternalRef { filename, block_id, })) =>
                                    OwnedCell::Value(OwnedValueRef::External { filename, block_id, }),
                            },
                        },
                    })),
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
