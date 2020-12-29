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
    Value { value: &'a [u8], },
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
        match value_cell {
            &kv::ValueCell { version, cell: kv::Cell::Value(ref value), } =>
                ValueCell { version, cell: Cell::Value { value: &value.value_bytes, }, },
            &kv::ValueCell { version, cell: kv::Cell::Tombstone, } =>
                ValueCell { version, cell: Cell::Tombstone, },
        }
    }
}

#[derive(Debug)]
pub enum Error {
    BlockMagicSerialize(bincode::Error),
    BlockHeaderSerialize(bincode::Error),
    EntrySerialize(bincode::Error),
    BlockMagicDeserialize(bincode::Error),
    InvalidBlockMagic { expected: u64, provided: u64, },
    BlockHeaderDeserialize(bincode::Error),
    EntryDeserialize(bincode::Error),
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

    pub fn entry(mut self, key: &kv::Key, value_cell: &kv::ValueCell, jump_ref: JumpRef) -> Result<BlockSerializerContinue<B>, Error> {
        let entry = Entry {
            key: &key.key_bytes,
            value_cell: value_cell.into(),
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

impl<'a, R, O> Iterator for BlockDeserializeIter<'a, R, O> where R: bincode::BincodeRead<'a>, O: Options {
    type Item = Result<(JumpRef<'a>, kv::KeyValuePair), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.entries_read >= self.block_header.entries_count {
            None
        } else {
            self.entries_read += 1;
            let maybe_entry: Result<Entry<'_>, Error> = serde::Deserialize::deserialize(&mut self.deserializer)
                .map_err(Error::EntryDeserialize);
            match maybe_entry {
                Ok(entry) => {
                    let key_from = unsafe {
                        // safe because both the starting and other pointer are either in bounds or one
                        // byte past the end of the same allocated object
                        entry.key.as_ptr().offset_from(self.block_bytes.as_ptr()) as usize
                    };
                    let key_to = key_from + entry.key.len();
                    let key_value_pair = kv::KeyValuePair {
                        key: kv::Key { key_bytes: self.block_bytes.subrange(key_from .. key_to), },
                        value_cell: kv::ValueCell {
                            version: entry.value_cell.version,
                            cell: match entry.value_cell.cell {
                                Cell::Value { value, } => {
                                    let value_from = unsafe {
                                        // safe because both the starting and other pointer are either in bounds or one
                                        // byte past the end of the same allocated object
                                        value.as_ptr().offset_from(self.block_bytes.as_ptr()) as usize
                                    };
                                    let value_to = value_from + value.len();
                                    kv::Cell::Value(kv::Value { value_bytes: self.block_bytes.subrange(value_from .. value_to), })
                                },
                                Cell::Tombstone =>
                                    kv::Cell::Tombstone,
                            },
                        },
                    };
                    Some(Ok((entry.jump_ref, key_value_pair)))
                },
                Err(error) =>
                    Some(Err(error)),
            }
        }
    }
}

fn bincode_options() -> impl Options {
    bincode::DefaultOptions::new()
        .with_no_limit()
        .with_big_endian()
        .with_fixint_encoding()
        .allow_trailing_bytes()
}
