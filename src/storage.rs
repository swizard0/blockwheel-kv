use std::{
    io,
};

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
pub enum Entry {
    JumpRefNoneCellTombstone {
        key_length: usize,
        cell_version: u64,
    },
    JumpRefNoneCellValue {
        key_length: usize,
        value_length: usize,
        cell_version: u64,
    },
    JumpRefLocalCellTombstone {
        block_id: block::Id,
        key_length: usize,
        cell_version: u64,
    },
    JumpRefLocalCellValue {
        block_id: block::Id,
        key_length: usize,
        value_length: usize,
        cell_version: u64,
    },
    JumpRefExternalCellTombstone {
        block_id: block::Id,
        filename_length: usize,
        key_length: usize,
        cell_version: u64,
    },
    JumpRefExternalCellValue {
        block_id: block::Id,
        filename_length: usize,
        key_length: usize,
        value_length: usize,
        cell_version: u64,
    },
}

#[derive(Clone, Debug)]
pub enum JumpRef<F> {
    None,
    Local(LocalJumpRef),
    External(ExternalJumpRef<F>),
}

#[derive(Clone, Debug)]
pub struct LocalJumpRef {
    pub block_id: block::Id,
}

#[derive(Clone, Debug)]
pub struct ExternalJumpRef<F> {
    pub filename: F,
    pub block_id: block::Id,
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

    pub fn entry<F>(
        mut self,
        key: &kv::Key,
        value_cell: &kv::ValueCell,
        jump_ref: JumpRef<F>,
    )
        -> Result<BlockSerializerContinue<B>, Error>
    where F: AsRef<[u8]>,
    {
        match (&jump_ref, &value_cell.cell) {
            (JumpRef::None, kv::Cell::Tombstone) => {
                bincode_options()
                    .serialize_into(self.block_bytes.as_mut(), &Entry::JumpRefNoneCellTombstone {
                        key_length: key.key_bytes.len(),
                        cell_version: value_cell.version,
                    })
                    .map_err(Error::EntrySerialize)?;
                self.block_bytes.as_mut().extend_from_slice(&key.key_bytes);
            },
            (JumpRef::None, kv::Cell::Value(value)) => {
                bincode_options()
                    .serialize_into(self.block_bytes.as_mut(), &Entry::JumpRefNoneCellValue {
                        key_length: key.key_bytes.len(),
                        value_length: value.value_bytes.len(),
                        cell_version: value_cell.version,
                    })
                    .map_err(Error::EntrySerialize)?;
                self.block_bytes.as_mut().extend_from_slice(&key.key_bytes);
                self.block_bytes.as_mut().extend_from_slice(&value.value_bytes);
            },
            (JumpRef::Local(LocalJumpRef { block_id, }), kv::Cell::Tombstone) => {
                bincode_options()
                    .serialize_into(self.block_bytes.as_mut(), &Entry::JumpRefLocalCellTombstone {
                        block_id: block_id.clone(),
                        key_length: key.key_bytes.len(),
                        cell_version: value_cell.version,
                    })
                    .map_err(Error::EntrySerialize)?;
                self.block_bytes.as_mut().extend_from_slice(&key.key_bytes);
            },
            (JumpRef::Local(LocalJumpRef { block_id, }), kv::Cell::Value(value)) => {
                bincode_options()
                    .serialize_into(self.block_bytes.as_mut(), &Entry::JumpRefLocalCellValue {
                        block_id: block_id.clone(),
                        key_length: key.key_bytes.len(),
                        value_length: value.value_bytes.len(),
                        cell_version: value_cell.version,
                    })
                    .map_err(Error::EntrySerialize)?;
                self.block_bytes.as_mut().extend_from_slice(&key.key_bytes);
                self.block_bytes.as_mut().extend_from_slice(&value.value_bytes);
            },
            (JumpRef::External(ExternalJumpRef { block_id, filename, }), kv::Cell::Tombstone) => {
                bincode_options()
                    .serialize_into(self.block_bytes.as_mut(), &Entry::JumpRefExternalCellTombstone {
                        block_id: block_id.clone(),
                        filename_length: filename.as_ref().len(),
                        key_length: key.key_bytes.len(),
                        cell_version: value_cell.version,
                    })
                    .map_err(Error::EntrySerialize)?;
                self.block_bytes.as_mut().extend_from_slice(filename.as_ref());
                self.block_bytes.as_mut().extend_from_slice(&key.key_bytes);
            },
            (JumpRef::External(ExternalJumpRef { block_id, filename, }), kv::Cell::Value(value)) => {
                bincode_options()
                    .serialize_into(self.block_bytes.as_mut(), &Entry::JumpRefExternalCellValue {
                        block_id: block_id.clone(),
                        filename_length: filename.as_ref().len(),
                        key_length: key.key_bytes.len(),
                        value_length: value.value_bytes.len(),
                        cell_version: value_cell.version,
                    })
                    .map_err(Error::EntrySerialize)?;
                self.block_bytes.as_mut().extend_from_slice(filename.as_ref());
                self.block_bytes.as_mut().extend_from_slice(&key.key_bytes);
                self.block_bytes.as_mut().extend_from_slice(&value.value_bytes);
            },
        }
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

pub struct BlockDeserializeIter {
    cursor: io::Cursor<Bytes>,
    block_header: BlockHeader,
    entries_read: usize,
}

impl BlockDeserializeIter {
    fn new(block_bytes: Bytes) -> Result<BlockDeserializeIter, Error> {
        let mut cursor = io::Cursor::new(block_bytes);
        let magic: u64 = bincode_options()
            .deserialize_from(&mut cursor)
            .map_err(Error::BlockMagicDeserialize)?;
        if magic != BLOCK_MAGIC {
            return Err(Error::InvalidBlockMagic { expected: BLOCK_MAGIC, provided: magic, });
        }
        let block_header: BlockHeader = bincode_options()
            .deserialize_from(&mut cursor)
            .map_err(Error::BlockHeaderDeserialize)?;
        Ok(BlockDeserializeIter {
            cursor,
            block_header,
            entries_read: 0,
        })
    }

    pub fn block_header(&self) -> &BlockHeader {
        &self.block_header
    }
}

impl Iterator for BlockDeserializeIter {
    type Item = Result<(JumpRef<Bytes>, kv::KeyValuePair), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.entries_read >= self.block_header.entries_count {
            None
        } else {
            self.entries_read += 1;
            let maybe_entry: Result<Entry, Error> = bincode_options()
                .deserialize_from(&mut self.cursor)
                .map_err(Error::EntryDeserialize);
            let offset = self.cursor.position() as usize;
            match maybe_entry {
                Ok(Entry::JumpRefNoneCellTombstone { key_length, cell_version, }) => {
                    let offset_to = offset + key_length;
                    let key_bytes = self.cursor.get_ref().subrange(offset .. offset_to);
                    self.cursor.set_position(offset_to as u64);
                    Some(Ok((
                        JumpRef::None,
                        kv::KeyValuePair {
                            key: kv::Key { key_bytes, },
                            value_cell: kv::ValueCell {
                                version: cell_version,
                                cell: kv::Cell::Tombstone,
                            },
                        },
                    )))
                },
                Ok(Entry::JumpRefNoneCellValue { key_length, value_length, cell_version, }) => {
                    let offset_value = offset + key_length;
                    let key_bytes = self.cursor.get_ref().subrange(offset .. offset_value);
                    let offset_to = offset_value + value_length;
                    let value_bytes = self.cursor.get_ref().subrange(offset_value .. offset_to);
                    self.cursor.set_position(offset_to as u64);
                    Some(Ok((
                        JumpRef::None,
                        kv::KeyValuePair {
                            key: kv::Key { key_bytes, },
                            value_cell: kv::ValueCell {
                                version: cell_version,
                                cell: kv::Cell::Value(kv::Value { value_bytes, }),
                            },
                        },
                    )))
                },
                Ok(Entry::JumpRefLocalCellTombstone { block_id, key_length, cell_version, }) => {
                    let offset_to = offset + key_length;
                    let key_bytes = self.cursor.get_ref().subrange(offset .. offset_to);
                    self.cursor.set_position(offset_to as u64);
                    Some(Ok((
                        JumpRef::Local(LocalJumpRef { block_id, }),
                        kv::KeyValuePair {
                            key: kv::Key { key_bytes, },
                            value_cell: kv::ValueCell {
                                version: cell_version,
                                cell: kv::Cell::Tombstone,
                            },
                        },
                    )))
                },
                Ok(Entry::JumpRefLocalCellValue { block_id, key_length, value_length, cell_version, }) => {
                    let offset_value = offset + key_length;
                    let key_bytes = self.cursor.get_ref().subrange(offset .. offset_value);
                    let offset_to = offset_value + value_length;
                    let value_bytes = self.cursor.get_ref().subrange(offset_value .. offset_to);
                    self.cursor.set_position(offset_to as u64);
                    Some(Ok((
                        JumpRef::Local(LocalJumpRef { block_id, }),
                        kv::KeyValuePair {
                            key: kv::Key { key_bytes, },
                            value_cell: kv::ValueCell {
                                version: cell_version,
                                cell: kv::Cell::Value(kv::Value { value_bytes, }),
                            },
                        },
                    )))
                },
                Ok(Entry::JumpRefExternalCellTombstone { block_id, filename_length, key_length, cell_version, }) => {
                    let offset_key = offset + filename_length;
                    let filename_bytes = self.cursor.get_ref().subrange(offset .. offset_key);
                    let offset_to = offset_key + key_length;
                    let key_bytes = self.cursor.get_ref().subrange(offset_key .. offset_to);
                    self.cursor.set_position(offset_to as u64);
                    Some(Ok((
                        JumpRef::External(ExternalJumpRef { block_id, filename: filename_bytes, }),
                        kv::KeyValuePair {
                            key: kv::Key { key_bytes, },
                            value_cell: kv::ValueCell {
                                version: cell_version,
                                cell: kv::Cell::Tombstone,
                            },
                        },
                    )))
                },
                Ok(Entry::JumpRefExternalCellValue { block_id, filename_length, key_length, value_length, cell_version, }) => {
                    let offset_key = offset + filename_length;
                    let filename_bytes = self.cursor.get_ref().subrange(offset .. offset_key);
                    let offset_value = offset_key + key_length;
                    let key_bytes = self.cursor.get_ref().subrange(offset_key .. offset_value);
                    let offset_to = offset_value + value_length;
                    let value_bytes = self.cursor.get_ref().subrange(offset_value .. offset_to);
                    self.cursor.set_position(offset_to as u64);
                    Some(Ok((
                        JumpRef::External(ExternalJumpRef { block_id, filename: filename_bytes, }),
                        kv::KeyValuePair {
                            key: kv::Key { key_bytes, },
                            value_cell: kv::ValueCell {
                                version: cell_version,
                                cell: kv::Cell::Value(kv::Value { value_bytes, }),
                            },
                        },
                    )))
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
