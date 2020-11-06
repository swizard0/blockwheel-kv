use std::{
    ops::{
        DerefMut,
    },
    marker::PhantomData,
};

use serde_derive::{
    Serialize,
    Deserialize,
};

use bincode::Options;

use super::{
    blockwheel::block,
};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct BlockHeader {
    pub node_type: NodeType,
    pub entries_count: usize,
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub enum NodeType { Root, Leaf, }

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Entry<'a> {
    #[serde(borrow)]
    pub jump_ref: JumpRef<'a>,
    pub key: &'a [u8],
    #[serde(borrow)]
    pub value_cell: ValueCell<'a>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum ValueCell<'a> {
    Value { value: &'a [u8], },
    Tombstone,
    Blackmark,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum JumpRef<'a> {
    None,
    Local(LocalJumpRef),
    #[serde(borrow)]
    External(ExternalJumpRef<'a>),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct LocalJumpRef {
    pub block_id: block::Id,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ExternalJumpRef<'a> {
    pub filename: &'a str,
    pub block_id: block::Id,
}

#[derive(Debug)]
pub enum Error {
    BlockHeaderSerialize(bincode::Error),
    EntrySerialize(bincode::Error),
    BlockHeaderDeserialize(bincode::Error),
    EntryDeserialize(bincode::Error),
}

pub struct BlockSerializer<B> {
    block_bytes: B,
    entries_left: usize,
}

impl<B> BlockSerializer<B> where B: DerefMut<Target = Vec<u8>> {
    pub fn start(node_type: NodeType, entries_count: usize, mut block_bytes: B) -> Result<BlockSerializerContinue<B>, Error> {
        block_bytes.deref_mut().clear();
        bincode_options()
            .serialize_into(block_bytes.deref_mut(), &BlockHeader { node_type, entries_count, })
            .map_err(Error::BlockHeaderSerialize)?;
        Ok(if entries_count == 0 {
            BlockSerializerContinue::Done(block_bytes)
        } else {
            BlockSerializerContinue::More(BlockSerializer { block_bytes, entries_left: entries_count, })
        })
    }

    pub fn entry(mut self, key: &[u8], value_cell: ValueCell, jump_ref: JumpRef) -> Result<BlockSerializerContinue<B>, Error> {
        let entry = Entry { key, value_cell, jump_ref, };
        bincode_options()
            .serialize_into(self.block_bytes.deref_mut(), &entry)
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
    block_bytes: &'a [u8],
)
    -> Result<BlockDeserializeIter<'a, impl bincode::BincodeRead<'a>, impl Options>, Error>
{
    let mut deserializer = bincode::Deserializer::from_slice(block_bytes, bincode_options());
    let block_header: BlockHeader = serde::Deserialize::deserialize(&mut deserializer)
        .map_err(Error::BlockHeaderDeserialize)?;
    Ok(BlockDeserializeIter {
        deserializer,
        block_header,
        entries_read: 0,
        _marker: PhantomData,
    })
}

#[allow(dead_code)]
impl<'a, R, O> BlockDeserializeIter<'a, R, O> where O: Options {
    pub fn block_header(&self) -> &BlockHeader {
        &self.block_header
    }
}

impl<'a, R, O> Iterator for BlockDeserializeIter<'a, R, O> where R: bincode::BincodeRead<'a>, O: Options {
    type Item = Result<Entry<'a>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.entries_read >= self.block_header.entries_count {
            None
        } else {
            self.entries_read += 1;
            Some(
                serde::Deserialize::deserialize(&mut self.deserializer)
                    .map_err(Error::EntryDeserialize)
            )
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
