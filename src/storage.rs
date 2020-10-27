use serde_derive::{
    Serialize,
    Deserialize,
};

use super::{
    kv,
    blockwheel::block,
};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Entry<'a> {
    #[serde(borrow)]
    pub jump_ref: JumpRef<'a>,
    #[serde(borrow)]
    pub key: &'a [u8],
    #[serde(borrow)]
    pub value: &'a [u8],
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
    EntrySerialize(bincode::Error),
}

pub fn serialize_key_value(kv: &kv::KeyValue, jump_ref: JumpRef<'_>, block_bytes: &mut Vec<u8>) -> Result<(), Error> {
    let entry = Entry {
        jump_ref,
        key: kv.key_data(),
        value: kv.value_data(),
    };
    bincode::serialize_into(block_bytes, &entry)
        .map_err(Error::EntrySerialize)
}
