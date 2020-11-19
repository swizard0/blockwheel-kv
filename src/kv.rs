use std::borrow::Borrow;

use alloc_pool::bytes::Bytes;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Key {
    pub key_bytes: Bytes,
}

impl From<Bytes> for Key {
    fn from(key_bytes: Bytes) -> Key {
        Key { key_bytes, }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Value {
    pub value_bytes: Bytes,
}

impl From<Bytes> for Value {
    fn from(value_bytes: Bytes) -> Value {
        Value { value_bytes, }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ValueCell {
    pub version: u64,
    pub cell: Cell,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Cell {
    Value(Value),
    Tombstone,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct KeyValuePair {
    pub key: Key,
    pub value_cell: ValueCell,
}

impl Borrow<[u8]> for Key {
    fn borrow(&self) -> &[u8] {
        &self.key_bytes
    }
}
