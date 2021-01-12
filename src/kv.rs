use std::borrow::Borrow;

use alloc_pool::bytes::{
    Bytes,
    BytesMut,
};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Key {
    pub key_bytes: Bytes,
}

impl From<Bytes> for Key {
    fn from(key_bytes: Bytes) -> Key {
        Key { key_bytes, }
    }
}

impl From<BytesMut> for Key {
    fn from(key_bytes: BytesMut) -> Key {
        key_bytes.freeze().into()
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

impl From<BytesMut> for Value {
    fn from(value_bytes: BytesMut) -> Value {
        value_bytes.freeze().into()
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ValueCell<V> {
    pub version: u64,
    pub cell: Cell<V>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Cell<V> {
    Value(V),
    Tombstone,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct KeyValuePair<V> {
    pub key: Key,
    pub value_cell: ValueCell<V>,
}

impl Borrow<[u8]> for Key {
    fn borrow(&self) -> &[u8] {
        &self.key_bytes
    }
}
