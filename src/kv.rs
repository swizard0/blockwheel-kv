use alloc_pool::bytes::Bytes;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Key {
    pub key_bytes: Bytes,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Value {
    pub value_bytes: Bytes,
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
