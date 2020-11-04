use alloc_pool::bytes::Bytes;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Key {
    pub key_bytes: Bytes,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Value {
    pub value_bytes: Bytes,
}
