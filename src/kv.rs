use alloc_pool::bytes::Bytes;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Key {
    pub key_bytes: Bytes,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Value {
    pub value_bytes: Bytes,
}
