use std::hash::{
    Hash,
    Hasher,
};

use siphasher::sip::{
    SipHasher13,
};

use alloc_pool::bytes::{
    Bytes,
    BytesMut,
    BytesPool,
};

pub struct Builder<'a> {
    bytes_pool: &'a BytesPool,
}

impl<'a> Builder<'a> {
    pub fn new(bytes_pool: &'a BytesPool) -> Self {
        Self { bytes_pool, }
    }

    pub fn write_key<W, E>(
        self,
        mut key_writer: W,
    )
        -> Result<BuilderKey<'a>, E>
    where W: FnMut(&mut Vec<u8>) -> Result<(), E>
    {
        let mut key_bytes = self.bytes_pool.lend();
        let () = key_writer(&mut key_bytes)?;
        Ok(BuilderKey {
            bytes_pool: self.bytes_pool,
            key_bytes,
        })
    }
}

pub struct BuilderKey<'a> {
    bytes_pool: &'a BytesPool,
    key_bytes: BytesMut,
}

impl<'a> BuilderKey<'a> {
    pub fn key(mut self) -> Key {
        let key_bytes = self.key_bytes.freeze();
        let data: &[u8] = &key_bytes;
        let mut hasher = SipHasher13::new();
        data.hash(&mut hasher);
        let key_hash = hasher.finish();
        Key { key_bytes, key_hash, }
    }

    pub fn write_value<W, E>(
        self,
        mut value_writer: W,
    )
        -> Result<KeyValue, E>
    where W: FnMut(&mut Vec<u8>) -> Result<(), E>
    {
        let mut bytes = self.bytes_pool.lend();
        let () = value_writer(&mut bytes)?;
        let key = self.key();
        Ok(KeyValue { key, value_bytes: bytes.freeze(), })
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct Key {
    key_bytes: Bytes,
    key_hash: u64,
}

#[derive(Clone, PartialEq, Debug)]
pub struct KeyValue {
    key: Key,
    value_bytes: Bytes,
}
