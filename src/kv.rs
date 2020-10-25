use std::{
    hash::{
        Hash,
        Hasher,
    },
    io::{
        self,
        Write,
        Cursor,
        IoSlice,
    },
    iter::Extend,
};

use siphasher::sip::{
    SipHasher13,
};

use alloc_pool::bytes::{
    Bytes,
    BytesMut,
};

#[derive(Clone, PartialEq, Debug)]
pub struct Key {
    key_header: KeyHeader,
    key_bytes: Bytes,
}

impl Key {
    fn data(&self) -> &[u8] {
        &self.key_bytes
    }
}

#[derive(Clone, PartialEq, Debug)]
struct KeyHeader {
    len: usize,
    hash: u64,
}

#[derive(Clone, PartialEq, Debug)]
pub struct KeyValue {
    key_header: KeyHeader,
    value_header: ValueHeader,
    key_value_bytes: Bytes,
}

#[derive(Clone, PartialEq, Debug)]
struct ValueHeader {
    offset: usize,
    len: usize,
}

impl KeyValue {
    fn key_data(&self) -> &[u8] {
        &self.key_value_bytes[.. self.key_header.len]
    }

    fn value_data(&self) -> &[u8] {
        &self.key_value_bytes[self.value_header.offset ..]
    }
}


pub struct Builder {
    bytes: BytesMut,
}

impl Builder {
    pub fn new(bytes: BytesMut) -> Self {
        Self { bytes, }
    }

    pub fn build_key(self) -> BuilderKey {
        let mut bytes = self.bytes;
        bytes.clear();
        BuilderKey { bytes, }
    }
}

pub struct BuilderKey {
    bytes: BytesMut,
}

impl BuilderKey {
    pub fn key_done(self) -> Key {
        let key_bytes = self.bytes.freeze();
        let len = key_bytes.len();
        let hash = key_hash(&key_bytes);
        Key { key_header: KeyHeader { len, hash, }, key_bytes, }
    }

    pub fn writer(&mut self) -> Writer<'_> {
        Writer::new(&mut self.bytes)
    }

    pub fn build_value(self) -> BuilderKeyValue {
        let bytes = self.bytes;
        let key_len = bytes.len();
        BuilderKeyValue { bytes, key_len, }
    }
}

pub struct BuilderKeyValue {
    bytes: BytesMut,
    key_len: usize,
}

impl BuilderKeyValue {
    pub fn writer(&mut self) -> Writer<'_> {
        Writer::new(&mut self.bytes)
    }

    pub fn key_value_done(self) -> KeyValue {
        let key_value_bytes = self.bytes.freeze();
        let key_len = self.key_len;
        let key_header = KeyHeader {
            len: key_len,
            hash: key_hash(&key_value_bytes[.. key_len]),
        };
        let value_header = ValueHeader {
            len: key_value_bytes.len() - key_len,
            offset: key_len,
        };
        KeyValue { key_header, value_header, key_value_bytes, }
    }
}

pub struct Writer<'a> {
    start_offset: u64,
    cursor: Cursor<&'a mut Vec<u8>>,
}

impl<'a> Writer<'a> {
    fn new(vec: &'a mut Vec<u8>) -> Self {
        let start_offset = vec.len() as u64;
        let mut cursor = Cursor::new(vec);
        cursor.set_position(start_offset);
        Self { start_offset, cursor, }
    }

    pub fn reset(&mut self) {
        let start_offset = self.start_offset;
        self.cursor.get_mut().truncate(start_offset as usize);
        self.cursor.set_position(start_offset);
    }
}

impl<'a> Extend<u8> for Writer<'a> {
    fn extend<T>(&mut self, iter: T) where T: IntoIterator<Item = u8> {
        self.cursor.get_mut().extend(iter);
        let new_len = self.cursor.get_ref().len();
        self.cursor.set_position(new_len as u64);
    }
}

impl<'a> Write for Writer<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.cursor.write(buf)
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        self.cursor.write_vectored(bufs)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn key_hash(data: &[u8]) -> u64 {
    let mut hasher = SipHasher13::new();
    data.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use alloc_pool::bytes::BytesMut;

    use super::{
        Builder,
        KeyValue,
    };

    #[test]
    fn only_key() {
        let mut key_builder = Builder::new(BytesMut::new_detached(Vec::new()))
            .build_key();
        key_builder
            .writer()
            .extend("my key".as_bytes().iter().cloned());
        let key = key_builder.key_done();
        assert_eq!(b"my key", key.data());
    }

    #[test]
    fn extend_and_write() {
        let mut key_builder = Builder::new(BytesMut::new_detached(Vec::new()))
            .build_key();
        key_builder
            .writer()
            .extend("my key".as_bytes().iter().cloned());
        let mut value_builder = key_builder
            .build_value();
        value_builder
            .writer()
            .write_all(b"my value")
            .unwrap();
        let kv = value_builder.key_value_done();
        assert_eq!(b"my key", kv.key_data());
        assert_eq!(b"my value", kv.value_data());
    }

    #[test]
    fn write_reset_and_extend_reset() {
        let mut key_builder = Builder::new(BytesMut::new_detached(Vec::new()))
            .build_key();
        let mut writer = key_builder
            .writer();
        writer.write_all(b"my key").unwrap();
        writer.reset();
        writer.write_all(b"my other key").unwrap();
        let mut value_builder = key_builder
            .build_value();
        let mut writer = value_builder
            .writer();
        writer.extend("my value".as_bytes().iter().cloned());
        writer.reset();
        writer.extend("my other value".as_bytes().iter().cloned());
        let kv = value_builder.key_value_done();
        assert_eq!(b"my other key", kv.key_data());
        assert_eq!(b"my other value", kv.value_data());
    }
}
