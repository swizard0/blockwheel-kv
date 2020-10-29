use super::{
    kv,
    context::Context,
};

#[derive(Debug)]
pub enum Request<C> where C: Context {
    Info(RequestInfo<C::Info>),
    Insert(RequestInsert<C::Insert>),
    Lookup(RequestLookup<C::Lookup>),
}

#[derive(Debug)]
pub struct RequestInfo<C> {
    pub context: C,
}

#[derive(Debug)]
pub struct RequestInsert<C> {
    pub key_value: kv::KeyValue,
    pub context: C,
}

#[derive(Debug)]
pub struct RequestLookup<C> {
    pub key: kv::Key,
    pub context: C,
}
