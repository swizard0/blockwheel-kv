use super::{
    kv::KeyValue,
    context::Context,
};

#[derive(Debug)]
pub enum Request<C> where C: Context {
    Info(RequestInfo<C::Info>),
    Insert(RequestInsert<C::Insert>),
}

#[derive(Debug)]
pub struct RequestInfo<C> {
    pub context: C,
}

#[derive(Debug)]
pub struct RequestInsert<C> {
    pub key_value: KeyValue,
    pub context: C,
}
