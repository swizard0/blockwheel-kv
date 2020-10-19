use super::{
    context::Context,
};

#[derive(Debug)]
pub enum Request<C> where C: Context {
    Info(RequestInfo<C::Info>),
}

#[derive(Debug)]
pub struct RequestInfo<C> {
    pub context: C,
}
