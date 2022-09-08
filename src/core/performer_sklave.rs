use arbeitssklave::{
    komm,
};

use crate::{
    AccessPolicy,
};

pub enum Order<A> where A: AccessPolicy {
    LookupRangeStreamCancel(komm::UmschlagAbbrechen<LookupRangeRef>),
    LookupRangeStreamNext(komm::Umschlag<LookupRangeStreamNext<A>, LookupRangeRef>),
}

pub struct LookupRangeRef {
    meister_ref: o1::set::Ref,
}

pub struct LookupRangeStreamNext<A> where A: AccessPolicy {
    pub rueckkopplung: komm::Rueckkopplung<A::Order, A::LookupRange>,
}

impl<A> From<komm::UmschlagAbbrechen<LookupRangeRef>> for Order<A> where A: AccessPolicy {
    fn from(v: komm::UmschlagAbbrechen<LookupRangeRef>) -> Order<A> {
        Order::LookupRangeStreamCancel(v)
    }
}

impl<A> From<komm::Umschlag<LookupRangeStreamNext<A>, LookupRangeRef>> for Order<A> where A: AccessPolicy {
    fn from(v: komm::Umschlag<LookupRangeStreamNext<A>, LookupRangeRef>) -> Order<A> {
        Order::LookupRangeStreamNext(v)
    }
}
