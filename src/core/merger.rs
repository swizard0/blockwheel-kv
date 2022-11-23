use std::{
    cmp::{
        Ordering,
    },
};

use crate::{
    kv,
    storage,
};

impl kont_utils::iters_merger::ComparableItem for kv::KeyValuePair<storage::ValueRef> {
    fn compare_primary(&self, other: &Self) -> Ordering {
        self.key.key_bytes.cmp(&other.key.key_bytes)
    }

    fn compare_secondary(&self, other: &Self) -> Ordering {
        self.value_cell.version.cmp(&other.value_cell.version)
    }
}

pub type ItersMergerCps<V, S> =
    kont_utils::iters_merger::Cps<V, S, kv::KeyValuePair<storage::ValueRef>>;
pub type Kont<V, S> =
    kont_utils::iters_merger::Kont<V, S, kv::KeyValuePair<storage::ValueRef>>;
pub type KontScheduleIterAwait<V, S> =
    kont_utils::iters_merger::KontScheduleIterAwait<V, S, kv::KeyValuePair<storage::ValueRef>>;
// pub type KontScheduleIterAwaitNext<V, S> =
//     kont_utils::iters_merger::KontScheduleIterAwaitNext<V, S, kv::KeyValuePair<storage::ValueRef>>;
pub type KontAwaitScheduled<V, S> =
    kont_utils::iters_merger::KontAwaitScheduled<V, S, kv::KeyValuePair<storage::ValueRef>>;
pub type KontAwaitScheduledNext<V, S> =
    kont_utils::iters_merger::KontAwaitScheduledNext<V, S, kv::KeyValuePair<storage::ValueRef>>;
pub type KontEmitDeprecated<V, S> =
    kont_utils::iters_merger::KontEmitDeprecated<V, S, kv::KeyValuePair<storage::ValueRef>>;
pub type KontEmitDeprecatedNext<V, S> =
    kont_utils::iters_merger::KontEmitDeprecatedNext<V, S, kv::KeyValuePair<storage::ValueRef>>;
pub type KontEmitItem<V, S> =
    kont_utils::iters_merger::KontEmitItem<V, S, kv::KeyValuePair<storage::ValueRef>>;
pub type KontEmitItemNext<V, S> =
    kont_utils::iters_merger::KontEmitItemNext<V, S, kv::KeyValuePair<storage::ValueRef>>;
