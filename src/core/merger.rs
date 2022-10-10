use std::{
    ops::{
        DerefMut,
    },
    cmp::{
        Ordering,
    },
    collections::{
        BinaryHeap,
    },
};

use crate::{
    kv,
    storage,
};

pub struct ItersMergerCps<V, S> {
    state: State,
    env: Env<V, S>,
}

pub enum Kont<V, S> where V: DerefMut<Target = Vec<S>> {
    ScheduleIterAwait(KontScheduleIterAwait<V, S>),
    AwaitScheduled(KontAwaitScheduled<V, S>),
    EmitDeprecated(KontEmitDeprecated<V, S>),
    EmitItem(KontEmitItem<V, S>),
    Finished,
}

pub struct KontScheduleIterAwait<V, S> where V: DerefMut<Target = Vec<S>> {
    pub await_iter: S,
    pub next: KontScheduleIterAwaitNext<V, S>,
}

pub struct KontScheduleIterAwaitNext<V, S> where V: DerefMut<Target = Vec<S>> {
    state_await: StateAwait,
    env: Env<V, S>,
}

pub struct KontAwaitScheduled<V, S> where V: DerefMut<Target = Vec<S>> {
    pub next: KontAwaitScheduledNext<V, S>,
}

pub struct KontAwaitScheduledNext<V, S> where V: DerefMut<Target = Vec<S>> {
    state_await: StateAwait,
    env: Env<V, S>,
}

pub struct KontEmitDeprecated<V, S> where V: DerefMut<Target = Vec<S>> {
    pub item: kv::KeyValuePair<storage::OwnedValueBlockRef>,
    pub next: KontEmitDeprecatedNext<V, S>,
}

pub struct KontEmitDeprecatedNext<V, S> where V: DerefMut<Target = Vec<S>> {
    env: Env<V, S>,
}

pub struct KontEmitItem<V, S> where V: DerefMut<Target = Vec<S>>  {
    pub item: kv::KeyValuePair<storage::OwnedValueBlockRef>,
    pub next: KontEmitItemNext<V, S>,
}

pub struct KontEmitItemNext<V, S> where V: DerefMut<Target = Vec<S>> {
    env: Env<V, S>,
}

struct Env<V, S> {
    iters: V,
    fronts: BinaryHeap<Front<S>>,
    candidate: Option<kv::KeyValuePair<storage::OwnedValueBlockRef>>,
}

struct Front<S> {
    front_item: kv::KeyValuePair<storage::OwnedValueBlockRef>,
    iter: S,
}

enum State {
    Await(StateAwait),
    Flush,
}

#[derive(Default)]
struct StateAwait {
    pending_count: usize,
}

impl<V, S> ItersMergerCps<V, S> {
    pub fn new(iters: V) -> Self {
        Self {
            state: State::Await(StateAwait::default()),
            env: Env {
                iters,
                fronts: BinaryHeap::new(),
                candidate: None,
            },
        }
    }
}

impl<V, S> ItersMergerCps<V, S> where V: DerefMut<Target = Vec<S>> {
    pub fn step(mut self) -> Kont<V, S> {
        loop {
            match self.state {

                State::Await(StateAwait { pending_count, }) if !self.env.iters.is_empty() => {
                    let await_iter = self.env.iters.pop().unwrap();
                    return Kont::ScheduleIterAwait(KontScheduleIterAwait {
                        await_iter,
                        next: KontScheduleIterAwaitNext {
                            state_await: StateAwait {
                                pending_count: pending_count + 1,
                            },
                            env: self.env,
                        },
                    });
                },

                State::Await(StateAwait { pending_count, }) if pending_count == 0 =>
                    self.state = State::Flush,

                State::Await(StateAwait { pending_count, }) =>
                    return Kont::AwaitScheduled(KontAwaitScheduled {
                        next: KontAwaitScheduledNext {
                            state_await: StateAwait { pending_count, },
                            env: self.env,
                        },
                    }),

                State::Flush =>
                    match self.env.candidate.take() {
                        None =>
                            match self.env.fronts.pop() {
                                None if self.env.iters.is_empty() =>
                                    return Kont::Finished,
                                None =>
                                    self.state = State::Await(StateAwait::default()),
                                Some(Front { front_item, iter, }) => {
                                    self.env.candidate = Some(front_item);
                                    self.env.iters.push(iter);
                                },
                            },
                        Some(candidate_item) =>
                            match self.env.fronts.peek() {
                                None if self.env.iters.is_empty() =>
                                    return Kont::EmitItem(KontEmitItem {
                                        item: candidate_item,
                                        next: KontEmitItemNext { env: self.env, },
                                    }),
                                None => {
                                    self.env.candidate = Some(candidate_item);
                                    self.state = State::Await(StateAwait::default());
                                },
                                Some(Front { front_item, .. }) =>
                                    if candidate_item.key.key_bytes == front_item.key.key_bytes {
                                        let should_swap = front_item.value_cell.version < candidate_item.value_cell.version;
                                        let Front { front_item: next_front_item, iter, } =
                                            self.env.fronts.pop().unwrap();
                                        let (deprecated_item, next_candidate_item) =
                                            if should_swap {
                                                (next_front_item, candidate_item)
                                            } else {
                                                (candidate_item, next_front_item)
                                            };
                                        self.env.candidate = Some(next_candidate_item);
                                        self.env.iters.push(iter);
                                        return Kont::EmitDeprecated(KontEmitDeprecated {
                                            item: deprecated_item,
                                            next: KontEmitDeprecatedNext { env: self.env, },
                                        });
                                    } else if self.env.iters.is_empty() {
                                        return Kont::EmitItem(KontEmitItem {
                                            item: candidate_item,
                                            next: KontEmitItemNext { env: self.env, },
                                        });
                                    } else {
                                        self.env.candidate = Some(candidate_item);
                                        self.state = State::Await(StateAwait::default());
                                    },
                            },
                    },

            }
        }
    }
}

impl<V, S> From<KontScheduleIterAwaitNext<V, S>> for ItersMergerCps<V, S> where V: DerefMut<Target = Vec<S>> {
    fn from(kont: KontScheduleIterAwaitNext<V, S>) -> ItersMergerCps<V, S> {
        ItersMergerCps {
            state: State::Await(kont.state_await),
            env: kont.env,
        }
    }
}

impl<V, S> KontScheduleIterAwaitNext<V, S> where V: DerefMut<Target = Vec<S>> {
    pub fn proceed(self) -> Kont<V, S> {
        ItersMergerCps::from(self).step()
    }
}

impl<V, S> From<KontAwaitScheduledNext<V, S>> for ItersMergerCps<V, S> where V: DerefMut<Target = Vec<S>> {
    fn from(kont: KontAwaitScheduledNext<V, S>) -> ItersMergerCps<V, S> {
        ItersMergerCps {
            state: State::Await(kont.state_await),
            env: kont.env,
        }
    }
}

impl<V, S> KontAwaitScheduledNext<V, S> where V: DerefMut<Target = Vec<S>> {
    pub fn item_arrived(mut self, await_iter: S, item: kv::KeyValuePair<storage::OwnedValueBlockRef>) -> Kont<V, S> {
        assert!(self.state_await.pending_count > 0);
        self.env.fronts.push(Front { front_item: item, iter: await_iter, });
        self.state_await.pending_count -= 1;
        ItersMergerCps::from(self).step()
    }

    pub fn no_more(mut self) -> Kont<V, S> {
        assert!(self.state_await.pending_count > 0);
        self.state_await.pending_count -= 1;
        ItersMergerCps::from(self).step()
    }
}

impl<V, S> From<KontEmitDeprecatedNext<V, S>> for ItersMergerCps<V, S> where V: DerefMut<Target = Vec<S>> {
    fn from(kont: KontEmitDeprecatedNext<V, S>) -> ItersMergerCps<V, S> {
        ItersMergerCps {
            state: State::Flush,
            env: kont.env,
        }
    }
}

impl<V, S> KontEmitDeprecatedNext<V, S> where V: DerefMut<Target = Vec<S>> {
    pub fn proceed(self) -> Kont<V, S> {
        ItersMergerCps::from(self).step()
    }
}

impl<V, S> From<KontEmitItemNext<V, S>> for ItersMergerCps<V, S> where V: DerefMut<Target = Vec<S>> {
    fn from(kont: KontEmitItemNext<V, S>) -> ItersMergerCps<V, S> {
        ItersMergerCps {
            state: State::Await(StateAwait::default()),
            env: kont.env,
        }
    }
}

impl<V, S> KontEmitItemNext<V, S> where V: DerefMut<Target = Vec<S>> {
    pub fn proceed(self) -> Kont<V, S> {
        ItersMergerCps::from(self).step()
    }
}

impl<S> PartialEq for Front<S> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<S> Eq for Front<S> { }

impl<S> Ord for Front<S> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.front_item.key.key_bytes
            .cmp(&other.front_item.key.key_bytes)
            .reverse()
            .then_with(|| {
                self.front_item.value_cell.version
                    .cmp(&other.front_item.value_cell.version)
                    .reverse()
            })
    }
}

impl<S> PartialOrd for Front<S> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use alloc_pool::{
        bytes::{
            BytesMut,
        },
    };

    use crate::{
        kv,
        core::{
            merger::{
                Kont,
                KontScheduleIterAwait,
                KontAwaitScheduled,
                KontEmitItem,
                KontEmitDeprecated,
                ItersMergerCps,
            },
        },
    };

    #[test]
    fn merge_basic() {
        let mut iters = vec![
            vec![4, 8, 9],
            vec![3, 6],
            vec![0, 1, 2, 5],
            vec![],
            vec![7, 10, 11],
        ];

        let iters_merger = ItersMergerCps::new(&mut iters);
        let mut await_set = vec![];
        let mut kont = iters_merger.step();
        let mut output = vec![];
        loop {
            kont = match kont {
                Kont::ScheduleIterAwait(KontScheduleIterAwait {
                    await_iter,
                    next,
                }) => {
                    await_set.push(await_iter);
                    next.proceed()
                },
                Kont::AwaitScheduled(KontAwaitScheduled { next, }) => {
                    let mut await_iter = await_set.pop().unwrap();
                    if await_iter.is_empty() {
                        next.no_more()
                    } else {
                        let byte = await_iter.remove(0);
                        next.item_arrived(
                            await_iter,
                            kv::KeyValuePair {
                                key: kv::Key { key_bytes: BytesMut::new_detached(vec![byte]).freeze(), },
                                value_cell: kv::ValueCell { version: 1, cell: kv::Cell::Tombstone, },
                            },
                        )
                    }
                },
                Kont::EmitDeprecated(KontEmitDeprecated { .. }) => {
                    unreachable!();
                },
                Kont::EmitItem(KontEmitItem {
                    item,
                    next,
                }) => {
                    output.push(item.key.key_bytes[0]);
                    next.proceed()
                },
                Kont::Finished =>
                    break,
            };
        }

        assert_eq!(output, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    }

    #[test]
    fn merge_deprecated_different_iters() {
        let mut iters = vec![
            vec![(0, 0), (1, 3), (2, 0)],
            vec![(0, 1), (1, 1), (2, 1)],
            vec![(1, 0), (1, 2), (1, 4), (3, 0)],
            vec![],
            vec![(0, 2)],
        ];
        let (output, deprecated) = perform_merge(&mut iters);

        assert_eq!(output, vec![(0, 2), (1, 4), (2, 1), (3, 0)]);
        assert_eq!(deprecated, vec![(0, 0), (0, 1), (1, 0), (1, 1), (1, 2), (1, 3), (2, 0)]);
    }

    #[test]
    fn merge_deprecated_same_iter() {
        let mut iters = vec![
            vec![(0, 0), (2, 0)],
            vec![(2, 1)],
            vec![(1, 0), (1, 1), (1, 2), (1, 3), (1, 4)],
            vec![],
        ];
        let (output, deprecated) = perform_merge(&mut iters);

        assert_eq!(output, vec![(0, 0), (1, 4), (2, 1)]);
        assert_eq!(deprecated, vec![(1, 0), (1, 1), (1, 2), (1, 3), (2, 0)]);
    }

    #[test]
    fn stress_10_derived_00() {
        let mut iters = vec![
            vec![(17556261672556393688, 1)],
            vec![(8093639385199525262, 3)],
            vec![(8093639385199525262, 0), (16107956337732637054, 1)],
            vec![(3144521344682756311, 7), (3144521344682756311, 8)],
            vec![(3144521344682756311, 0), (16010932765423118261, 3)],
            vec![(3144521344682756311, 6)],
            vec![(16122783848902096046, 2)],
            vec![(6956425666106740206, 5), (17556261672556393688, 2)],
            vec![(16748507097280151939, 0)],
            vec![(6991660086521729989, 0)],
            vec![(6956425666106740206, 1)],
            vec![(3144521344682756311, 4), (6956425666106740206, 4)],
            vec![(8093639385199525262, 1), (16010932765423118261, 0)],
            vec![(11486697784006332595, 1), (16010932765423118261, 4)],
            vec![(6956425666106740206, 3), (8093639385199525262, 4)],
            vec![(3144521344682756311, 2), (16010932765423118261, 5)],
            vec![(3144521344682756311, 1)],
            vec![(6991660086521729989, 2)],
            vec![(16010932765423118261, 7)],
            vec![(8093639385199525262, 6), (16010932765423118261, 6)],
            vec![(17556261672556393688, 0)],
            vec![(16122783848902096046, 0)],
            vec![(3144521344682756311, 5), (6956425666106740206, 0)],
            vec![(8093639385199525262, 5)],
            vec![(3144521344682756311, 9)],
            vec![(11486697784006332595, 0)],
            vec![(16010932765423118261, 8)],
            vec![(6956425666106740206, 2)],
            vec![(3144521344682756311, 3), (11486697784006332595, 2)],
            vec![(16010932765423118261, 1)],
            vec![(16107956337732637054, 0)],
            vec![(8093639385199525262, 2), (16010932765423118261, 2)],
            vec![(11486697784006332595, 3)],
            vec![(6991660086521729989, 1), (16122783848902096046, 1)],
        ];
        let sample_output = vec![
            (3144521344682756311, 9),
            (6956425666106740206, 5),
            (6991660086521729989, 2),
            (8093639385199525262, 6),
            (11486697784006332595, 3),
            (16010932765423118261, 8),
            (16107956337732637054, 1),
            (16122783848902096046, 2),
            (16748507097280151939, 0),
            (17556261672556393688, 2),
        ];
        let sample_deprecated = vec![
            (3144521344682756311, 0),
            (3144521344682756311, 1),
            (3144521344682756311, 2),
            (3144521344682756311, 3),
            (3144521344682756311, 4),
            (3144521344682756311, 5),
            (3144521344682756311, 6),
            (3144521344682756311, 7),
            (3144521344682756311, 8),
            (6956425666106740206, 0),
            (6956425666106740206, 1),
            (6956425666106740206, 2),
            (6956425666106740206, 3),
            (6956425666106740206, 4),
            (6991660086521729989, 0),
            (6991660086521729989, 1),
            (8093639385199525262, 0),
            (8093639385199525262, 1),
            (8093639385199525262, 2),
            (8093639385199525262, 3),
            (8093639385199525262, 4),
            (8093639385199525262, 5),
            (11486697784006332595, 0),
            (11486697784006332595, 1),
            (11486697784006332595, 2),
            (16010932765423118261, 0),
            (16010932765423118261, 1),
            (16010932765423118261, 2),
            (16010932765423118261, 3),
            (16010932765423118261, 4),
            (16010932765423118261, 5),
            (16010932765423118261, 6),
            (16010932765423118261, 7),
            (16107956337732637054, 0),
            (16122783848902096046, 0),
            (16122783848902096046, 1),
            (17556261672556393688, 0),
            (17556261672556393688, 1),
        ];

        let (output, deprecated) = perform_merge(&mut iters);

        assert_eq!(output, sample_output);
        assert_eq!(deprecated, sample_deprecated);
    }

    #[test]
    fn stress() {
        use std::collections::{
            HashMap,
        };

        use rand::{
            Rng,
        };

        let total_items = 32764;
        let mut items = Vec::with_capacity(total_items);
        let mut rng = rand::thread_rng();
        for _ in 0 .. total_items {
            let item: u64 = rng.gen();
            let versions_count = rng.gen_range(1 ..= 10);
            for version in 0 .. versions_count {
                items.push((item, version));
            }
        }

        let mut sample_output_map = HashMap::new();
        let mut sample_deprecated = Vec::new();

        let mut iters = Vec::new();
        while !items.is_empty() {
            let lo = total_items / 100;
            let hi = total_items / 3;
            let iter_items_count = rng.gen_range(lo .. hi);
            let mut iter = Vec::with_capacity(iter_items_count);
            for _ in 0 .. iter_items_count {
                if items.is_empty() {
                    break;
                }
                let index = rng.gen_range(0 .. items.len());
                let item = items.swap_remove(index);
                iter.push(item);

                match sample_output_map.get(&item.0) {
                    None => {
                        sample_output_map.insert(item.0, item.1);
                    },
                    Some(&version) if version < item.1 => {
                        sample_deprecated.push((item.0, version));
                        sample_output_map.insert(item.0, item.1);
                    },
                    Some(..) => {
                        sample_deprecated.push(item);
                    },
                }
            }
            iter.sort();
            iters.push(iter);
        }

        let mut sample_output: Vec<_> = sample_output_map.into_iter().collect();
        sample_output.sort();
        sample_deprecated.sort();

        let (output, deprecated) = perform_merge(&mut iters);

        assert_eq!(output, sample_output);
        assert_eq!(deprecated, sample_deprecated);
    }

    #[allow(clippy::type_complexity)]
    fn perform_merge(iters: &mut Vec<Vec<(u64, u64)>>) -> (Vec<(u64, u64)>, Vec<(u64, u64)>) {
        let iters_merger = ItersMergerCps::new(iters);
        let mut await_set = vec![];
        let mut kont = iters_merger.step();
        let mut output = vec![];
        let mut deprecated = vec![];
        loop {
            kont = match kont {
                Kont::ScheduleIterAwait(KontScheduleIterAwait {
                    await_iter,
                    next,
                }) => {
                    await_set.push(await_iter);
                    next.proceed()
                },
                Kont::AwaitScheduled(KontAwaitScheduled { next, }) => {
                    let mut await_iter = await_set.pop().unwrap();
                    if await_iter.is_empty() {
                        next.no_more()
                    } else {
                        let (key_u64, version) = await_iter.remove(0);
                        next.item_arrived(
                            await_iter,
                            kv::KeyValuePair {
                                key: kv::Key { key_bytes: BytesMut::new_detached(key_u64.to_be_bytes().to_vec()).freeze(), },
                                value_cell: kv::ValueCell { version, cell: kv::Cell::Tombstone, },
                            },
                        )
                    }
                },
                Kont::EmitDeprecated(KontEmitDeprecated {
                    item,
                    next,
                }) => {
                    deprecated.push((u64::from_be_bytes(item.key.key_bytes.to_vec().try_into().unwrap()), item.value_cell.version));
                    next.proceed()
                },
                Kont::EmitItem(KontEmitItem {
                    item,
                    next,
                }) => {
                    output.push((u64::from_be_bytes(item.key.key_bytes.to_vec().try_into().unwrap()), item.value_cell.version));
                    next.proceed()
                },
                Kont::Finished =>
                    break,
            };
        }

        deprecated.sort();
        (output, deprecated)
    }
}
