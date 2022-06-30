use std::{
    mem,
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

use futures::{
    channel::{
        mpsc,
    },
    stream::{
        FuturesUnordered,
    },
    select,
    StreamExt,
};

use crate::{
    kv,
    storage,
    core::{
        KeyValueRef,
    },
};

pub type KeyValuesIterRx = mpsc::Receiver<KeyValueRef>;

pub struct ItersMerger<V> where V: DerefMut<Target = Vec<KeyValuesIterRx>> {
    kont: Kont<V, KeyValuesIterRx>,
}

#[derive(Debug)]
pub enum Error {
    BackendIterPeerLost,
}

impl<V> ItersMerger<V> where V: DerefMut<Target = Vec<KeyValuesIterRx>> {
    pub fn new(iters: V) -> ItersMerger<V> {
        ItersMerger {
            kont: ItersMergerCps::new(iters)
                .step(),
        }
    }
}

impl<V> ItersMerger<V> where V: DerefMut<Target = Vec<KeyValuesIterRx>> {
    pub async fn next(&mut self) -> Result<Option<kv::KeyValuePair<storage::OwnedValueBlockRef>>, Error> {
        self.next_with_deprecated(|_| ()).await
    }

    pub async fn next_with_deprecated<F>(
        &mut self,
        mut deprecated: F,
    )
        -> Result<Option<kv::KeyValuePair<storage::OwnedValueBlockRef>>, Error>
    where F: FnMut(kv::KeyValuePair<storage::OwnedValueBlockRef>)
    {
        enum AwaitSet<I> {
            Empty,
            One(I),
            Two(I, I),
            Set,
        }
        let mut await_set_futures = FuturesUnordered::new();
        let mut await_set = AwaitSet::Empty;

        loop {
            async fn make_future(mut await_iter: KeyValuesIterRx) -> (Option<KeyValueRef>, KeyValuesIterRx) {
                let maybe_item = await_iter.next().await;
                (maybe_item, await_iter)
            }

            match mem::replace(&mut self.kont, Kont::Done) {
                Kont::ScheduleIterAwait { await_iter, next, } => {
                    await_set = match await_set {
                        AwaitSet::Empty =>
                            AwaitSet::One(await_iter),
                        AwaitSet::One(prev_await_iter) =>
                            AwaitSet::Two(prev_await_iter, await_iter),
                        AwaitSet::Two(await_iter_a, await_iter_b) => {
                            assert!(await_set_futures.is_empty());
                            await_set_futures.push(make_future(await_iter_a));
                            await_set_futures.push(make_future(await_iter_b));
                            await_set_futures.push(make_future(await_iter));
                            AwaitSet::Set
                        },
                        AwaitSet::Set => {
                            assert!(!await_set_futures.is_empty());
                            await_set_futures.push(make_future(await_iter));
                            AwaitSet::Set
                        },
                    };
                    self.kont = next.proceed();
                },
                Kont::AwaitScheduled { next, } => {
                    let (maybe_item, await_iter) = match mem::replace(&mut await_set, AwaitSet::Empty) {
                        AwaitSet::Empty =>
                            unreachable!(),
                        AwaitSet::One(await_iter) => {
                            make_future(await_iter).await
                        },
                        AwaitSet::Two(mut await_iter_a, mut await_iter_b) =>
                            select! {
                                result = await_iter_a.next() => {
                                    await_set = AwaitSet::One(await_iter_b);
                                    (result, await_iter_a)
                                },
                                result = await_iter_b.next() => {
                                    await_set = AwaitSet::One(await_iter_a);
                                    (result, await_iter_b)
                                },
                            },
                        AwaitSet::Set => {
                            let result = await_set_futures.next().await.unwrap();
                            if !await_set_futures.is_empty() {
                                await_set = AwaitSet::Set;
                            }
                            result
                        },
                    };
                    match maybe_item {
                        None =>
                            return Err(Error::BackendIterPeerLost),
                        Some(key_value_ref) =>
                            self.kont = next.proceed_with_item(await_iter, key_value_ref),
                    }
                },
                Kont::Deprecated { item, next, } => {
                    deprecated(item);
                    self.kont = next.proceed();
                },
                Kont::Item { item, next, } => {
                    self.kont = next.proceed();
                    return Ok(Some(item));
                },
                Kont::Done =>
                    return Ok(None),
            }
        }
    }
}

pub struct ItersMergerCps<V, S> {
    state: State,
    env: Env<V, S>,
}

pub enum Kont<V, S> where V: DerefMut<Target = Vec<S>> {
    ScheduleIterAwait {
        await_iter: S,
        next: KontScheduleIterAwaitNext<V, S>,
    },
    AwaitScheduled {
        next: KontAwaitScheduledNext<V, S>,
    },
    Deprecated {
        item: kv::KeyValuePair<storage::OwnedValueBlockRef>,
        next: KontDeprecatedNext<V, S>,
    },
    Item {
        item: kv::KeyValuePair<storage::OwnedValueBlockRef>,
        next: KontItemNext<V, S>,
    },
    Done,
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

                State::Await(StateAwait { pending_count, }) =>
                    match self.env.iters.pop() {
                        Some(await_iter) =>
                            return Kont::ScheduleIterAwait {
                                await_iter,
                                next: KontScheduleIterAwaitNext {
                                    state_await: StateAwait {
                                        pending_count: pending_count + 1,
                                    },
                                    env: self.env,
                                },
                            },
                        None if pending_count == 0 =>
                            self.state = State::Flush,
                        None =>
                            return Kont::AwaitScheduled {
                                next: KontAwaitScheduledNext {
                                    state_await: StateAwait { pending_count, },
                                    env: self.env,
                                },
                            },
                    },

                State::Flush =>
                    match self.env.candidate.take() {
                        None =>
                            match self.env.fronts.pop() {
                                None if self.env.iters.is_empty() =>
                                    return Kont::Done,
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
                                    return Kont::Item {
                                        item: candidate_item,
                                        next: KontItemNext { env: self.env, },
                                    },
                                None => {
                                    self.env.candidate = Some(candidate_item);
                                    self.state = State::Await(StateAwait::default());
                                },
                                Some(Front { front_item, .. }) =>
                                    if candidate_item.key.key_bytes == front_item.key.key_bytes {
                                        let should_swap = front_item.value_cell.version < candidate_item.value_cell.version;
                                        let Front { front_item: next_front_item, iter, } =
                                            self.env.fronts.pop().unwrap();
                                        let (deprecated_item, next_candidate_item) = if should_swap {
                                            (next_front_item, candidate_item)
                                        } else {
                                            (candidate_item, next_front_item)
                                        };
                                        self.env.candidate = Some(next_candidate_item);
                                        self.env.iters.push(iter);
                                        return Kont::Deprecated {
                                            item: deprecated_item,
                                            next: KontDeprecatedNext { env: self.env, },
                                        };
                                    } else if self.env.iters.is_empty() {
                                        return Kont::Item {
                                            item: candidate_item,
                                            next: KontItemNext { env: self.env, },
                                        };
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

pub struct KontScheduleIterAwaitNext<V, S> where V: DerefMut<Target = Vec<S>> {
    state_await: StateAwait,
    env: Env<V, S>,
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

pub struct KontAwaitScheduledNext<V, S> where V: DerefMut<Target = Vec<S>> {
    state_await: StateAwait,
    env: Env<V, S>,
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
    pub fn proceed_with_item(mut self, await_iter: S, item: KeyValueRef) -> Kont<V, S> {
        assert!(self.state_await.pending_count > 0);
        match item {
            KeyValueRef::NoMore => {
                self.state_await.pending_count -= 1;
                ItersMergerCps::from(self).step()
            },
            KeyValueRef::BlockFinish(..) =>
                Kont::ScheduleIterAwait {
                    await_iter,
                    next: KontScheduleIterAwaitNext {
                        state_await: self.state_await,
                        env: self.env,
                    },
                },
            KeyValueRef::Item { key, value_cell, } => {
                self.env.fronts.push(Front {
                    front_item: kv::KeyValuePair { key, value_cell, },
                    iter: await_iter,
                });
                self.state_await.pending_count -= 1;
                ItersMergerCps::from(self).step()
           },
        }
    }
}

pub struct KontDeprecatedNext<V, S> where V: DerefMut<Target = Vec<S>> {
    env: Env<V, S>,
}

impl<V, S> From<KontDeprecatedNext<V, S>> for ItersMergerCps<V, S> where V: DerefMut<Target = Vec<S>> {
    fn from(kont: KontDeprecatedNext<V, S>) -> ItersMergerCps<V, S> {
        ItersMergerCps {
            state: State::Flush,
            env: kont.env,
        }
    }
}

impl<V, S> KontDeprecatedNext<V, S> where V: DerefMut<Target = Vec<S>> {
    pub fn proceed(self) -> Kont<V, S> {
        ItersMergerCps::from(self).step()
    }
}

pub struct KontItemNext<V, S> where V: DerefMut<Target = Vec<S>> {
    env: Env<V, S>,
}

impl<V, S> From<KontItemNext<V, S>> for ItersMergerCps<V, S> where V: DerefMut<Target = Vec<S>> {
    fn from(kont: KontItemNext<V, S>) -> ItersMergerCps<V, S> {
        ItersMergerCps {
            state: State::Await(StateAwait::default()),
            env: kont.env,
        }
    }
}

impl<V, S> KontItemNext<V, S> where V: DerefMut<Target = Vec<S>> {
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
            KeyValueRef,
            merger::{
                Kont,
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
                Kont::ScheduleIterAwait { await_iter, next, } => {
                    await_set.push(await_iter);
                    next.proceed()
                },
                Kont::AwaitScheduled { next, } => {
                    let mut await_iter = await_set.pop().unwrap();
                    let item = if await_iter.is_empty() {
                        KeyValueRef::NoMore
                    } else {
                        let byte = await_iter.remove(0);
                        KeyValueRef::Item {
                            key: kv::Key { key_bytes: BytesMut::new_detached(vec![byte]).freeze(), },
                            value_cell: kv::ValueCell { version: 1, cell: kv::Cell::Tombstone, },
                        }
                    };
                    next.proceed_with_item(await_iter, item)
                },
                Kont::Deprecated { .. } => {
                    unreachable!();
                },
                Kont::Item { item, next, } => {
                    output.push(item.key.key_bytes[0]);
                    next.proceed()
                },
                Kont::Done =>
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

        println!("iters = {iters:?}");
        println!("sample_output: {sample_output:?}");
        println!("sample_deprecated: {sample_deprecated:?}");

        let (output, deprecated) = perform_merge(&mut iters);

        assert_eq!(output, sample_output);
        assert_eq!(deprecated, sample_deprecated);
    }

    fn perform_merge(iters: &mut Vec<Vec<(u64, u64)>>) -> (Vec<(u64, u64)>, Vec<(u64, u64)>) {
        let iters_merger = ItersMergerCps::new(iters);
        let mut await_set = vec![];
        let mut kont = iters_merger.step();
        let mut output = vec![];
        let mut deprecated = vec![];
        loop {
            kont = match kont {
                Kont::ScheduleIterAwait { await_iter, next, } => {
                    await_set.push(await_iter);
                    next.proceed()
                },
                Kont::AwaitScheduled { next, } => {
                    let mut await_iter = await_set.pop().unwrap();
                    let item = if await_iter.is_empty() {
                        KeyValueRef::NoMore
                    } else {
                        let (key_u64, version) = await_iter.remove(0);
                        KeyValueRef::Item {
                            key: kv::Key { key_bytes: BytesMut::new_detached(key_u64.to_be_bytes().to_vec()).freeze(), },
                            value_cell: kv::ValueCell { version: version.try_into().unwrap(), cell: kv::Cell::Tombstone, },
                        }
                    };
                    next.proceed_with_item(await_iter, item)
                },
                Kont::Deprecated { item, next, } => {
                    deprecated.push((u64::from_be_bytes(item.key.key_bytes.to_vec().try_into().unwrap()), item.value_cell.version));
                    next.proceed()
                },
                Kont::Item { item, next, } => {
                    output.push((u64::from_be_bytes(item.key.key_bytes.to_vec().try_into().unwrap()), item.value_cell.version));
                    next.proceed()
                },
                Kont::Done =>
                    break,
            };
        }

        deprecated.sort();
        (output, deprecated)
    }
}
