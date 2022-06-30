use std::{
    mem,
    ops::{
        Deref,
        DerefMut,
    },
    cmp::{
        Ordering,
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

pub struct KeyValuesIter<S> {
    stream: S,
    iter_state: IterState,
}

impl<S> KeyValuesIter<S> {
    pub fn new(stream: S) -> KeyValuesIter<S> {
        KeyValuesIter {
            stream,
            iter_state: IterState::NotReady,
        }
    }

    pub fn stream(&mut self) -> &mut S {
        &mut self.stream
    }
}

pub type KeyValuesIterRx = KeyValuesIter<mpsc::Receiver<KeyValueRef>>;

pub struct ItersMerger<V> where V: DerefMut<Target = Vec<KeyValuesIterRx>> {
    kont: Kont<V, mpsc::Receiver<KeyValueRef>>,
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
            async fn make_future(
                mut await_iter: AwaitIter<mpsc::Receiver<KeyValueRef>>
            )
                -> (Option<KeyValueRef>, AwaitIter<mpsc::Receiver<KeyValueRef>>)
            {
                let maybe_item = await_iter.stream().next().await;
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
                                result = await_iter_a.stream().next() => {
                                    await_set = AwaitSet::One(await_iter_b);
                                    (result, await_iter_a)
                                },
                                result = await_iter_b.stream().next() => {
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
                Kont::Item { best_item, next, } => {
                    self.kont = next.proceed();
                    return Ok(Some(best_item));
                },
                Kont::Done =>
                    return Ok(None),
            }
        }
    }
}

pub struct ItersMergerCps<V> {
    state: State,
    iters: V,
}

enum IterState {
    NotReady,
    FrontItem(kv::KeyValuePair<storage::OwnedValueBlockRef>),
}

pub enum Kont<V, S> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    ScheduleIterAwait {
        await_iter: AwaitIter<S>,
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
        best_item: kv::KeyValuePair<storage::OwnedValueBlockRef>,
        next: KontItemNext<V, S>,
    },
    Done,
}

pub struct AwaitIter<S> {
    iter: KeyValuesIter<S>,
}

enum State {
    Await(StateAwait),
    Iter(StateIter),
}

#[derive(Default)]
struct StateAwait {
    cursor_idx: usize,
    pending_count: usize,
}

#[derive(Default)]
struct StateIter {
    cursor_idx: usize,
    best_item_idx: Option<usize>,
    deprecated: bool,
}

impl<V> ItersMergerCps<V> {
    pub fn new(iters: V) -> Self {
        Self {
            state: State::Await(StateAwait::default()),
            iters,
        }
    }
}

impl<V, S> ItersMergerCps<V> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    pub fn step(mut self) -> Kont<V, S> {
        loop {
            match self.state {

                State::Await(StateAwait { mut cursor_idx, pending_count, }) => {
                    while cursor_idx < self.iters.len() {
                        match &self.iters[cursor_idx].iter_state {
                            IterState::NotReady => {
                                let iter = self.iters.swap_remove(cursor_idx);
                                return Kont::ScheduleIterAwait {
                                    await_iter: AwaitIter { iter, },
                                    next: KontScheduleIterAwaitNext {
                                        state_await: StateAwait {
                                            cursor_idx,
                                            pending_count: pending_count + 1,
                                        },
                                        iters: self.iters,
                                    },
                                }
                            },
                            IterState::FrontItem(..) =>
                                cursor_idx += 1,
                        }
                    }

                    if pending_count == 0 {
                        self.state = State::Iter(StateIter::default());
                    } else {
                        return Kont::AwaitScheduled {
                            next: KontAwaitScheduledNext {
                                state_await: StateAwait { cursor_idx, pending_count, },
                                iters: self.iters,
                            },
                        };
                    }
                },

                State::Iter(StateIter { mut cursor_idx, mut best_item_idx, mut deprecated, }) => {
                    while cursor_idx < self.iters.len() {
                        match self.iters[cursor_idx].iter_state {
                            IterState::NotReady =>
                                unreachable!(),
                            IterState::FrontItem(ref front_item) => {
                                match best_item_idx {
                                    None => {
                                        assert!(!deprecated);
                                        best_item_idx = Some(cursor_idx);
                                    },
                                    Some(best_idx) =>
                                        match (&self.iters[best_idx], front_item) {
                                            (
                                                KeyValuesIter {
                                                    iter_state: IterState::FrontItem(
                                                        kv::KeyValuePair {
                                                            key: key_best,
                                                            value_cell: kv::ValueCell { version: version_best, .. },
                                                        }
                                                    ),
                                                    ..
                                                },
                                                kv::KeyValuePair {
                                                    key: key_cur,
                                                    value_cell: kv::ValueCell { version: version_cur, .. },
                                                },
                                            ) =>
                                                match key_cur.key_bytes.cmp(&key_best.key_bytes) {
                                                    Ordering::Less => {
                                                        best_item_idx = Some(cursor_idx);
                                                        deprecated = false;
                                                    },
                                                    Ordering::Equal => {
                                                        deprecated = true;
                                                        if version_cur < version_best {
                                                            best_item_idx = Some(cursor_idx);
                                                        }
                                                    },
                                                    Ordering::Greater =>
                                                        (),
                                                },
                                            (KeyValuesIter { iter_state: IterState::NotReady, .. }, _) =>
                                                unreachable!(),
                                        },
                                }
                                cursor_idx += 1;
                            },
                        }
                    }

                    match best_item_idx {
                        None =>
                            return Kont::Done,
                        Some(best_idx) =>
                            return match mem::replace(&mut self.iters[best_idx].iter_state, IterState::NotReady) {
                                IterState::NotReady =>
                                    unreachable!(),
                                IterState::FrontItem(best_item) if deprecated =>
                                    Kont::Deprecated {
                                        item: best_item,
                                        next: KontDeprecatedNext {
                                            state_await: StateAwait::default(),
                                            iters: self.iters,
                                        },
                                    },
                                IterState::FrontItem(best_item) =>
                                    Kont::Item {
                                        best_item,
                                        next: KontItemNext {
                                            state_await: StateAwait::default(),
                                            iters: self.iters,
                                        },
                                    },
                            },
                    }
                },

            }
        }
    }

}

impl<S> Deref for AwaitIter<S> {
    type Target = KeyValuesIter<S>;

    fn deref(&self) -> &Self::Target {
        &self.iter
    }
}

impl<S> DerefMut for AwaitIter<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.iter
    }
}

// impl<S> From<KeyValuesIter<S>> for AwaitIter<S> {
//     fn from(iter: KeyValuesIter<S>) -> Self {
//         Self { iter, }
//     }
// }

// impl<S> From<AwaitIter<S>> for KeyValuesIter<S> {
//     fn from(await_iter: AwaitIter<S>) -> Self {
//         await_iter.iter
//     }
// }

pub struct KontScheduleIterAwaitNext<V, S> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    state_await: StateAwait,
    iters: V,
}

impl<V, S> From<KontScheduleIterAwaitNext<V, S>> for ItersMergerCps<V> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    fn from(kont: KontScheduleIterAwaitNext<V, S>) -> ItersMergerCps<V> {
        ItersMergerCps {
            state: State::Await(kont.state_await),
            iters: kont.iters,
        }
    }
}

impl<V, S> KontScheduleIterAwaitNext<V, S> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    pub fn proceed(self) -> Kont<V, S> {
        ItersMergerCps::from(self).step()
    }

}

pub struct KontAwaitScheduledNext<V, S> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    state_await: StateAwait,
    iters: V,
}

impl<V, S> From<KontAwaitScheduledNext<V, S>> for ItersMergerCps<V> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    fn from(kont: KontAwaitScheduledNext<V, S>) -> ItersMergerCps<V> {
        ItersMergerCps {
            state: State::Await(kont.state_await),
            iters: kont.iters,
        }
    }
}

impl<V, S> KontAwaitScheduledNext<V, S> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    pub fn proceed_with_item(mut self, mut await_iter: AwaitIter<S>, item: KeyValueRef) -> Kont<V, S> {
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
                        iters: self.iters,
                    },
                },
            KeyValueRef::Item { key, value_cell, } => {
                await_iter.iter.iter_state =
                    IterState::FrontItem(kv::KeyValuePair { key, value_cell, });
                self.iters.push(await_iter.iter);
                self.state_await.pending_count -= 1;
                ItersMergerCps::from(self).step()
           },
        }
    }
}

pub struct KontDeprecatedNext<V, S> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    state_await: StateAwait,
    iters: V,
}

impl<V, S> From<KontDeprecatedNext<V, S>> for ItersMergerCps<V> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    fn from(kont: KontDeprecatedNext<V, S>) -> ItersMergerCps<V> {
        ItersMergerCps {
            state: State::Await(kont.state_await),
            iters: kont.iters,
        }
    }
}

impl<V, S> KontDeprecatedNext<V, S> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    pub fn proceed(self) -> Kont<V, S> {
        ItersMergerCps::from(self).step()
    }
}

pub struct KontItemNext<V, S> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    state_await: StateAwait,
    iters: V,
}

impl<V, S> From<KontItemNext<V, S>> for ItersMergerCps<V> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    fn from(kont: KontItemNext<V, S>) -> ItersMergerCps<V> {
        ItersMergerCps {
            state: State::Await(kont.state_await),
            iters: kont.iters,
        }
    }
}

impl<V, S> KontItemNext<V, S> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    pub fn proceed(self) -> Kont<V, S> {
        ItersMergerCps::from(self).step()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        kv,
        core::{
            KeyValueRef,
        },
    };

    use alloc_pool::{
        bytes::{
            BytesMut,
        },
    };

    use super::{
        Kont,
        ItersMergerCps,
        KeyValuesIter,
    };

    #[test]
    fn merge_basic() {
        let mut iters = vec![
            KeyValuesIter::new(vec![4, 8, 9]),
            KeyValuesIter::new(vec![3, 6]),
            KeyValuesIter::new(vec![0, 1, 2, 5]),
            KeyValuesIter::new(vec![]),
            KeyValuesIter::new(vec![7, 10, 11]),
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
                    let item = if await_iter.stream().is_empty() {
                        KeyValueRef::NoMore
                    } else {
                        let byte = await_iter.stream().remove(0);
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
                Kont::Item { best_item, next, } => {
                    output.push(best_item.key.key_bytes[0]);
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
            KeyValuesIter::new(vec![(0, 0), (1, 3), (2, 0)]),
            KeyValuesIter::new(vec![(0, 1), (1, 1), (2, 1)]),
            KeyValuesIter::new(vec![(1, 0), (1, 2), (1, 4), (3, 0)]),
            KeyValuesIter::new(vec![]),
            KeyValuesIter::new(vec![(0, 2)]),
        ];

        let iters_merger = ItersMergerCps::new(&mut iters);
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
                    let item = if await_iter.stream().is_empty() {
                        KeyValueRef::NoMore
                    } else {
                        let (byte, version) = await_iter.stream().remove(0);
                        KeyValueRef::Item {
                            key: kv::Key { key_bytes: BytesMut::new_detached(vec![byte]).freeze(), },
                            value_cell: kv::ValueCell { version: version.try_into().unwrap(), cell: kv::Cell::Tombstone, },
                        }
                    };
                    next.proceed_with_item(await_iter, item)
                },
                Kont::Deprecated { item, next, } => {
                    deprecated.push((item.key.key_bytes[0], item.value_cell.version));
                    next.proceed()
                },
                Kont::Item { best_item, next, } => {
                    output.push((best_item.key.key_bytes[0], best_item.value_cell.version));
                    next.proceed()
                },
                Kont::Done =>
                    break,
            };
        }

        assert_eq!(output, vec![(0, 2), (1, 4), (2, 1), (3, 0)]);
        assert_eq!(deprecated, vec![(0, 0), (0, 1), (1, 0), (1, 1), (1, 2), (1, 3), (2, 0)]);
    }

    #[test]
    fn merge_deprecated_same_iter() {
        let mut iters = vec![
            KeyValuesIter::new(vec![(0, 0), (2, 0)]),
            KeyValuesIter::new(vec![(2, 1)]),
            KeyValuesIter::new(vec![(1, 0), (1, 1), (1, 2), (1, 3), (1, 4)]),
            KeyValuesIter::new(vec![]),
        ];

        let iters_merger = ItersMergerCps::new(&mut iters);
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
                    let item = if await_iter.stream().is_empty() {
                        KeyValueRef::NoMore
                    } else {
                        let (byte, version) = await_iter.stream().remove(0);
                        KeyValueRef::Item {
                            key: kv::Key { key_bytes: BytesMut::new_detached(vec![byte]).freeze(), },
                            value_cell: kv::ValueCell { version: version.try_into().unwrap(), cell: kv::Cell::Tombstone, },
                        }
                    };
                    next.proceed_with_item(await_iter, item)
                },
                Kont::Deprecated { item, next, } => {
                    deprecated.push((item.key.key_bytes[0], item.value_cell.version));
                    next.proceed()
                },
                Kont::Item { best_item, next, } => {
                    output.push((best_item.key.key_bytes[0], best_item.value_cell.version));
                    next.proceed()
                },
                Kont::Done =>
                    break,
            };
        }

        assert_eq!(output, vec![(0, 0), (1, 4), (2, 1)]);
        assert_eq!(deprecated, vec![(1, 0), (1, 1), (1, 2), (1, 3), (2, 0)]);
    }
}
