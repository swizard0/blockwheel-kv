use std::{
    mem,
    ops::DerefMut,
    cmp::Ordering,
};

use futures::{
    channel::{
        mpsc,
    },
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
            kont: Inner::new(iters)
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
        loop {
            match mem::replace(&mut self.kont, Kont::Done) {
                Kont::IterAwait { mut next, } =>
                    match next.current_iter().stream.next().await {
                        None =>
                            return Err(Error::BackendIterPeerLost),
                        Some(key_value_ref) =>
                            self.kont = next.proceed_with_item(key_value_ref),
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

    pub fn run(self) -> Kont<V, mpsc::Receiver<KeyValueRef>> {
        self.kont
    }
}

struct Inner<V> {
    state: State,
    iters: V,
}

enum IterState {
    NotReady,
    FrontItem(kv::KeyValuePair<storage::OwnedValueBlockRef>),
}

pub enum Kont<V, S> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    IterAwait {
        next: KontIterAwaitNext<V, S>,
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

enum State {
    Iter(StateIter),
}

#[derive(Default)]
struct StateIter  {
    cursor_idx: usize,
    best_item_idx: Option<usize>,
    deprecated: bool,
}

impl<V> Inner<V> {
    fn new(iters: V) -> Self {
        Self {
            state: State::Iter(StateIter::default()),
            iters,
        }
    }
}

impl<V, S> Inner<V> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    fn step(mut self) -> Kont<V, S> {
        loop {
            match self.state {

                State::Iter(StateIter { mut cursor_idx, mut best_item_idx, mut deprecated, }) => {
                    while cursor_idx < self.iters.len() {
                        match self.iters[cursor_idx].iter_state {
                            IterState::NotReady =>
                                return Kont::IterAwait {
                                    next: KontIterAwaitNext {
                                        state_iter: StateIter {
                                            cursor_idx,
                                            best_item_idx,
                                            deprecated,
                                        },
                                        iters: self.iters,
                                    },
                                },
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
                                            state_iter: StateIter::default(),
                                            iters: self.iters,
                                        },
                                    },
                                IterState::FrontItem(best_item) =>
                                    Kont::Item {
                                        best_item,
                                        next: KontItemNext {
                                            state_iter: StateIter::default(),
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

pub struct KontIterAwaitNext<V, S> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    state_iter: StateIter,
    iters: V,
}

impl<V, S> From<KontIterAwaitNext<V, S>> for Inner<V> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    fn from(kont: KontIterAwaitNext<V, S>) -> Inner<V> {
        Inner {
            state: State::Iter(kont.state_iter),
            iters: kont.iters,
        }
    }
}

impl<V, S> KontIterAwaitNext<V, S> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    pub fn current_iter(&mut self) -> &mut KeyValuesIter<S> {
        &mut self.iters[self.state_iter.cursor_idx]
    }

    pub fn proceed_with_item(mut self, item: KeyValueRef) -> Kont<V, S> {
        match item {
            KeyValueRef::NoMore => {
                self.iters.swap_remove(self.state_iter.cursor_idx);
                Inner::from(self).step()
            },
            KeyValueRef::BlockFinish(..) =>
                Kont::IterAwait {
                    next: KontIterAwaitNext { ..self },
                },
            KeyValueRef::Item { key, value_cell, } => {
                self.current_iter().iter_state =
                    IterState::FrontItem(kv::KeyValuePair { key, value_cell, });
                Inner::from(self).step()
           },
        }
    }
}

pub struct KontDeprecatedNext<V, S> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    state_iter: StateIter,
    iters: V,
}

impl<V, S> From<KontDeprecatedNext<V, S>> for Inner<V> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    fn from(kont: KontDeprecatedNext<V, S>) -> Inner<V> {
        Inner {
            state: State::Iter(kont.state_iter),
            iters: kont.iters,
        }
    }
}

impl<V, S> KontDeprecatedNext<V, S> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    pub fn proceed(self) -> Kont<V, S> {
        Inner::from(self).step()
    }
}

pub struct KontItemNext<V, S> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    state_iter: StateIter,
    iters: V,
}

impl<V, S> From<KontItemNext<V, S>> for Inner<V> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    fn from(kont: KontItemNext<V, S>) -> Inner<V> {
        Inner {
            state: State::Iter(kont.state_iter),
            iters: kont.iters,
        }
    }
}

impl<V, S> KontItemNext<V, S> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    pub fn proceed(self) -> Kont<V, S> {
        Inner::from(self).step()
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
        Inner,
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

        let iters_merger = Inner::new(&mut iters);
        let mut kont = iters_merger.step();
        let mut output = vec![];
        loop {
            kont = match kont {
                Kont::IterAwait { mut next, } => {
                    let item = if next.current_iter().stream.is_empty() {
                        KeyValueRef::NoMore
                    } else {
                        let byte = next.current_iter().stream.remove(0);
                        KeyValueRef::Item {
                            key: kv::Key { key_bytes: BytesMut::new_detached(vec![byte]).freeze(), },
                            value_cell: kv::ValueCell { version: 1, cell: kv::Cell::Tombstone, },
                        }
                    };
                    next.proceed_with_item(item)
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
    fn merge_deprecated() {
        let mut iters = vec![
            KeyValuesIter::new(vec![(0, 0), (1, 3), (2, 0)]),
            KeyValuesIter::new(vec![(0, 1), (1, 1), (2, 1)]),
            KeyValuesIter::new(vec![(1, 0), (1, 2), (1, 4), (3, 0)]),
            KeyValuesIter::new(vec![]),
            KeyValuesIter::new(vec![(0, 2)]),
        ];

        let iters_merger = Inner::new(&mut iters);
        let mut kont = iters_merger.step();
        let mut output = vec![];
        let mut deprecated = vec![];
        loop {
            kont = match kont {
                Kont::IterAwait { mut next, } => {
                    let item = if next.current_iter().stream.is_empty() {
                        KeyValueRef::NoMore
                    } else {
                        let (byte, version) = next.current_iter().stream.remove(0);
                        KeyValueRef::Item {
                            key: kv::Key { key_bytes: BytesMut::new_detached(vec![byte]).freeze(), },
                            value_cell: kv::ValueCell { version: version.try_into().unwrap(), cell: kv::Cell::Tombstone, },
                        }
                    };
                    next.proceed_with_item(item)
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
}
