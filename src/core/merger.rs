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
    advance_next_idx: Option<usize>,
}

impl<S> KeyValuesIter<S> {
    pub fn new(stream: S) -> KeyValuesIter<S> {
        KeyValuesIter {
            stream,
            iter_state: IterState::NotReady,
            advance_next_idx: None,
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
    Iter {
        cursor_idx: usize,
        advance_head_idx: Option<usize>,
    },
    Flush {
        advance_head_idx: Option<usize>,
        best_item: Option<kv::KeyValuePair<storage::OwnedValueBlockRef>>,
    },
}

impl<V> Inner<V> {
    fn new(iters: V) -> Self {
        Self {
            state: State::Iter { cursor_idx: 0, advance_head_idx: None, },
            iters,
        }
    }
}

impl<V, S> Inner<V> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    fn step(mut self) -> Kont<V, S> {
        loop {
            match self.state {

                State::Iter { mut cursor_idx, mut advance_head_idx, } => {
                    while cursor_idx < self.iters.len() {
                        match self.iters[cursor_idx].iter_state {
                            IterState::NotReady =>
                                return Kont::IterAwait {
                                    next: KontIterAwaitNext {
                                        cursor_idx,
                                        advance_head_idx,
                                        iters: self.iters,
                                    },
                                },
                            IterState::FrontItem(ref front_item) => {
                                match advance_head_idx {
                                    None => {
                                        self.iters[cursor_idx].advance_next_idx = None;
                                        advance_head_idx = Some(cursor_idx);
                                    },
                                    Some(head_idx) =>
                                        match (&self.iters[head_idx], front_item) {
                                            (
                                                KeyValuesIter { iter_state: IterState::FrontItem(kv::KeyValuePair { key: key_min, .. }), .. },
                                                kv::KeyValuePair { key: key_cur, .. },
                                            ) =>
                                                match key_cur.key_bytes.cmp(&key_min.key_bytes) {
                                                    Ordering::Less => {
                                                        self.iters[cursor_idx].advance_next_idx = None;
                                                        advance_head_idx = Some(cursor_idx);
                                                    },
                                                    Ordering::Equal => {
                                                        self.iters[cursor_idx].advance_next_idx = Some(head_idx);
                                                        advance_head_idx = Some(cursor_idx);
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

                    self.state = State::Flush {
                        advance_head_idx,
                        best_item: None,
                    };
                },

                State::Flush { best_item: Some(item), advance_head_idx: None, } =>
                    return Kont::Item {
                        best_item: item,
                        next: KontItemNext {
                            state: State::Iter {
                                cursor_idx: 0,
                                advance_head_idx: None,
                            },
                            iters: self.iters,
                        },
                    },

                State::Flush { best_item: None, advance_head_idx: None, } =>
                    return Kont::Done,

                State::Flush { mut best_item, advance_head_idx: Some(advance_head_idx), } => {
                    let current_iter = &mut self.iters[advance_head_idx];
                    match mem::replace(&mut current_iter.iter_state, IterState::NotReady) {
                        IterState::NotReady =>
                            unreachable!(),
                        IterState::FrontItem(front_item) =>
                            match best_item {
                                None =>
                                    best_item = Some(front_item),
                                Some(prev_best) =>
                                    return if prev_best.value_cell.version < front_item.value_cell.version {
                                        Kont::Deprecated {
                                            item: prev_best,
                                            next: KontDeprecatedNext {
                                                state: State::Flush {
                                                    best_item: Some(front_item),
                                                    advance_head_idx: current_iter.advance_next_idx,
                                                },
                                                iters: self.iters,
                                            },
                                        }
                                    } else {
                                        Kont::Deprecated {
                                            item: front_item,
                                            next: KontDeprecatedNext {
                                                state: State::Flush {
                                                    best_item: Some(prev_best),
                                                    advance_head_idx: current_iter.advance_next_idx,
                                                },
                                                iters: self.iters,
                                            },
                                        }
                                    },
                            },
                    }
                    self.state = State::Flush {
                        best_item,
                        advance_head_idx: current_iter.advance_next_idx,
                    };
                },

            }
        }
    }

}

pub struct KontIterAwaitNext<V, S> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    cursor_idx: usize,
    iters: V,
    advance_head_idx: Option<usize>,
}

impl<V, S> KontIterAwaitNext<V, S> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    pub fn current_iter(&mut self) -> &mut KeyValuesIter<S> {
        &mut self.iters[self.cursor_idx]
    }

    pub fn proceed_with_item(mut self, item: KeyValueRef) -> Kont<V, S> {
        match item {
            KeyValueRef::NoMore => {
                self.iters.swap_remove(self.cursor_idx);
                let inner = Inner {
                    state: State::Iter {
                        cursor_idx: self.cursor_idx,
                        advance_head_idx: self.advance_head_idx,
                    },
                    iters: self.iters,
                };
                inner.step()
            },
            KeyValueRef::BlockFinish(..) =>
                Kont::IterAwait {
                    next: KontIterAwaitNext {
                        cursor_idx: self.cursor_idx,
                        iters: self.iters,
                        advance_head_idx: self.advance_head_idx,
                    },
                },
            KeyValueRef::Item { key, value_cell, } => {
                self.current_iter().iter_state =
                    IterState::FrontItem(kv::KeyValuePair { key, value_cell, });
                let inner = Inner {
                    state: State::Iter {
                        cursor_idx: self.cursor_idx,
                        advance_head_idx: self.advance_head_idx,
                    },
                    iters: self.iters,
                };
                inner.step()
           },
        }
    }
}

pub struct KontDeprecatedNext<V, S> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    state: State,
    iters: V,
}

impl<V, S> KontDeprecatedNext<V, S> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    pub fn proceed(self) -> Kont<V, S> {
        let inner = Inner {
            state: self.state,
            iters: self.iters,
        };
        inner.step()
    }
}

pub struct KontItemNext<V, S> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    state: State,
    iters: V,
}

impl<V, S> KontItemNext<V, S> where V: DerefMut<Target = Vec<KeyValuesIter<S>>> {
    pub fn proceed(self) -> Kont<V, S> {
        let inner = Inner {
            state: self.state,
            iters: self.iters,
        };
        inner.step()
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
}
