use alloc_pool::{
    bytes::{
        BytesMut,
    },
};

use crate::{
    kv,
    version,
    core::{
        performer::{
            Params,
            Performer,
            SearchForest,
            Kont,
            KontPoll,
            KontInserted,
            KontFlushButcher,
        },
    },
};

fn key(v: isize) -> kv::Key {
    kv::Key::from(BytesMut::new_detached(format!("i am a key {}", v).into()))
}

fn value(v: isize) -> kv::Value {
    kv::Value::from(BytesMut::new_detached(format!("i am a value {}", v).into()))
}

#[test]
fn basic_insert() {
    let version_provider =
        version::Provider::from_unix_epoch_seed();
    let performer = Performer::new(
        Params { butcher_block_size: 2, ..Default::default() },
        version_provider,
        SearchForest::new(),
    );
    let kont = performer.step();
    let next = match kont {
        Kont::Poll(KontPoll { next, }) =>
            next,
        _ =>
            panic!("expected Kont::Poll, got other"),
    };
    // database: 0 entries
    let kont = next.incoming_insert(key(1), value(1));
    let next = match kont {
        Kont::Inserted(KontInserted { next, .. }) =>
            next,
        _ =>
            panic!("expected Kont::Inserted, got other"),
    };
    let kont = next.got_it();
    let next = match kont {
        Kont::Poll(KontPoll { next, }) =>
            next,
        _ =>
            panic!("expected Kont::Poll, got other"),
    };
    // database: 1 entries
    let kont = next.incoming_insert(key(2), value(2));
    let next = match kont {
        Kont::Inserted(KontInserted { next, .. }) =>
            next,
        _ =>
            panic!("expected Kont::Inserted, got other"),
    };
    // should request flush (max 2 entries limit)
    let kont = next.got_it();
    let next = match kont {
        Kont::FlushButcher(KontFlushButcher { search_tree_ref, frozen_memcache, next, }) =>
            next,
        _ =>
            panic!("expected Kont::FlushButcher, got other"),
    };
    // database: 2 entries with flush requested
    let kont = next.scheduled();
    let next = match kont {
        Kont::Poll(KontPoll { next, }) =>
            next,
        _ =>
            panic!("expected Kont::Poll, got other"),
    };
    // database: 2 entries with flush scheduled
    let kont = next.incoming_insert(key(3), value(3));
    let next = match kont {
        Kont::Inserted(KontInserted { next, .. }) =>
            next,
        _ =>
            panic!("expected Kont::Inserted, got other"),
    };
    // database: 3 entries with flush scheduled
    let kont = next.got_it();
    let next = match kont {
        Kont::Poll(KontPoll { next, }) =>
            next,
        _ =>
            panic!("expected Kont::Poll, got other"),
    };
}
