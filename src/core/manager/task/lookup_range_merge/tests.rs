use std::{
    sync::{
        Arc,
        Mutex,
    },
    collections::{
        HashMap,
    },
};

use futures::{
    channel::{
        oneshot,
    },
    StreamExt,
};

use alloc_pool::{
    pool,
    bytes::{
        Bytes,
    },
    Unique,
};

use crate::{
    kv,
    job,
    storage,
    core::{
        manager::{
            task::{
                lookup_range_merge::{
                    inner_run,
                },
                WheelRef,
                WheelsPid,
                BlockwheelPid,
            },
        },
        search_ranges_merge::{
            Source,
            SourceButcher,
            SourceSearchTree,
            RangesMergeCps,
        },
        performer::{
            LookupRangeToken,
            LookupRangeSource,
            LookupRangesMerger,
        },
        tests::{
            BLOCKWHEEL_FILENAME,
            to_bytes,
            make_memcache,
            make_kinda_tree,
            kinda_parse_item,
        },
        BlockRef,
        SearchRangeBounds,
        RequestLookupKind,
        RequestLookupKindSingle,
        RequestLookupKindRange,
    },
    LookupRange,
    KeyValueStreamItem,
};

#[tokio::test]
async fn basic_range() {
    let key_from: kv::Key = to_bytes("3 third").into();
    let key_to: kv::Key = to_bytes("6 sixth").into();
    let found_items = emulate_lookup(Lookup::Range { key_from, key_to, }).await;
    assert_eq!(
        found_items,
        vec![
            ("3 third".to_string(), Some("data 3".to_string()), 0),
            ("3 thirdz".to_string(), Some("data 3z".to_string()), 1),
            ("4 fourth".to_string(), Some("data 4zz".to_string()), 2),
            ("5 fifth".to_string(), Some("data 5".to_string()), 0),
        ],
    );
}

#[tokio::test]
async fn basic_single_found() {
    let key: kv::Key = to_bytes("5 fifth").into();
    let found_items = emulate_lookup(Lookup::Single { key, }).await;
    assert_eq!(
        found_items,
        vec![
            ("5 fifth".to_string(), Some("data 5".to_string()), 0),
        ],
    );
}

enum Lookup {
    Single { key: kv::Key, },
    Range { key_from: kv::Key, key_to: kv::Key, },
}

async fn emulate_lookup(lookup: Lookup) -> Vec<(String, Option<String>, u64)> {
    let search_range: SearchRangeBounds = match &lookup {
        Lookup::Single { key, } =>
            (key.clone() ..= key.clone()).into(),
        Lookup::Range { key_from, key_to, } =>
            (key_from.clone() .. key_to.clone()).into(),
    };

    let (root_block, kinda_tree) = make_kinda_tree();
    let source_a = LookupRangeSource {
        source: Source::SearchTree(SourceSearchTree::new(
            search_range.clone(),
            root_block,
        )),
    };

    let frozen_memcache = make_memcache([
        ("2 secondz", "data 2z", 1),
        ("3 thirdz", "data 3z", 1),
        ("4 fourth", "data 4zz", 2),
        ("8 eighthz", "data 8z", 1),
    ]);
    let source_b = LookupRangeSource {
        source: Source::Butcher(SourceButcher::new(
            search_range.clone(),
            frozen_memcache,
        )),
    };

    let kv_pool = pool::Pool::new();
    let block_entry_steps_pool = pool::Pool::new();
    let sources = Unique::new_detached(vec![source_a, source_b]);
    let ranges_merge =
        RangesMergeCps::new(
            sources,
            kv_pool,
            block_entry_steps_pool,
        );

    let lookup_ranges_merger = LookupRangesMerger {
        source: ranges_merge,
        token: LookupRangeToken::default(),
    };

    let thread_pool: edeltraud::Edeltraud<job::Job> = edeltraud::Builder::new()
        .worker_threads(1)
        .build()
        .unwrap();

    let wheels_pid = kinda_tree_make_wheels_pid(kinda_tree);

    let (result_tx, result_rx) = oneshot::channel();
    let request_lookup_kind = match lookup {
        Lookup::Single { key, } => {
            let (reply_tx, reply_rx) = oneshot::channel();
            tokio::spawn(async move {
                let results = match reply_rx.await.unwrap() {
                    None =>
                        vec![],
                    Some(value_cell) => {
                        let item = kinda_parse_item(&kv::KeyValuePair {
                            key,
                            value_cell: kv::ValueCell::<storage::OwnedValueBlockRef>::from(value_cell),
                        });
                        vec![item]
                    },
                };
                assert!(result_tx.send(results).is_ok());
            });

            RequestLookupKind::Single(RequestLookupKindSingle { reply_tx, })
        },
        Lookup::Range { .. } => {
            let (reply_tx, reply_rx) = oneshot::channel();
            tokio::spawn(async move {
                let LookupRange { mut key_values_rx, } = reply_rx.await.unwrap();
                let mut results = Vec::new();
                loop {
                    let item = match key_values_rx.next().await.unwrap() {
                        KeyValueStreamItem::KeyValue(item) =>
                            kinda_parse_item(&item.into()),
                        KeyValueStreamItem::NoMore =>
                            break,
                    };
                    results.push(item);
                }
                assert!(key_values_rx.next().await.is_none());
                assert!(result_tx.send(results).is_ok());
            });
            RequestLookupKind::Range(RequestLookupKindRange { reply_tx, })
        },
    };

    let _done =
        inner_run(
            lookup_ranges_merger,
            request_lookup_kind,
            wheels_pid,
            thread_pool,
            0,
        )
        .await
        .unwrap();

    result_rx.await.unwrap()
}

fn kinda_tree_make_wheels_pid(kinda_tree: HashMap<BlockRef, Bytes>) -> WheelsPid {
    let write_fn = |_block_bytes| panic!("unimplemented on purpose");
    let read_fn = move |block_id| {
        let block_ref = BlockRef {
            blockwheel_filename: to_bytes(BLOCKWHEEL_FILENAME).into(),
            block_id,
        };
        kinda_tree.get(&block_ref).unwrap().clone()
    };

    let blockwheel_pid = BlockwheelPid::Custom {
        write_block: Arc::new(Mutex::new(write_fn)),
        read_block: Arc::new(Mutex::new(read_fn)),
    };

    let acquire_fn = || panic!("unimplemented on purpose");
    let get_fn = move |_filename| {
        WheelRef {
            blockwheel_filename: to_bytes(BLOCKWHEEL_FILENAME).into(),
            blockwheel_pid: blockwheel_pid.clone(),
        }
    };

    WheelsPid::Custom {
        acquire: Arc::new(Mutex::new(acquire_fn)),
        get: Arc::new(Mutex::new(get_fn)),
    }
}
