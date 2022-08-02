use std::{
    sync::{
        Arc,
        Mutex,
    },
    collections::{
        HashSet,
    },
};

use o1::{
    set::{
        Set,
    },
};

use alloc_pool::{
    pool,
    bytes::{
        Bytes,
        BytesMut,
        BytesPool,
    },
};

use crate::{
    kv,
    job,
    wheels,
    core::{
        storage,
        manager::{
            task::{
                flush_butcher::{
                    WheelRef,
                    WheelsPid,
                    BlockwheelPid,
                    inner_run,
                },
            },
        },
        search_tree_builder,
        OrdKey,
        MemCache,
    },
};

const BLOCKWHEEL_FILENAME: &'static str = "my_filename.blockwheel";

fn to_bytes(s: &str) -> Bytes {
    BytesMut::new_detached(s.as_bytes().to_vec()).freeze()
}

#[tokio::test]
async fn basic() {
    let mut set = Set::new();
    let search_tree_ref = set.insert("some data");
    let thread_pool: edeltraud::Edeltraud<job::Job> = edeltraud::Builder::new()
        .worker_threads(1)
        .build()
        .unwrap();
    let frozen_memcache = make_memcache();
    let wheels_pid = make_wheels_pid();
    let blocks_pool = BytesPool::new();
    let block_entries_pool = pool::Pool::new();
    let search_tree_builder_params = search_tree_builder::Params {
        tree_items_count: frozen_memcache.len(),
        tree_block_size: 3,
    };
    let values_inline_size_limit = 128;

    let done =
        inner_run(
            search_tree_ref,
            frozen_memcache,
            wheels_pid,
            blocks_pool,
            block_entries_pool,
            search_tree_builder_params,
            values_inline_size_limit,
            thread_pool,
        )
        .await
        .unwrap();
    assert_eq!(done.search_tree_ref, search_tree_ref);
    assert_eq!(&*done.root_block.blockwheel_filename, BLOCKWHEEL_FILENAME.as_bytes());
}

fn make_wheels_pid() -> WheelsPid {
    let mut samples = HashSet::from([
        vec![
            ("key 1".to_string(), "value 1".to_string(), 1u64),
            ("key 2".to_string(), "value 2".to_string(), 1),
            ("key 3".to_string(), "value 3".to_string(), 1),
        ],
        vec![
            ("key 4".to_string(), "value 4".to_string(), 2),
            ("key 6".to_string(), "value 6".to_string(), 2),
            ("key 7".to_string(), "value 7".to_string(), 1),
        ],
        vec![
            ("key 5".to_string(), "value 5".to_string(), 1),
        ],
    ]);

    let mut global_block_id = ero_blockwheel_fs::block::Id::init();

    let write_fn = move |block_bytes: Bytes| {
        let block_iter = storage::block_deserialize_iter(&block_bytes).unwrap();
        let items: Vec<_> = block_iter
            .map(|maybe_entry| maybe_entry.unwrap())
            .map(|entry| {
                (
                    String::from_utf8(entry.key.to_vec()).unwrap(),
                    match entry.value_cell.cell {
                        storage::Cell::Value(storage::ValueRef::Inline(value)) =>
                            String::from_utf8(value.to_vec()).unwrap(),
                        _ =>
                            unreachable!(),
                    },
                    entry.value_cell.version,
                )
            })
            .collect();
        assert!(samples.remove(&items));

        let block_id = global_block_id.clone();
        global_block_id = global_block_id.next();
        block_id
    };

    let blockwheel_filename: wheels::WheelFilename =
        to_bytes(BLOCKWHEEL_FILENAME).into();
    let blockwheel_pid = BlockwheelPid::Custom(
        Arc::new(Mutex::new(write_fn)),
    );

    let acquire_fn = move || {
        WheelRef {
            blockwheel_filename: blockwheel_filename.clone(),
            blockwheel_pid: blockwheel_pid.clone(),
        }
    };

    WheelsPid::Custom(
        Arc::new(Mutex::new(acquire_fn))
    )
}

fn make_memcache() -> Arc<MemCache> {
    let items = [
        ("key 1", "value 1", 1),
        ("key 2", "value 2", 1),
        ("key 3", "value 3", 1),
        ("key 4", "value 4", 2),
        ("key 5", "value 5", 1),
        ("key 6", "value 6", 2),
        ("key 7", "value 7", 1),
    ];
    let mut memcache = MemCache::new();
    memcache.extend(
        items
            .into_iter()
            .map(|(key, value, version)| {
                (
                    OrdKey::new(to_bytes(key).into()),
                    kv::ValueCell {
                        version,
                        cell: kv::Cell::Value(to_bytes(value).into()),
                    },
                )
            })
    );
    Arc::new(memcache)
}
