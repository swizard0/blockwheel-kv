use std::{
    sync::{
        Arc,
        Mutex,
    },
    collections::{
        HashSet,
    },
};

use alloc_pool::{
    pool,
    bytes::{
        Bytes,
        BytesPool,
    },
};

use crate::{
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
        tests::{
            BLOCKWHEEL_FILENAME,
            to_bytes,
            make_memcache,
        },
    },
};

#[tokio::test]
async fn basic() {
    let search_tree_id = 77;
    let thread_pool: edeltraud::Edeltraud<job::Job> = edeltraud::Builder::new()
        .worker_threads(1)
        .build()
        .unwrap();
    let frozen_memcache = make_memcache([
        ("key 1", "value 1", 1),
        ("key 2", "value 2", 1),
        ("key 3", "value 3", 1),
        ("key 4", "value 4", 2),
        ("key 5", "value 5", 1),
        ("key 6", "value 6", 2),
        ("key 7", "value 7", 1),
    ]);
    let wheels_pid = basic_make_wheels_pid();
    let blocks_pool = BytesPool::new();
    let block_entries_pool = pool::Pool::new();
    let search_tree_builder_params = search_tree_builder::Params {
        tree_items_count: frozen_memcache.len(),
        tree_block_size: 3,
    };
    let values_inline_size_limit = 128;

    let done =
        inner_run(
            search_tree_id,
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
    assert_eq!(done.search_tree_id, search_tree_id);
    assert_eq!(&*done.root_block.blockwheel_filename, BLOCKWHEEL_FILENAME.as_bytes());
}

#[tokio::test]
async fn external_value() {
    let search_tree_id = 77;
    let thread_pool: edeltraud::Edeltraud<job::Job> = edeltraud::Builder::new()
        .worker_threads(1)
        .build()
        .unwrap();
    let frozen_memcache = make_memcache([
        ("key 1", "0123456789ABCDEF", 1),
    ]);
    let wheels_pid = external_value_make_wheels_pid();
    let blocks_pool = BytesPool::new();
    let block_entries_pool = pool::Pool::new();
    let search_tree_builder_params = search_tree_builder::Params {
        tree_items_count: frozen_memcache.len(),
        tree_block_size: 3,
    };
    let values_inline_size_limit = 10;

    let done =
        inner_run(
            search_tree_id,
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
    assert_eq!(done.search_tree_id, search_tree_id);
    assert_eq!(&*done.root_block.blockwheel_filename, BLOCKWHEEL_FILENAME.as_bytes());
}

fn basic_make_wheels_pid() -> WheelsPid {
    let mut samples = HashSet::from([
        vec![
            ("key 1".to_string(), Some("value 1".to_string()), 1u64),
            ("key 2".to_string(), Some("value 2".to_string()), 1),
            ("key 3".to_string(), Some("value 3".to_string()), 1),
        ],
        vec![
            ("key 4".to_string(), Some("value 4".to_string()), 2),
            ("key 6".to_string(), Some("value 6".to_string()), 2),
            ("key 7".to_string(), Some("value 7".to_string()), 1),
        ],
        vec![
            ("key 5".to_string(), Some("value 5".to_string()), 1),
        ],
    ]);

    let mut global_block_id = ero_blockwheel_fs::block::Id::init();

    let write_fn = move |block_bytes| {
        let items = deserialize_block_entries(&block_bytes);
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

fn external_value_make_wheels_pid() -> WheelsPid {
    let mut sample_value = Some("0123456789ABCDEF".to_string());
    let mut sample_block = Some(
        ("key 1".to_string(), 1u64),
    );

    let mut global_block_id = ero_blockwheel_fs::block::Id::init();

    let write_fn = move |block_bytes| {
        match sample_value.take() {
            Some(sample) => {
                let value = storage::value_block_deserialize(&block_bytes).unwrap();
                assert_eq!(sample.as_bytes(), &*value);
            },
            None =>
                match sample_block.take() {
                    Some((sample_key, sample_version)) => {
                        let items = deserialize_block_entries(&block_bytes);
                        assert_eq!(items, vec![(sample_key, None, sample_version)]);
                    },
                    None =>
                        unreachable!(),
                },
        }
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

fn deserialize_block_entries(block_bytes: &Bytes) -> Vec<(String, Option<String>, u64)> {
    let block_iter = storage::block_deserialize_iter(block_bytes).unwrap();
    block_iter
        .map(|maybe_entry| maybe_entry.unwrap())
        .map(|entry| {
            (
                String::from_utf8(entry.key.to_vec()).unwrap(),
                match entry.value_cell.cell {
                    storage::Cell::Value(storage::ValueRef::Inline(value)) =>
                        Some(String::from_utf8(value.to_vec()).unwrap()),
                    storage::Cell::Value(storage::ValueRef::Local(..) | storage::ValueRef::External(..)) =>
                        None,
                    _ =>
                        unreachable!(),
                },
                entry.value_cell.version,
            )
        })
        .collect()
}
