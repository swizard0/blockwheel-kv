
use futures::{
    channel::{
        mpsc,
    },
    stream,
    SinkExt,
};

use alloc_pool::{
    bytes::{
        Bytes,
        BytesMut,
    },
    Shared,
    Unique,
};

use crate::{
    kv,
    job,
    core::{
        manager::{
            task::{
                merge_lookup_range::{
                    Args,
                },
            },
        },
        KeyValueRef,
        SearchRangeBounds,
    },
};

#[tokio::test]
async fn basic() {

    let butcher_iter_items: Vec<_> = [(1, 0), (4, 0), (8, 0), (9, 0)]
        .into_iter()
        .map(make_kv_pair)
        .collect();

    let search_tree_streams = vec![
        stream::iter([(2, 0), (3, 0)].iter().cloned().map(make_key_value_ref).map(Ok)),
        stream::iter([].iter().cloned().map(make_key_value_ref).map(Ok)),
        stream::iter([(10, 0)].iter().cloned().map(make_key_value_ref).map(Ok)),
        stream::iter([(5, 0), (6, 0), (7, 0), (11, 0)].iter().cloned().map(make_key_value_ref).map(Ok)),
    ];

    let mut search_tree_iters = Vec::new();
    for mut search_tree_stream in search_tree_streams {
        let (mut tx, rx) = mpsc::channel(0);
        tokio::spawn(async move {
            tx.send_all(&mut search_tree_stream);
        });
        search_tree_iters.push(rx);
    }

    let (key_values_tx, key_values_rx) = mpsc::channel(0);

    let thread_pool: edeltraud::Edeltraud<job::Job> = edeltraud::Builder::new()
        .build()
        .unwrap();

    let args = Args {
        range: SearchRangeBounds::unbounded(),
        key_values_tx,
        butcher_iter_items: Unique::new_detached(butcher_iter_items).freeze(),
        merger_iters: Unique::new_detached(search_tree_iters),
        wheels_pid: todo!(),
        thread_pool,
    };


}

fn make_kv_pair((key, version): (u64, u64)) -> kv::KeyValuePair<kv::Value> {
    let (key, value_cell) = make_key_version(key, version);
    kv::KeyValuePair { key, value_cell, }
}

fn make_key_value_ref((key, version): (u64, u64)) -> KeyValueRef {
    let (key, value_cell) = make_key_version(key, version);
    KeyValueRef::Item { key, value_cell, }
}

fn make_key_version<V>(key: u64, version: u64) -> (kv::Key, kv::ValueCell<V>) {
    (
        kv::Key {
            key_bytes: BytesMut::new_detached(key.to_be_bytes().to_vec()).freeze(),
        },
        kv::ValueCell {
            version: version.try_into().unwrap(),
            cell: kv::Cell::Tombstone,
        },
    )
}
