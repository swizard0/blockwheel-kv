
use futures::{
    channel::{
        mpsc,
    },
    stream,
    SinkExt,
    StreamExt,
};

use alloc_pool::{
    bytes::{
        BytesMut,
    },
    Unique,
};

use crate::{
    kv,
    job,
    wheels,
    core::{
        manager::{
            task::{
                merge_lookup_range,
            },
        },
        KeyValueRef,
        SearchRangeBounds,
    },
    KeyValueStreamItem,
};

#[tokio::test]
async fn basic() {
    let key_values_rx = spawn_run(
        vec![(1, 0), (4, 0), (8, 0), (9, 0)],
        vec![
            vec![(2, 0), (3, 0)],
            vec![],
            vec![(10, 0)],
            vec![(5, 0), (6, 0), (7, 0), (11, 0)],
        ],
    );

    let outcome: Vec<_> = key_values_rx.map(to_my_stream_item).collect().await;
    assert_eq!(
        outcome,
        vec![
            MyStreamItem::Item { key: 1, version: 0, },
            MyStreamItem::Item { key: 2, version: 0, },
            MyStreamItem::Item { key: 3, version: 0, },
            MyStreamItem::Item { key: 4, version: 0, },
            MyStreamItem::Item { key: 5, version: 0, },
            MyStreamItem::Item { key: 6, version: 0, },
            MyStreamItem::Item { key: 7, version: 0, },
            MyStreamItem::Item { key: 8, version: 0, },
            MyStreamItem::Item { key: 9, version: 0, },
            MyStreamItem::Item { key: 10, version: 0, },
            MyStreamItem::Item { key: 11, version: 0, },
            MyStreamItem::NoMore,
        ],
    );
}

#[tokio::test]
async fn stress_10_derived_00() {
    let key_values_rx = spawn_run(
        vec![(17556261672556393688, 1)],
        vec![
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
        ],
    );

    let outcome: Vec<_> = key_values_rx.map(to_my_stream_item).collect().await;
    assert_eq!(
        outcome,
        vec![
            MyStreamItem::Item { key: 3144521344682756311, version: 9, },
            MyStreamItem::Item { key: 6956425666106740206, version: 5, },
            MyStreamItem::Item { key: 6991660086521729989, version: 2, },
            MyStreamItem::Item { key: 8093639385199525262, version: 6, },
            MyStreamItem::Item { key: 11486697784006332595, version: 3, },
            MyStreamItem::Item { key: 16010932765423118261, version: 8, },
            MyStreamItem::Item { key: 16107956337732637054, version: 1, },
            MyStreamItem::Item { key: 16122783848902096046, version: 2, },
            MyStreamItem::Item { key: 16748507097280151939, version: 0, },
            MyStreamItem::Item { key: 17556261672556393688, version: 2, },
            MyStreamItem::NoMore,
        ],
    );
}

fn spawn_run(butcher_iter_data: Vec<(u64, u64)>, search_tree_iters_data: Vec<Vec<(u64, u64)>>) -> mpsc::Receiver<KeyValueStreamItem> {
    let butcher_iter_items: Vec<_> = butcher_iter_data
        .into_iter()
        .map(make_kv_pair)
        .collect();

    let search_tree_streams: Vec<_> = search_tree_iters_data
        .into_iter()
        .map(|data| {
            stream::iter(data.into_iter().map(make_key_value_ref).map(Ok))
        })
        .collect();

    let mut search_tree_iters = Vec::new();
    for mut search_tree_stream in search_tree_streams {
        let (mut tx, rx) = mpsc::channel(0);
        tokio::spawn(async move {
            tx.send_all(&mut search_tree_stream).await.unwrap();
            tx.send(KeyValueRef::NoMore).await.unwrap();
        });
        search_tree_iters.push(rx);
    }

    let (key_values_tx, key_values_rx) = mpsc::channel(0);

    let thread_pool: edeltraud::Edeltraud<job::Job> = edeltraud::Builder::new()
        .build()
        .unwrap();

    let (wheels_requests_tx, _wheels_requests_rx) = mpsc::channel(0);
    let wheels_pid = wheels::Pid::from_external(wheels_requests_tx);

    let args = merge_lookup_range::Args {
        range: SearchRangeBounds::unbounded(),
        key_values_tx,
        butcher_iter_items: Unique::new_detached(butcher_iter_items).freeze(),
        merger_iters: Unique::new_detached(search_tree_iters),
        wheels_pid,
        thread_pool,
    };

    tokio::spawn(async move { merge_lookup_range::run(args).await.unwrap(); });

    key_values_rx
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

#[derive(PartialEq, Eq, Debug)]
enum MyStreamItem {
    Item { key: u64, version: u64, },
    NoMore,
}

fn to_my_stream_item(stream_item: KeyValueStreamItem) -> MyStreamItem {
    match stream_item {
        KeyValueStreamItem::KeyValue(kv_pair) => {
            let key = u64::from_be_bytes(kv_pair.key.key_bytes.to_vec().try_into().unwrap());
            let version = kv_pair.value_cell.version;
            MyStreamItem::Item { key, version, }
        },
        KeyValueStreamItem::NoMore =>
            MyStreamItem::NoMore,
    }
}
