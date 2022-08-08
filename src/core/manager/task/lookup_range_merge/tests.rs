use std::{
    sync::{
        Arc,
        Mutex,
    },
    collections::{
        HashSet,
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
                lookup_range_merge::{
                    inner_run,
                },
                WheelRef,
                WheelsPid,
                BlockwheelPid,
            },
        },
        tests::{
            BLOCKWHEEL_FILENAME,
            to_bytes,
            make_memcache,
            make_kinda_tree,
            kinda_parse_item,
        },
        RequestLookupKind,
        RequestLookupKindRange,
    },
    LookupRange,
    KeyValueStreamItem,
};

#[tokio::test]
async fn basic() {
    let thread_pool: edeltraud::Edeltraud<job::Job> = edeltraud::Builder::new()
        .worker_threads(1)
        .build()
        .unwrap();

    let (reply_tx, reply_rx) = oneshot::channel();
    let request_lookup_kind = RequestLookupKind::Range(RequestLookupKindRange { reply_tx, });

    let (result_tx, result_rx) = oneshot::channel();
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

    let done =
        inner_run(
            todo!(), // ranges_merger: performer::LookupRangesMerger,
            request_lookup_kind,
            todo!(), // wheels_pid: WheelsPid,
            thread_pool,
            0, // iter_send_buffer: usize,
        )
        .await
        .unwrap();
    // assert_eq!(done.lookup_range_token., search_tree_id);
    // assert_eq!(&*done.root_block.blockwheel_filename, BLOCKWHEEL_FILENAME.as_bytes());
    let found_items = result_rx.await.unwrap();
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
