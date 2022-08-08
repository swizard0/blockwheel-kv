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
        },
    },
};

#[tokio::test]
async fn basic() {
    let thread_pool: edeltraud::Edeltraud<job::Job> = edeltraud::Builder::new()
        .worker_threads(1)
        .build()
        .unwrap();

    let done =
        inner_run(
            todo!(), // ranges_merger: performer::LookupRangesMerger,
            todo!(), // lookup_context: RequestLookupKind,
            todo!(), // wheels_pid: WheelsPid,
            thread_pool,
            0, // iter_send_buffer: usize,
        )
        .await
        .unwrap();
    // assert_eq!(done.lookup_range_token., search_tree_id);
    // assert_eq!(&*done.root_block.blockwheel_filename, BLOCKWHEEL_FILENAME.as_bytes());
}
