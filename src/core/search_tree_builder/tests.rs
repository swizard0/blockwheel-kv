use alloc_pool::{
    pool,
};

use crate::{
    storage,
    core::{
        search_tree_builder::{
            Params,
            BuilderCps,
            Kont,
            KontPollNextItemOrProcessedBlock,
            KontPollProcessedBlock,
            KontProcessBlockAsync,
        },
    },
};

#[test]
fn basic_00() {
    let builder: BuilderCps<&'static str, usize> = BuilderCps::new(
        pool::Pool::new(),
        Params {
            tree_items_count: 5,
            tree_block_size: 2,
        },
    );
    let kont = builder.step().unwrap();
    let kont = match kont {
        Kont::PollNextItemOrProcessedBlock(KontPollNextItemOrProcessedBlock { next, }) =>
            next.item_arrived("first").unwrap(),
        _ =>
            panic!("expected proper Kont::PollNextItemOrProcessedBlock but got other"),
    };
    let kont = match kont {
        Kont::PollNextItemOrProcessedBlock(KontPollNextItemOrProcessedBlock { next, }) =>
            next.item_arrived("second").unwrap(),
        _ =>
            panic!("expected proper Kont::PollNextItemOrProcessedBlock but got other"),
    };
    let (kont, block_0_ref) = match kont {
        Kont::ProcessBlockAsync(KontProcessBlockAsync { node_type: storage::NodeType::Leaf, block_entries, async_ref, next, }) => {
            assert_eq!(block_entries.len(), 2);
            assert_eq!(block_entries[0].item, "first");
            assert_eq!(block_entries[0].child_block_ref, None);
            assert_eq!(block_entries[1].item, "second");
            assert_eq!(block_entries[1].child_block_ref, None);
            (next.process_scheduled().unwrap(), async_ref)
        },
        _ =>
            panic!("expected proper Kont::ProcessBlockAsync but got other"),
    };
    let kont = match kont {
        Kont::PollNextItemOrProcessedBlock(KontPollNextItemOrProcessedBlock { next, }) =>
            next.item_arrived("third").unwrap(),
        _ =>
            panic!("expected proper Kont::PollNextItemOrProcessedBlock but got other"),
    };
    let kont = match kont {
        Kont::PollNextItemOrProcessedBlock(KontPollNextItemOrProcessedBlock { next, }) =>
            next.item_arrived("fourth").unwrap(),
        _ =>
            panic!("expected proper Kont::PollNextItemOrProcessedBlock but got other"),
    };
    let (kont, block_1_ref) = match kont {
        Kont::ProcessBlockAsync(KontProcessBlockAsync { node_type: storage::NodeType::Leaf, block_entries, async_ref, next, }) => {
            assert_eq!(block_entries.len(), 1);
            assert_eq!(block_entries[0].item, "fourth");
            assert_eq!(block_entries[0].child_block_ref, None);
            (next.process_scheduled().unwrap(), async_ref)
        },
        _ =>
            panic!("expected proper Kont::ProcessBlockAsync but got other"),
    };
    let kont = match kont {
        Kont::PollNextItemOrProcessedBlock(KontPollNextItemOrProcessedBlock { next, }) =>
            next.item_arrived("fifth").unwrap(),
        _ =>
            panic!("expected proper Kont::PollNextItemOrProcessedBlock but got other"),
    };
    let kont = match kont {
        Kont::PollProcessedBlock(KontPollProcessedBlock { next, }) =>
            next.block_processed(block_1_ref, 16777217).unwrap(),
        _ =>
            panic!("expected proper Kont::PollProcessedBlock but got other"),
    };
    let kont = match kont {
        Kont::PollProcessedBlock(KontPollProcessedBlock { next, }) =>
            next.block_processed(block_0_ref, 16777216).unwrap(),
        _ =>
            panic!("expected proper Kont::PollProcessedBlock but got other"),
    };
    let (kont, block_2_ref) = match kont {
        Kont::ProcessBlockAsync(KontProcessBlockAsync {
            node_type: storage::NodeType::Root { tree_entries_count, },
            block_entries,
            async_ref,
            next,
        }) => {
            assert_eq!(tree_entries_count, 5);
            assert_eq!(block_entries.len(), 2);
            assert_eq!(block_entries[0].item, "third");
            assert_eq!(block_entries[0].child_block_ref, Some(16777216));
            assert_eq!(block_entries[1].item, "fifth");
            assert_eq!(block_entries[1].child_block_ref, Some(16777217));
            (next.process_scheduled().unwrap(), async_ref)
        },
        _ =>
            panic!("expected proper Kont::ProcessBlockAsync but got other"),
    };
    let kont = match kont {
        Kont::PollProcessedBlock(KontPollProcessedBlock { next, }) =>
            next.block_processed(block_2_ref, 16777218).unwrap(),
        _ =>
            panic!("expected proper Kont::PollProcessedBlock but got other"),
    };
    match kont {
        Kont::Finished { root_block_ref: 16777218, } =>
            (),
        _ =>
            panic!("expected proper Kont::Finished but got other"),
    }
}
