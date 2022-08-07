use alloc_pool::{
    pool,
};

use crate::{
    kv,
    core::{
        search_ranges_merge::{
            RangesMergeCps,
            RangesMergeCpsInit,
            Source,
            SourceButcher,
            SourceSearchTree,
            Kont,
            KontRequireBlockAsync,
            KontAwaitBlocks,
            KontEmitDeprecated,
            KontEmitItem,
            KontFinished,
        },
        tests::{
            to_bytes,
            make_memcache,
            make_kinda_tree,
            kinda_parse_item,
        },
        SearchRangeBounds,
    },
};

#[test]
fn basic() {
    let key_from: kv::Key = to_bytes("3 third").into();
    let key_to: kv::Key = to_bytes("6 sixth").into();
    let search_range: SearchRangeBounds = (key_from .. key_to).into();

    let (root_block, kinda_tree) = make_kinda_tree();
    let source_a = Source::SearchTree(SourceSearchTree::new(
        search_range.clone(),
        root_block,
    ));

    let frozen_memcache = make_memcache([
        ("2 secondz", "data 2z", 1),
        ("3 thirdz", "data 3z", 1),
        ("4 fourth", "data 4zz", 2),
        ("8 eighthz", "data 8z", 1),
    ]);
    let source_b = Source::Butcher(SourceButcher::new(
        search_range.clone(),
        frozen_memcache,
    ));

    let kv_pool = pool::Pool::new();
    let block_entry_steps_pool = pool::Pool::new();
    let mut sources = vec![source_a, source_b];
    let ranges_merge =
        RangesMergeCps::from(
            RangesMergeCpsInit::new(
                &mut sources,
                kv_pool,
                block_entry_steps_pool,
            ),
        );

    let mut found_items = Vec::new();
    let mut found_deprecated = Vec::new();
    let mut await_blocks = Vec::new();

    let mut ranges_merge_kont = ranges_merge.step().unwrap();
    loop {
        match ranges_merge_kont {
            Kont::RequireBlockAsync(KontRequireBlockAsync { block_ref, async_token, next, }) => {
                await_blocks.push((block_ref, async_token));
                ranges_merge_kont = next.scheduled().unwrap();
            },
            Kont::AwaitBlocks(KontAwaitBlocks { next, }) => {
                let (block_ref, async_token) = await_blocks.pop().unwrap();
                let block_bytes = kinda_tree.get(&block_ref).unwrap().clone();
                ranges_merge_kont = next.block_arrived(async_token, block_bytes).unwrap();
            },
            Kont::EmitDeprecated(KontEmitDeprecated { ref item, next, }) => {
                found_deprecated.push(kinda_parse_item(item));
                ranges_merge_kont = next.proceed().unwrap();
            },
            Kont::EmitItem(KontEmitItem { ref item, next, }) => {
                found_items.push(kinda_parse_item(item));
                ranges_merge_kont = next.proceed().unwrap();
            },
            Kont::Finished(KontFinished { sources, }) => {
                assert!(await_blocks.is_empty());
                assert_eq!(sources.len(), 2);
                break;
            },
        }
    }

    assert_eq!(
        found_items,
        vec![
            ("3 third".to_string(), Some("data 3".to_string()), 0),
            ("3 thirdz".to_string(), Some("data 3z".to_string()), 1),
            ("4 fourth".to_string(), Some("data 4zz".to_string()), 2),
            ("5 fifth".to_string(), Some("data 5".to_string()), 0),
        ],
    );
    assert_eq!(
        found_deprecated,
        vec![("4 fourth".to_string(), Some("data 4".to_string()), 0)],
    );
}
