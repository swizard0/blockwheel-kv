use std::{
    ops::{
        RangeBounds,
    },
};

use alloc_pool::{
    pool,
};

use crate::{
    kv,
    core::{
        search_tree_walker::{
            WalkerCps,
            Kont,
            KontRequireBlock,
            KontItemFound,
        },
        tests::{
            to_bytes,
            make_kinda_tree,
            kinda_parse_item,
        },
    },
};

#[test]
fn search_range_three_blocks() {
    let key_from: kv::Key = to_bytes("3 third").into();
    let key_to: kv::Key = to_bytes("6 sixth").into();
    walk(
        key_from .. key_to,
        &[
            ("3 third", "data 3"),
            ("4 fourth", "data 4"),
            ("5 fifth", "data 5")
        ],
    );
}

#[test]
fn search_one() {
    let key_from: kv::Key = to_bytes("5 fifth").into();
    let key_to: kv::Key = to_bytes("5 fifth").into();
    walk(
        key_from ..= key_to,
        &[
            ("5 fifth", "data 5")
        ],
    );
}

#[test]
fn search_none() {
    let key_from: kv::Key = to_bytes("5 fifth other").into();
    let key_to: kv::Key = to_bytes("6 sixt").into();
    walk(key_from .. key_to, &[]);
}

#[test]
fn search_two() {
    let key_to: kv::Key = to_bytes("2 secondz").into();
    walk(
        .. key_to,
        &[
            ("1 first", "data 1"),
            ("2 second", "data 2"),
        ],
    );
}

#[test]
fn search_all() {
    walk(
        ..,
        &[
            ("1 first", "data 1"),
            ("2 second", "data 2"),
            ("3 third", "data 3"),
            ("4 fourth", "data 4"),
            ("5 fifth", "data 5"),
            ("6 sixth", "data 6"),
            ("7 seventh", "data 7"),
        ],
    );
}

fn walk<R>(range: R, sample: &[(&str, &str)]) where R: RangeBounds<kv::Key> {
    let (root_block, kinda_tree) = make_kinda_tree();
    let block_entry_steps_pool = pool::Pool::new();
    let walker = WalkerCps::new(root_block, range.into(), block_entry_steps_pool);
    let mut kont = walker.step().unwrap();
    let mut found_items = vec![];
    loop {
        match kont {
            Kont::RequireBlock(KontRequireBlock { block_ref, next, }) => {
                let block_bytes = kinda_tree.get(&block_ref).unwrap().clone();
                kont = next.block_arrived(block_bytes).unwrap();
            },
            Kont::ItemFound(KontItemFound { item, next, }) => {
                let (key, maybe_value, _version) = kinda_parse_item(&item);
                found_items.push((key, maybe_value.unwrap()));
                kont = next.item_received().unwrap();
            },
            Kont::Finished =>
                break,
        }
    }
    let sample_vec: Vec<_> = sample.iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    assert_eq!(found_items, sample_vec);
}
