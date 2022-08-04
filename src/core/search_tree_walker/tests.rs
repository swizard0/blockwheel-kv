use std::{
    ops::{
        RangeBounds,
    },
    collections::{
        HashMap,
    },
};

use alloc_pool::{
    pool,
    bytes::{
        Bytes,
        BytesMut,
    },
};

use crate::{
    kv,
    wheels,
    storage,
    core::{
        search_tree_walker::{
            WalkerCps,
            Kont,
            KontRequireBlock,
            KontItemFound,
        },
        BlockRef,
    },
};

fn to_bytes(s: &str) -> Bytes {
    BytesMut::new_detached(s.as_bytes().to_vec()).freeze()
}

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
    let key_from: kv::Key = to_bytes("5 fifth other").into();
    let key_to: kv::Key = to_bytes("6 sixt").into();
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
                let key = String::from_utf8(item.key.key_bytes.to_vec()).unwrap();
                let value = match item.value_cell {
                    kv::ValueCell { cell: kv::Cell::Value(storage::OwnedValueBlockRef::Inline(value)), .. } =>
                        String::from_utf8(value.value_bytes.to_vec()).unwrap(),
                    _ =>
                        unreachable!(),
                };
                found_items.push((key, value));
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

const BLOCKWHEEL_FILENAME: &'static str = "my_filename.blockwheel";

fn make_kinda_tree() -> (BlockRef, HashMap<BlockRef, Bytes>) {
    let mut global_block_id = ero_blockwheel_fs::block::Id::init();

    let (block_a_ref, bytes_a) = make_kinda_block(
        storage::NodeType::Leaf,
        &mut global_block_id,
        vec![
            (None, "1 first", "data 1"),
            (None, "2 second", "data 2"),
            (None, "3 third", "data 3"),
        ],
    );
    let (block_b_ref, bytes_b) = make_kinda_block(
        storage::NodeType::Leaf,
        &mut global_block_id,
        vec![
            (None, "5 fifth", "data 5"),
        ],
    );
    let (block_c_ref, bytes_c) = make_kinda_block(
        storage::NodeType::Root { tree_entries_count: 7, },
        &mut global_block_id,
        vec![
            (Some(block_a_ref.clone()), "4 fourth", "data 4"),
            (Some(block_b_ref.clone()), "6 sixth", "data 6"),
            (None, "7 seventh", "data 7"),
        ],
    );
    let kinda_tree = HashMap::from([
        (block_a_ref, bytes_a),
        (block_b_ref, bytes_b),
        (block_c_ref.clone(), bytes_c),
    ]);
    (block_c_ref, kinda_tree)
}

fn make_kinda_block(
    node_type: storage::NodeType,
    global_block_id: &mut ero_blockwheel_fs::block::Id,
    items: Vec<(Option<BlockRef>, &str, &str)>,
)
    -> (BlockRef, Bytes)
{
    let blockwheel_filename: wheels::WheelFilename =
        to_bytes(BLOCKWHEEL_FILENAME).into();
    let block_id = global_block_id.clone();
    let block_ref = BlockRef { blockwheel_filename: blockwheel_filename.clone(), block_id, };
    *global_block_id = global_block_id.next();

    let block_bytes = BytesMut::new_detached(Vec::new());
    let mut kont = storage::BlockSerializer::start(node_type, items.len(), block_bytes).unwrap();
    let mut items_iter = items.into_iter();
    loop {
        match kont {
            storage::BlockSerializerContinue::Done(block_bytes) =>
                return (block_ref, block_bytes.freeze()),
            storage::BlockSerializerContinue::More(serializer) => {
                let (maybe_jump_ref, key, value) = items_iter.next().unwrap();
                let value_cell = storage::ValueCell {
                    version: 0,
                    cell: storage::Cell::Value(
                        storage::ValueRef::Inline(value.as_bytes()),
                    ),
                };
                let entry = storage::Entry {
                    jump_ref: storage::JumpRef::from_maybe_block_ref(
                        &maybe_jump_ref,
                        &blockwheel_filename,
                    ),
                    key: key.as_bytes(),
                    value_cell,
                };
                kont = serializer.entry(entry).unwrap();
            },
        }
    }
}
