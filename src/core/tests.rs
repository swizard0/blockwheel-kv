use std::{
    sync::{
        Arc,
    },
    collections::{
        HashMap,
    },
};

use alloc_pool::{
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
        OrdKey,
        MemCache,
        BlockRef,
    },
};

pub const BLOCKWHEEL_FILENAME: &str = "my_filename.blockwheel";

pub fn to_bytes(s: &str) -> Bytes {
    BytesMut::new_detached(s.as_bytes().to_vec()).freeze()
}

pub fn make_memcache<'a, I>(items: I) -> Arc<MemCache> where I: IntoIterator<Item = (&'a str, &'a str, u64)> + 'a {
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

pub fn make_kinda_tree() -> (BlockRef, HashMap<BlockRef, Bytes>) {
    let mut global_block_id = blockwheel_fs::block::Id::init();

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

pub fn make_kinda_block(
    node_type: storage::NodeType,
    global_block_id: &mut blockwheel_fs::block::Id,
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
    let mut kont = storage::BlockSerializer::start(node_type, items.len(), block_bytes);
    let mut items_iter = items.into_iter();
    loop {
        match kont {
            storage::BlockSerializerContinue::Done(block_bytes) =>
                return (block_ref, block_bytes.freeze()),
            storage::BlockSerializerContinue::More(serializer) => {
                let (maybe_jump_ref, key, value) = items_iter.next().unwrap();
                let value_cell = kv::ValueCell {
                    version: 0,
                    cell: kv::Cell::Value(storage::ValueRef::Inline(to_bytes(value))),
                };
                let entry = storage::Entry {
                    jump_ref: storage::JumpRef::from_maybe_block_ref(
                        &maybe_jump_ref,
                        &blockwheel_filename,
                    ),
                    key: kv::Key { key_bytes: to_bytes(key), },
                    value_cell,
                };
                kont = serializer.entry(entry);
            },
        }
    }
}

pub fn kinda_parse_item(item: &kv::KeyValuePair<storage::ValueRef>) -> (String, Option<String>, u64) {
    (
        String::from_utf8(item.key.key_bytes.to_vec()).unwrap(),
        match &item.value_cell.cell {
            kv::Cell::Value(storage::ValueRef::Inline(value)) =>
                Some(String::from_utf8(value.to_vec()).unwrap()),
            kv::Cell::Value(storage::ValueRef::Local(..)) |
            kv::Cell::Value(storage::ValueRef::External(..)) |
            kv::Cell::Tombstone =>
                None,
        },
        item.value_cell.version,
    )
}
