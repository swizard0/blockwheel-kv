use super::{
    wheels::{
        WheelFilename,
    },
    blockwheel::{
        block,
    },
};

pub mod manager;
pub mod butcher;
pub mod search_tree;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct BlockRef {
    blockwheel_filename: WheelFilename,
    block_id: block::Id,
}
