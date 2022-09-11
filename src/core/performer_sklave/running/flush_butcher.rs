use std::{
    sync::{
        Arc,
    },
    collections::{
        HashMap,
        hash_map::{
            Entry,
        },
    },
};

use o1::{
    set::{
        Ref,
    },
};

use alloc_pool::{
    pool,
    bytes::{
        BytesPool,
    },
    Unique,
};

use arbeitssklave::{
    komm,
};

use crate::{
    kv,
    job,
    wheels,
    storage,
    core::{
        performer_sklave,
        search_tree_builder,
        MemCache,
        BlockRef,
        SearchTreeBuilderCps,
        SearchTreeBuilderKont,
        SearchTreeBuilderBlockNext,
        SearchTreeBuilderBlockEntry,
    },
    AccessPolicy,
};

pub enum Order {
    Terminate,
    WriteBlock(OrderWriteBlock),
}

pub struct OrderWriteBlock {
    pub write_block_result: Result<blockwheel_fs::block::Id, blockwheel_fs::RequestWriteBlockError>,
    pub target: WriteBlockTarget,
}

#[derive(Debug)]
pub enum WriteBlockTarget {
    WriteBlock {
        async_ref: Ref,
        blockwheel_filename: wheels::WheelFilename,
    },
    WriteValue {
        async_ref: Ref,
        block_entry_index: usize,
        blockwheel_filename: wheels::WheelFilename,
    },
}

pub struct Welt<A> where A: AccessPolicy {
    kont: Option<Kont>,
    meister_ref: o1::set::Ref,
    sendegeraet: komm::Sendegeraet<performer_sklave::Order<A>>,
    wheels: wheels::Wheels<A>,
    blocks_pool: BytesPool,
    block_entries_pool: pool::Pool<Vec<SearchTreeBuilderBlockEntry>>,
    search_tree_builder_params: search_tree_builder::Params,
    values_inline_size_limit: usize,
    incoming_orders: Vec<Order>,
    value_writes_pending: HashMap<Ref, ValueWritePending>,
    maybe_feedback: Option<komm::Rueckkopplung<performer_sklave::Order<A>, performer_sklave::FlushButcherDrop>>,
}

impl<A> Welt<A> where A: AccessPolicy {
    pub fn new(
        frozen_memcache: Arc<MemCache>,
        tree_block_size: usize,
        values_inline_size_limit: usize,
        meister_ref: Ref,
        sendegeraet: komm::Sendegeraet<performer_sklave::Order<A>>,
        wheels: wheels::Wheels<A>,
        blocks_pool: BytesPool,
        block_entries_pool: pool::Pool<Vec<SearchTreeBuilderBlockEntry>>,
        feedback: komm::Rueckkopplung<performer_sklave::Order<A>, performer_sklave::FlushButcherDrop>,
    )
        -> Self
    {
        Welt {
            search_tree_builder_params: search_tree_builder::Params {
                tree_items_count: frozen_memcache.len(),
                tree_block_size: tree_block_size,
            },
            values_inline_size_limit,
            kont: Some(Kont::Start { frozen_memcache, }),
            meister_ref,
            sendegeraet,
            wheels,
            blocks_pool,
            block_entries_pool,
            incoming_orders: Vec::new(),
            value_writes_pending: HashMap::new(),
            maybe_feedback: Some(feedback),
        }
    }
}

struct ValueWritePending {
    node_type: storage::NodeType,
    block_entries: Unique<Vec<SearchTreeBuilderBlockEntry>>,
    pending_count: usize,
}

pub type Meister<A> = arbeitssklave::Meister<Welt<A>, Order>;
pub type SklaveJob<A> = arbeitssklave::SklaveJob<Welt<A>, Order>;

pub enum Kont {
    Start { frozen_memcache: Arc<MemCache>, },
    Continue { next: SearchTreeBuilderBlockNext, },
    Finished { root_block_ref: BlockRef, },
}

#[derive(Debug)]
enum Error {
    OrphanSklave(arbeitssklave::Error),
    SearchTreeBuilder(search_tree_builder::Error),
    SerializeBlockStorage(storage::Error),
    SerializeValueBlockStorage(storage::Error),
    BlockWriteWriteBlockRequest(arbeitssklave::Error),
    BlockWriteWriteValueRequest(arbeitssklave::Error),
    WriteBlock(blockwheel_fs::RequestWriteBlockError),
    FeedbackCommit(komm::Error),
}

pub fn run_job<A, P>(sklave_job: SklaveJob<A>, thread_pool: &P)
where A: AccessPolicy,
      P: edeltraud::ThreadPool<job::Job<A>>,
{
    if let Err(error) = job(sklave_job, thread_pool) {
        log::error!("terminated with an error: {error:?}");
    }
}

fn job<A, P>(mut sklave_job: SklaveJob<A>, thread_pool: &P) -> Result<(), Error>
where A: AccessPolicy,
      P: edeltraud::ThreadPool<job::Job<A>>,
{
    'outer: loop {
        // first retrieve all orders available
        if let Some(Kont::Start { .. }) = sklave_job.sklavenwelt().kont {
            // skip it on initialize
        } else {
            let gehorsam = sklave_job.zu_ihren_diensten()
                .map_err(Error::OrphanSklave)?;
            match gehorsam {
                arbeitssklave::Gehorsam::Rasten =>
                    return Ok(()),
                arbeitssklave::Gehorsam::Machen { mut befehle, } =>
                    loop {
                        match befehle.befehl() {
                            arbeitssklave::SklavenBefehl::Mehr { befehl, mut mehr_befehle, } => {
                                mehr_befehle
                                    .sklavenwelt_mut()
                                    .incoming_orders.push(befehl);
                                befehle = mehr_befehle;
                            },
                            arbeitssklave::SklavenBefehl::Ende { sklave_job: next_sklave_job, } => {
                                sklave_job = next_sklave_job;
                                break;
                            },
                        }
                    },
            }
        }

        loop {
            let sklavenwelt = sklave_job.sklavenwelt_mut();
            match sklavenwelt.kont.take().unwrap() {

                Kont::Start { frozen_memcache, } => {
                    let builder = search_tree_builder::BuilderCps::new(
                        sklavenwelt.block_entries_pool.clone(),
                        sklavenwelt.search_tree_builder_params,
                    );
                    sklavenwelt.kont =
                        Some(init_build(sklavenwelt, frozen_memcache, builder, thread_pool)?);
                },

                Kont::Continue { next, } =>
                    loop {
                        match sklavenwelt.incoming_orders.pop() {
                            None => {
                                sklavenwelt.kont = Some(Kont::Continue { next, });
                                continue 'outer;
                            },
                            Some(Order::Terminate) =>
                                return Ok(()),
                            Some(Order::WriteBlock(OrderWriteBlock {
                                write_block_result: Ok(block_id),
                                target: WriteBlockTarget::WriteBlock { async_ref, blockwheel_filename, },
                            })) => {
                                let block_ref = BlockRef { blockwheel_filename, block_id, };
                                let builder_kont = next.block_processed(async_ref, block_ref)
                                    .map_err(Error::SearchTreeBuilder)?;
                                sklavenwelt.kont =
                                    Some(proceed_build(sklavenwelt, builder_kont, thread_pool)?);
                                break;
                            },
                            Some(Order::WriteBlock(OrderWriteBlock {
                                write_block_result: Ok(block_id),
                                target: WriteBlockTarget::WriteValue { async_ref, block_entry_index, blockwheel_filename, },
                            })) => {
                                let block_ref = BlockRef { blockwheel_filename, block_id, };
                                match sklavenwelt.value_writes_pending.entry(async_ref) {
                                    Entry::Occupied(mut oe) => {
                                        let value_write_pending = oe.get_mut();
                                        assert!(value_write_pending.pending_count > 0);
                                        value_write_pending.pending_count -= 1;
                                        let block_entry = &mut value_write_pending
                                            .block_entries[block_entry_index];
                                        match &mut block_entry.item.value_cell.cell {
                                            value @ kv::Cell::Value(storage::OwnedValueBlockRef::Inline(..)) =>
                                                *value = kv::Cell::Value(storage::OwnedValueBlockRef::Ref(block_ref)),
                                            kv::Cell::Value(storage::OwnedValueBlockRef::Ref(..)) =>
                                                unreachable!(),
                                            kv::Cell::Tombstone =>
                                                unreachable!(),
                                        }

                                        if value_write_pending.pending_count == 0 {
                                            let (_, value_write_pending) = oe.remove_entry();
                                            serialize_block(
                                                sklavenwelt,
                                                value_write_pending.node_type,
                                                async_ref,
                                                value_write_pending.block_entries,
                                                thread_pool,
                                            )?;
                                        }
                                    },
                                    Entry::Vacant(..) =>
                                        unreachable!(),
                                }
                            },
                            Some(Order::WriteBlock(OrderWriteBlock { write_block_result: Err(error), .. })) =>
                                return Err(Error::WriteBlock(error)),
                        }
                    },

                Kont::Finished { root_block_ref: root_block, } => {
                    if let Some(feedback) = sklavenwelt.maybe_feedback.take() {
                        feedback.commit(performer_sklave::FlushButcherDone { root_block, })
                            .map_err(Error::FeedbackCommit)?;
                    }
                    return Ok(());
                },

            }
        }
    }
}

fn init_build<A, P>(
    sklavenwelt: &mut Welt<A>,
    frozen_memcache: Arc<MemCache>,
    builder: SearchTreeBuilderCps,
    thread_pool: &P,
)
    -> Result<Kont, Error>
where A: AccessPolicy,
      P: edeltraud::ThreadPool<job::Job<A>>,
{
    let mut memcache_iter = frozen_memcache.iter();

    let mut builder_kont = builder.step()
        .map_err(Error::SearchTreeBuilder)?;
    loop {
        match builder_kont {
            search_tree_builder::Kont::PollNextItemOrProcessedBlock(
                search_tree_builder::KontPollNextItemOrProcessedBlock { next, },
            ) => {
                let (ord_key, value_cell) = memcache_iter.next().unwrap();
                let item = kv::KeyValuePair {
                    key: ord_key.as_ref().clone(),
                    value_cell: value_cell.clone().into(),
                };
                builder_kont = next.item_arrived(item)
                    .map_err(Error::SearchTreeBuilder)?;
            },
            search_tree_builder::Kont::PollProcessedBlock(
                search_tree_builder::KontPollProcessedBlock { next, },
            ) => {
                assert!(memcache_iter.next().is_none());
                return Ok(Kont::Continue { next, });
            }
            search_tree_builder::Kont::ProcessBlockAsync(
                search_tree_builder::KontProcessBlockAsync { node_type, block_entries, async_ref, next, },
            ) => {
                process_ready_block(sklavenwelt, node_type, block_entries, async_ref, thread_pool)?;
                builder_kont = next.process_scheduled()
                    .map_err(Error::SearchTreeBuilder)?;
            },
            search_tree_builder::Kont::Finished { .. } =>
                unreachable!("totally unexpected Kont::Finished during search tree building in butcher flush"),
        }
    }
}

fn proceed_build<A, P>(
    sklavenwelt: &mut Welt<A>,
    mut builder_kont: SearchTreeBuilderKont,
    thread_pool: &P,
)
    -> Result<Kont, Error>
where A: AccessPolicy,
      P: edeltraud::ThreadPool<job::Job<A>>,
{
    loop {
        match builder_kont {
            search_tree_builder::Kont::PollNextItemOrProcessedBlock(..) =>
                unreachable!("totally unexpected Kont::PollNextItemOrProcessedBlock during search tree writing"),
            search_tree_builder::Kont::PollProcessedBlock(
                search_tree_builder::KontPollProcessedBlock { next, },
            ) =>
                return Ok(Kont::Continue { next, }),
            search_tree_builder::Kont::ProcessBlockAsync(
                search_tree_builder::KontProcessBlockAsync { node_type, block_entries, async_ref, next, },
            ) => {
                process_ready_block(sklavenwelt, node_type, block_entries, async_ref, thread_pool)?;
                builder_kont = next.process_scheduled()
                    .map_err(Error::SearchTreeBuilder)?;
            },
            search_tree_builder::Kont::Finished { root_block_ref, .. } =>
                return Ok(Kont::Finished { root_block_ref, }),
        }
    }
}

fn process_ready_block<A, P>(
    sklavenwelt: &mut Welt<A>,
    node_type: storage::NodeType,
    block_entries: Unique<Vec<SearchTreeBuilderBlockEntry>>,
    async_ref: Ref,
    thread_pool: &P,
)
    -> Result<(), Error>
where A: AccessPolicy,
      P: edeltraud::ThreadPool<job::Job<A>>,
{
    let mut values_write_pending = 0;
    for (block_entry_index, block_entry) in block_entries.iter().enumerate() {
        match &block_entry.item.value_cell.cell {
            kv::Cell::Value(storage::OwnedValueBlockRef::Inline(
                kv::Value { ref value_bytes, },
            )) if value_bytes.len() > sklavenwelt.values_inline_size_limit => {
                let mut block_bytes = sklavenwelt.blocks_pool.lend();
                storage::value_block_serialize(value_bytes, &mut block_bytes)
                    .map_err(Error::SerializeValueBlockStorage)?;

                let wheel_ref = sklavenwelt.wheels.acquire();
                let rueckkopplung = sklavenwelt
                    .sendegeraet
                    .rueckkopplung(
                        performer_sklave::WheelRouteWriteBlock::FlushButcher {
                            route: performer_sklave::FlushButcherRoute {
                                meister_ref: sklavenwelt.meister_ref,
                            },
                            target: WriteBlockTarget::WriteValue {
                                async_ref,
                                block_entry_index,
                                blockwheel_filename: wheel_ref
                                    .blockwheel_filename
                                    .clone(),
                            },
                        },
                    );
                wheel_ref.meister
                    .write_block(
                        block_bytes.freeze(),
                        rueckkopplung,
                        &edeltraud::ThreadPoolMap::new(thread_pool),
                    )
                    .map_err(Error::BlockWriteWriteValueRequest)?;
                values_write_pending += 1;
            },
            kv::Cell::Value(..) | kv::Cell::Tombstone =>
                (),
        }
    }

    if values_write_pending == 0 {
        serialize_block(sklavenwelt, node_type, async_ref, block_entries, thread_pool)
    } else {
        sklavenwelt.value_writes_pending.insert(
            async_ref,
            ValueWritePending {
                node_type,
                block_entries,
                pending_count: values_write_pending,
            },
        );
        Ok(())
    }
}

fn serialize_block<A, P>(
    sklavenwelt: &mut Welt<A>,
    node_type: storage::NodeType,
    async_ref: Ref,
    mut block_entries: Unique<Vec<SearchTreeBuilderBlockEntry>>,
    thread_pool: &P,
)
    -> Result<(), Error>
where A: AccessPolicy,
      P: edeltraud::ThreadPool<job::Job<A>>,
{
    let wheel_ref = sklavenwelt.wheels.acquire();
    let block_bytes = sklavenwelt.blocks_pool.lend();
    let mut kont = storage::BlockSerializer::start(node_type, block_entries.len(), block_bytes)
        .map_err(Error::SerializeBlockStorage)?;
    let mut block_entries_iter = block_entries.drain(..);
    let block_bytes = loop {
        match kont {
            storage::BlockSerializerContinue::Done(block_bytes) =>
                break block_bytes.freeze(),
            storage::BlockSerializerContinue::More(serializer) =>
                match block_entries_iter.next() {
                    Some(block_entry) => {
                        let ref value_ref_cell = block_entry.item.value_cell
                            .into_owned_value_ref(&wheel_ref.blockwheel_filename);
                        let entry = storage::Entry {
                            jump_ref: storage::JumpRef::from_maybe_block_ref(
                                &block_entry.child_block_ref,
                                &wheel_ref.blockwheel_filename,
                            ),
                            key: &block_entry.item.key.key_bytes,
                            value_cell: value_ref_cell.into(),
                        };
                        kont = serializer.entry(entry)
                            .map_err(Error::SerializeBlockStorage)?;
                    },
                    _ =>
                        unreachable!(),
                },
        }
    };

    let rueckkopplung = sklavenwelt
        .sendegeraet
        .rueckkopplung(
            performer_sklave::WheelRouteWriteBlock::FlushButcher {
                route: performer_sklave::FlushButcherRoute {
                    meister_ref: sklavenwelt.meister_ref,
                },
                target: WriteBlockTarget::WriteBlock {
                    async_ref,
                    blockwheel_filename: wheel_ref
                        .blockwheel_filename
                        .clone(),
                },
            },
        );
    wheel_ref.meister
        .write_block(
            block_bytes,
            rueckkopplung,
            &edeltraud::ThreadPoolMap::new(thread_pool),
        )
        .map_err(Error::BlockWriteWriteBlockRequest)?;

    Ok(())
}
