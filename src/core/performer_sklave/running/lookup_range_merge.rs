use alloc_pool::{
    bytes::{
        Bytes,
    },
};

use alloc_pool_pack::{
    SourceBytesRef,
    ReadFromSource,
};

use arbeitssklave::{
    komm::{
        self,
        Echo,
    },
};

use crate::{
    kv,
    wheels,
    storage,
    core::{
        performer,
        performer_sklave,
        search_ranges_merge,
        FsReadBlock,
        SearchRangesMergeCps,
        SearchRangesMergeBlockNext,
        SearchRangesMergeItemNext,
    },
    HideDebug,
    EchoPolicy,
};

pub enum Order<E> where E: EchoPolicy {
    ReadBlock(OrderReadBlock),
    ReadBlockCancel(komm::UmschlagAbbrechen<ReadBlockTarget>),
    LookupRangeFirst(komm::StreamStarten<OrderItemFirst<E>>),
    LookupRangeNext(komm::StreamMehr<OrderItemNext<E>>),
    LookupRangeCancel(komm::StreamAbbrechen),
}

pub struct OrderReadBlock {
    pub read_block_result: Result<Bytes, blockwheel_fs::RequestReadBlockError>,
    pub target: ReadBlockTarget,
}

pub struct OrderItemFirst<E> where E: EchoPolicy {
    pub stream_echo: E::LookupRange,
}

pub struct OrderItemNext<E> where E: EchoPolicy {
    pub stream_echo: E::LookupRange,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum ReadBlockTarget {
    LoadValue,
    LoadBlock(ReadBlockTargetLoadBlock),
}

#[derive(Debug)]
pub struct ReadBlockTargetLoadBlock {
    async_token: HideDebug<search_ranges_merge::AsyncToken<performer::LookupRangeSource>>,
}

#[allow(dead_code)]
pub struct Welt<E> where E: EchoPolicy {
    kont: Option<Kont<E>>,
    sendegeraet: komm::Sendegeraet<Order<E>>,
    wheels: wheels::Wheels<E>,
    _drop_bomb: komm::Rueckkopplung<performer_sklave::Order<E>, performer_sklave::LookupRangeMergeDrop>,
    received_stream_start: Option<komm::StreamStarten<OrderItemFirst<E>>>,
    received_stream_next: Option<komm::StreamMehr<OrderItemNext<E>>>,
    received_block_tasks: Vec<ReceivedBlockTask>,
    received_value_bytes: Option<Bytes>,
}

impl<E> Welt<E> where E: EchoPolicy {
    pub fn new(
        merger: SearchRangesMergeCps,
        sendegeraet: komm::Sendegeraet<Order<E>>,
        wheels: wheels::Wheels<E>,
        drop_bomb: komm::Rueckkopplung<performer_sklave::Order<E>, performer_sklave::LookupRangeMergeDrop>,
    )
        -> Self
    {
        Welt {
            kont: Some(Kont::WaitStart { merger, }),
            sendegeraet,
            wheels,
            _drop_bomb: drop_bomb,
            received_stream_start: None,
            received_stream_next: None,
            received_block_tasks: Vec::new(),
            received_value_bytes: None,
        }
    }
}

pub type SklaveJob<E> = arbeitssklave::SklaveJob<Welt<E>, Order<E>>;

enum Kont<E> where E: EchoPolicy {
    WaitStart {
        merger: SearchRangesMergeCps,
    },
    AwaitBlocks {
        stream_echo: E::LookupRange,
        stream_token: komm::StreamToken,
        next: SearchRangesMergeBlockNext,
    },
    ReadyItem {
        key_value_pair: kv::KeyValuePair<kv::Value>,
        stream_echo: E::LookupRange,
        stream_token: komm::StreamToken,
        next: SearchRangesMergeItemNext,
    },
    AwaitItemNext {
        next: SearchRangesMergeItemNext,
    },
    AwaitItemValue {
        key: kv::Key,
        version: u64,
        stream_echo: E::LookupRange,
        stream_token: komm::StreamToken,
        next: SearchRangesMergeItemNext,
    },
}

#[derive(Debug)]
pub enum Error {
    OrphanSklave(arbeitssklave::Error),
    SearchRangesMerge(search_ranges_merge::Error),
    SendegeraetGone(komm::EchoError),
    WheelNotFound {
        blockwheel_filename: wheels::WheelFilename,
    },
    ValueLoadReadBlockRequest(blockwheel_fs::Error),
    BlockLoadReadBlockRequest(blockwheel_fs::Error),
    LoadBlock(blockwheel_fs::RequestReadBlockError),
    LoadValue(blockwheel_fs::RequestReadBlockError),
    ValueDeserialize(storage::ReadValueBlockError),
    WheelIsGoneDuringReadBlock,
}

pub fn run_job<E, J>(sklave_job: SklaveJob<E>, thread_pool: &edeltraud::Handle<J>)
where E: EchoPolicy,
      J: From<blockwheel_fs::job::SklaveJob<wheels::WheelEchoPolicy<E>>>,
{
    if let Err(error) = job(sklave_job, thread_pool) {
        log::error!("terminated with an error: {error:?}");
    }
}

fn job<E, J>(mut sklave_job: SklaveJob<E>, thread_pool: &edeltraud::Handle<J>) -> Result<(), Error>
where E: EchoPolicy,
      J: From<blockwheel_fs::job::SklaveJob<wheels::WheelEchoPolicy<E>>>,
{
    'outer: loop {
        // first retrieve all orders available
        let gehorsam = sklave_job.zu_ihren_diensten()
            .map_err(Error::OrphanSklave)?;
        match gehorsam {
            arbeitssklave::Gehorsam::Rasten =>
                return Ok(()),
            arbeitssklave::Gehorsam::Machen { mut befehle, } =>
                loop {
                    match befehle.befehl() {
                        arbeitssklave::SklavenBefehl::Mehr { befehl, mehr_befehle, } => {
                            befehle = mehr_befehle;
                            let sklavenwelt = &mut *befehle;

                            match befehl {
                                Order::LookupRangeFirst(order) => {
                                    assert!(sklavenwelt.received_stream_start.is_none());
                                    sklavenwelt.received_stream_start = Some(order);
                                },
                                Order::LookupRangeNext(order) => {
                                    assert!(sklavenwelt.received_stream_next.is_none());
                                    sklavenwelt.received_stream_next = Some(order);
                                },
                                Order::LookupRangeCancel(komm::StreamAbbrechen { stream_id, }) => {
                                    log::debug!("stream id = {stream_id:?} cancelled, terminating");
                                    return Ok(());
                                },
                                Order::ReadBlock(OrderReadBlock {
                                    read_block_result: Ok(block_bytes),
                                    target: ReadBlockTarget::LoadBlock(ReadBlockTargetLoadBlock {
                                        async_token: HideDebug(async_token),
                                    }),
                                }) =>
                                    sklavenwelt.received_block_tasks
                                    .push(ReceivedBlockTask { async_token, block_bytes, }),
                                Order::ReadBlock(OrderReadBlock {
                                    read_block_result: Ok(block_bytes),
                                    target: ReadBlockTarget::LoadValue,
                                }) => {
                                    let value_bytes =
                                        storage::ValueBlock::read_from_source(
                                            &mut SourceBytesRef::from(&block_bytes),
                                        )
                                        .map_err(Error::ValueDeserialize)?
                                        .into();
                                    assert!(sklavenwelt.received_value_bytes.is_none());
                                    sklavenwelt.received_value_bytes =
                                        Some(value_bytes);
                                },
                                Order::ReadBlock(OrderReadBlock {
                                    read_block_result: Err(error),
                                    target: ReadBlockTarget::LoadBlock { .. },
                                }) =>
                                    return Err(Error::LoadBlock(error)),
                                Order::ReadBlock(OrderReadBlock {
                                    read_block_result: Err(error),
                                    target: ReadBlockTarget::LoadValue,
                                }) =>
                                    return Err(Error::LoadValue(error)),
                                Order::ReadBlockCancel(..) =>
                                    return Err(Error::WheelIsGoneDuringReadBlock),
                            }
                        },
                        arbeitssklave::SklavenBefehl::Ende { sklave_job: next_sklave_job, } => {
                            sklave_job = next_sklave_job;
                            break;
                        },
                    }
                },
        }

        let sklavenwelt = &mut *sklave_job;

        'kont: loop {
            let (
                mut merger_kont,
                stream_echo,
                stream_token,
            ) = match sklavenwelt.kont.take().unwrap() {
                Kont::WaitStart { merger, } =>
                    match sklavenwelt.received_stream_start.take() {
                        Some(komm::StreamStarten {
                            inhalt: OrderItemFirst {
                                stream_echo,
                            },
                            stream_token,
                        }) => (
                            merger.step()
                                .map_err(Error::SearchRangesMerge)?,
                            stream_echo,
                            stream_token,
                        ),
                        None => {
                            sklavenwelt.kont = Some(Kont::WaitStart { merger, });
                            continue 'outer;
                        },
                    },
                Kont::AwaitBlocks {
                    stream_echo,
                    stream_token,
                    next,
                } =>
                    match sklavenwelt.received_block_tasks.pop() {
                        Some(ReceivedBlockTask {
                            async_token,
                            block_bytes,
                        }) => (
                            next.block_arrived(async_token, block_bytes)
                                .map_err(Error::SearchRangesMerge)?,
                            stream_echo,
                            stream_token,
                        ),
                        None => {
                            sklavenwelt.kont = Some(Kont::AwaitBlocks { stream_echo, stream_token, next, });
                            continue 'outer;
                        },
                    },
                Kont::ReadyItem {
                    key_value_pair,
                    stream_echo,
                    stream_token,
                    next,
                } => {
                    let streamzeug = stream_token.streamzeug_zeug(key_value_pair);
                    stream_echo.commit_echo(streamzeug)
                        .map_err(Error::SendegeraetGone)?;
                    sklavenwelt.kont =
                        Some(Kont::AwaitItemNext { next, });
                    continue 'kont;
                },
                Kont::AwaitItemNext { next, } =>
                    match sklavenwelt.received_stream_next.take() {
                        Some(komm::StreamMehr {
                            inhalt: OrderItemNext {
                                stream_echo,
                            },
                            stream_token,
                        }) => {
                            let merger_kont = next.proceed()
                                .map_err(Error::SearchRangesMerge)?;
                            (merger_kont, stream_echo, stream_token)
                        },
                        None => {
                            sklavenwelt.kont = Some(Kont::AwaitItemNext { next, });
                            continue 'outer;
                        },
                    },
                Kont::AwaitItemValue {
                    key,
                    version,
                    stream_echo,
                    stream_token,
                    next,
                } =>
                    match sklavenwelt.received_value_bytes.take() {
                        Some(value_bytes) => {
                            sklavenwelt.kont =
                                Some(Kont::ReadyItem {
                                    key_value_pair: kv::KeyValuePair {
                                        key,
                                        value_cell: kv::ValueCell {
                                            version,
                                            cell: kv::Cell::Value(kv::Value { value_bytes, }),
                                        },
                                    },
                                    stream_echo,
                                    stream_token,
                                    next,
                                });
                            continue 'kont;
                        },
                        None => {
                            sklavenwelt.kont = Some(Kont::AwaitItemValue { key, version, stream_echo, stream_token, next, });
                            continue 'outer;
                        },
                    },
            };

            loop {
                match merger_kont {
                    search_ranges_merge::Kont::RequireBlockAsync(
                        search_ranges_merge::KontRequireBlockAsync {
                            block_ref,
                            async_token,
                            next,
                        },
                    ) => {
                        let wheel_ref = sklavenwelt.wheels.get(&block_ref.blockwheel_filename)
                            .ok_or_else(|| Error::WheelNotFound {
                                blockwheel_filename: block_ref.blockwheel_filename.clone(),
                            })?;
                        let rueckkopplung = sklavenwelt
                            .sendegeraet
                            .rueckkopplung(ReadBlockTarget::LoadBlock(ReadBlockTargetLoadBlock {
                                async_token: HideDebug(async_token),
                            }));
                        wheel_ref.meister
                            .read_block(
                                block_ref.block_id,
                                FsReadBlock::LookupRangeMerge { rueckkopplung, },
                                thread_pool,
                            )
                            .map_err(Error::BlockLoadReadBlockRequest)?;
                        merger_kont = next.scheduled()
                            .map_err(Error::SearchRangesMerge)?;
                    },
                    search_ranges_merge::Kont::AwaitBlocks(search_ranges_merge::KontAwaitBlocks {
                        next,
                    }) => {
                        sklavenwelt.kont =
                            Some(Kont::AwaitBlocks { stream_echo, stream_token, next, });
                        break;
                    },
                    search_ranges_merge::Kont::BlockFinished(search_ranges_merge::KontBlockFinished {
                        next,
                        ..
                    }) => {
                        merger_kont = next.proceed()
                            .map_err(Error::SearchRangesMerge)?;
                    },
                    search_ranges_merge::Kont::EmitDeprecated(search_ranges_merge::KontEmitDeprecated {
                        next,
                        ..
                    }) => {
                        merger_kont = next.proceed()
                            .map_err(Error::SearchRangesMerge)?;
                    },
                    search_ranges_merge::Kont::EmitItem(
                        search_ranges_merge::KontEmitItem {
                            item,
                            next,
                        },
                    ) => {
                        match item {
                            kv::KeyValuePair {
                                key,
                                value_cell: kv::ValueCell {
                                    version,
                                    cell: kv::Cell::Value(storage::ValueRef::Inline(value_bytes)),
                                },
                            } => {
                                sklavenwelt.kont = Some(Kont::ReadyItem {
                                    key_value_pair: kv::KeyValuePair {
                                        key,
                                        value_cell: kv::ValueCell {
                                            version,
                                            cell: kv::Cell::Value(kv::Value { value_bytes, }),
                                        },
                                    },
                                    stream_echo,
                                    stream_token,
                                    next,
                                });
                            },
                            kv::KeyValuePair { key, value_cell: kv::ValueCell { version, cell: kv::Cell::Tombstone, }, } => {
                                sklavenwelt.kont = Some(Kont::ReadyItem {
                                    key_value_pair: kv::KeyValuePair {
                                        key,
                                        value_cell: kv::ValueCell {
                                            version,
                                            cell: kv::Cell::Tombstone,
                                        },
                                    },
                                    stream_echo,
                                    stream_token,
                                    next,
                                });
                            },
                            kv::KeyValuePair {
                                key,
                                value_cell: kv::ValueCell {
                                    version,
                                    cell: kv::Cell::Value(storage::ValueRef::External(block_ref)),
                                },
                            } => {
                                let wheel_ref = sklavenwelt.wheels.get(&block_ref.blockwheel_filename)
                                    .ok_or_else(|| Error::WheelNotFound {
                                        blockwheel_filename: block_ref.blockwheel_filename.clone(),
                                    })?;
                                let rueckkopplung = sklavenwelt
                                    .sendegeraet
                                    .rueckkopplung(ReadBlockTarget::LoadValue);
                                wheel_ref.meister
                                    .read_block(
                                        block_ref.block_id,
                                        FsReadBlock::LookupRangeMerge { rueckkopplung, },
                                        thread_pool,
                                    )
                                    .map_err(Error::ValueLoadReadBlockRequest)?;
                                sklavenwelt.kont =
                                    Some(Kont::AwaitItemValue { key, version, stream_echo, stream_token, next, });
                            },
                            kv::KeyValuePair { value_cell: kv::ValueCell { cell: kv::Cell::Value(storage::ValueRef::Local(..)), .. }, .. } =>
                                unreachable!("totally unexpected ValueRef::Local value from `search_range_merge`"),
                        }
                        break;
                    },
                    search_ranges_merge::Kont::Finished => {
                        let streamzeug = stream_token
                            .streamzeug_nicht_mehr();
                        stream_echo.commit_echo(streamzeug)
                            .map_err(Error::SendegeraetGone)?;
                        return Ok(());
                    },
                }
            }
        }
    }
}

struct ReceivedBlockTask {
    block_bytes: Bytes,
    async_token: search_ranges_merge::AsyncToken<performer::LookupRangeSource>,
}

impl<E> From<komm::UmschlagAbbrechen<ReadBlockTarget>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::UmschlagAbbrechen<ReadBlockTarget>) -> Self {
        Self::ReadBlockCancel(v)
    }
}

impl<E> From<komm::Umschlag<Result<Bytes, blockwheel_fs::RequestReadBlockError>, ReadBlockTarget>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::Umschlag<Result<Bytes, blockwheel_fs::RequestReadBlockError>, ReadBlockTarget>) -> Self {
        Self::ReadBlock(OrderReadBlock {
            read_block_result: v.inhalt,
            target: v.stamp,
        })
    }
}

impl<E> From<komm::StreamStarten<OrderItemFirst<E>>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::StreamStarten<OrderItemFirst<E>>) -> Self {
        Self::LookupRangeFirst(v)
    }
}

impl<E> From<komm::StreamMehr<OrderItemNext<E>>> for Order<E> where E: EchoPolicy {
    fn from(v: komm::StreamMehr<OrderItemNext<E>>) -> Order<E> {
        Order::LookupRangeNext(v)
    }
}

impl<E> From<komm::StreamAbbrechen> for Order<E> where E: EchoPolicy {
    fn from(v: komm::StreamAbbrechen) -> Order<E> {
        Order::LookupRangeCancel(v)
    }
}
