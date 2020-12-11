use std::{
    fs,
    collections::{
        HashMap,
    },
};

use futures::{
    Future,
    SinkExt,
    StreamExt,
    select,
    pin_mut,
    channel::mpsc,
};

use rand::Rng;

use ero::{
    supervisor::{
        SupervisorPid,
        SupervisorGenServer,
    },
};

use alloc_pool::bytes::{
    BytesPool,
};

use super::{
    kv,
    wheels,
    version,
    storage,
    blockwheel,
};

use crate as blockwheel_kv;

#[test]
fn stress() {
    env_logger::init();

    // let runtime = tokio::runtime::Builder::new_current_thread()
    //     .build()
    //     .unwrap();

    // let limits = Limits {
    //     active_tasks: 128,
    //     actions: 512,
    //     key_size_bytes: 32,
    //     value_size_bytes: 4096,
    // };
    // let init_wheel_size_bytes = (limits.key_size_bytes + limits.value_size_bytes) * limits.actions;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let limits = Limits {
        active_tasks: 1024,
        actions: 32768,
        key_size_bytes: 32,
        value_size_bytes: 4096,
    };
    let init_wheel_size_bytes = 134217728; // (limits.key_size_bytes + limits.value_size_bytes) * limits.actions / 8;

    let kv = blockwheel_kv::Params {
        ..Default::default()
    };
    let work_block_size_bytes = (limits.key_size_bytes + limits.value_size_bytes) * kv.tree_block_size;

    let params = Params {
        wheel_a: blockwheel::Params {
            wheel_filename: "/tmp/blockwheel_kv_a_stress".into(),
            init_wheel_size_bytes,
            work_block_size_bytes,
            lru_cache_size_bytes: 0,
            defrag_parallel_tasks_limit: 8,
            ..Default::default()
        },
        wheel_b: blockwheel::Params {
            wheel_filename: "/tmp/blockwheel_kv_b_stress".into(),
            init_wheel_size_bytes,
            work_block_size_bytes,
            lru_cache_size_bytes: 0,
            defrag_parallel_tasks_limit: 8,
            ..Default::default()
        },
        kv,
    };

    let version_provider = version::Provider::from_unix_epoch_seed();
    let mut data = DataIndex {
        index: HashMap::new(),
        data: Vec::new(),
        current_version: 0,
    };
    let mut counter = Counter::default();

    fs::remove_file(&params.wheel_a.wheel_filename).ok();
    fs::remove_file(&params.wheel_b.wheel_filename).ok();
    runtime.block_on(stress_loop(params.clone(), &version_provider, &mut data, &mut counter, &limits)).unwrap();

    // next load existing wheel and repeat stress
    counter.clear();
    runtime.block_on(stress_loop(params.clone(), &version_provider, &mut data, &mut counter, &limits)).unwrap();

    fs::remove_file(&params.wheel_a.wheel_filename).ok();
    fs::remove_file(&params.wheel_b.wheel_filename).ok();
}

#[derive(Clone)]
struct Params {
    wheel_a: blockwheel::Params,
    wheel_b: blockwheel::Params,
    kv: blockwheel_kv::Params,
}

#[derive(Clone, Copy, Default, Debug)]
struct Limits {
    actions: usize,
    active_tasks: usize,
    key_size_bytes: usize,
    value_size_bytes: usize,
}

#[derive(Clone, Copy, Default, Debug)]
struct Counter {
    lookups: usize,
    lookups_range: usize,
    inserts: usize,
    removes: usize,
}

impl Counter {
    fn sum(&self) -> usize {
        self.lookups + self.lookups_range + self.inserts + self.removes
    }

    fn clear(&mut self) {
        self.lookups = 0;
        self.lookups_range = 0;
        self.inserts = 0;
        self.removes = 0;
    }
}

struct DataIndex {
    index: HashMap<kv::Key, usize>,
    data: Vec<kv::KeyValuePair>,
    current_version: u64,
}

#[derive(Debug)]
enum Error {
    GenTaskJoin(tokio::task::JoinError),
    Insert(blockwheel_kv::InsertError),
    Lookup(blockwheel_kv::LookupError),
    LookupRange(blockwheel_kv::LookupRangeError),
    Remove(blockwheel_kv::RemoveError),
    Flush(blockwheel_kv::FlushError),
    UnexpectedLookupRangeRxFinish,
    ExpectedValueNotFound {
        key: kv::Key,
        value_cell: kv::ValueCell,
    },
    UnexpectedValueFound {
        key: kv::Key,
        expected_value_cell: kv::ValueCell,
        found_value_cell: kv::ValueCell,
    },
    UnexpectedValueForLookupRange {
        key: kv::Key,
        key_value_pair: kv::KeyValuePair,
    },
    WheelAGoneDuringInfo,
    WheelBGoneDuringInfo,
    WheelsGoneDuringFlush,
    WheelsIterBlocks(wheels::IterBlocksError),
    WheelsIterBlocksRxDropped,
    Storage(storage::Error),
    BackwardIterKeyNotFound,
}

async fn stress_loop(
    params: Params,
    version_provider: &version::Provider,
    data: &mut DataIndex,
    counter: &mut Counter,
    limits: &Limits,
)
    -> Result<(), Error>
{
    let supervisor_gen_server = SupervisorGenServer::new();
    let mut supervisor_pid = supervisor_gen_server.pid();
    tokio::spawn(supervisor_gen_server.run());

    let blocks_pool = BytesPool::new();
    let blockwheel_a_filename = params.wheel_a.wheel_filename.clone().into();
    let blockwheel_b_filename = params.wheel_b.wheel_filename.clone().into();

    let wheel_a_gen_server = blockwheel::GenServer::new();
    let mut wheel_a_pid = wheel_a_gen_server.pid();
    supervisor_pid.spawn_link_permanent(
        wheel_a_gen_server.run(supervisor_pid.clone(), blocks_pool.clone(), params.wheel_a),
    );

    let wheel_b_gen_server = blockwheel::GenServer::new();
    let mut wheel_b_pid = wheel_b_gen_server.pid();
    supervisor_pid.spawn_link_permanent(
        wheel_b_gen_server.run(supervisor_pid.clone(), blocks_pool.clone(), params.wheel_b),
    );

    let wheels_gen_server = wheels::GenServer::new();
    let mut wheels_pid = wheels_gen_server.pid();
    supervisor_pid.spawn_link_permanent(
        wheels_gen_server.run(
            vec![
                wheels::WheelRef {
                    blockwheel_filename: blockwheel_a_filename,
                    blockwheel_pid: wheel_a_pid.clone(),
                },
                wheels::WheelRef {
                    blockwheel_filename: blockwheel_b_filename,
                    blockwheel_pid: wheel_b_pid.clone(),
                },
            ],
            wheels::Params::default(),
        ),
    );

    let wheel_kv_gen_server = blockwheel_kv::GenServer::new();
    let mut wheel_kv_pid = wheel_kv_gen_server.pid();
    supervisor_pid.spawn_link_permanent(
        wheel_kv_gen_server.run(
            supervisor_pid.clone(),
            blocks_pool.clone(),
            version_provider.clone(),
            wheels_pid.clone(),
            params.kv,
        ),
    );

    let mut rng = rand::thread_rng();
    let (done_tx, done_rx) = mpsc::channel(0);
    pin_mut!(done_rx);
    let mut active_tasks_counter = Counter::default();
    let mut actions_counter = 0;

    enum TaskDone {
        Lookup { key: kv::Key, found_value_cell: kv::ValueCell, version_snapshot: u64, lookup_kind: LookupKind, },
        Insert { key: kv::Key, value: kv::Value, version: u64, },
        Remove { key: kv::Key, version: u64, },
    }

    #[derive(Debug)]
    enum LookupKind { Single, Range, };

    fn spawn_task<T>(
        supervisor_pid: &mut SupervisorPid,
        mut done_tx: mpsc::Sender<Result<TaskDone, Error>>,
        task: T,
    )
    where T: Future<Output = Result<TaskDone, Error>> + Send + 'static
    {
        supervisor_pid.spawn_link_temporary(async move {
            let result = task.await;
            done_tx.send(result).await.ok();
        })
    }

    fn process(task_done: TaskDone, data: &mut DataIndex, counter: &mut Counter, active_tasks_counter: &mut Counter) -> Result<(), Error> {
        match task_done {
            TaskDone::Insert { key, value, version, } => {
                let data_cell = kv::KeyValuePair {
                    key: key.clone(),
                    value_cell: kv::ValueCell {
                        version,
                        cell: kv::Cell::Value(value.clone()),
                    },
                };
                let updated = if let Some(&offset) = data.index.get(&key) {
                    if data.data[offset].value_cell.version < data_cell.value_cell.version {
                        data.data[offset] = data_cell;
                        true
                    } else {
                        false
                    }
                } else {
                    let offset = data.data.len();
                    data.data.push(data_cell);
                    data.index.insert(key, offset);
                    true
                };
                if updated {
                    data.current_version = version;
                }
                counter.inserts += 1;
                active_tasks_counter.inserts -= 1;
            },
            TaskDone::Lookup { key, found_value_cell, version_snapshot, lookup_kind, } => {
                let &offset = data.index.get(&key).unwrap();
                let kv::KeyValuePair { value_cell: kv::ValueCell { version: version_current, cell: ref cell_current, }, .. } = data.data[offset];
                let kv::ValueCell { version: version_found, cell: ref cell_found, } = found_value_cell;
                if version_found == version_current {
                    if cell_found == cell_current {
                        // everything is up to date
                    } else {
                        // version matches, but actual values are not
                        return Err(Error::UnexpectedValueFound {
                            key,
                            expected_value_cell: data.data[offset].value_cell.clone(),
                            found_value_cell,
                        });
                    }
                } else if version_found < version_current {
                    if version_snapshot < version_current {
                        // deprecated lookup (ignoring)
                    } else {
                        // lookup started after value is actually updated, something wrong
                        return Err(Error::UnexpectedValueFound {
                            key,
                            expected_value_cell: data.data[offset].value_cell.clone(),
                            found_value_cell,
                        });
                    }
                } else {
                    unreachable!("key = {:?}, version_current = {}, version_found = {}", key, version_current, version_found);
                }
                match lookup_kind {
                    LookupKind::Single => {
                        counter.lookups += 1;
                        active_tasks_counter.lookups -= 1;
                    },
                    LookupKind::Range => {
                        counter.lookups_range += 1;
                        active_tasks_counter.lookups_range -= 1;
                    },
                }
            }
            TaskDone::Remove { key, version, } => {
                let data_cell = kv::KeyValuePair {
                    key: key.clone(),
                    value_cell: kv::ValueCell {
                        version,
                        cell: kv::Cell::Tombstone,
                    },
                };
                let &offset = data.index.get(&key).unwrap();
                data.data[offset] = data_cell;
                data.current_version = version;
                counter.removes += 1;
                active_tasks_counter.removes -= 1;
            },
        }
        Ok(())
    }

    loop {
        if actions_counter >= limits.actions {
            std::mem::drop(done_tx);

            while active_tasks_counter.sum() > 0 {
                log::debug!("terminating, waiting for {} tasks to finish | active = {:?}", active_tasks_counter.sum(), active_tasks_counter);
                process(done_rx.next().await.unwrap()?, data, counter, &mut active_tasks_counter)?;
            }
            break;
        }
        let maybe_task_result =
            if (active_tasks_counter.sum() >= limits.active_tasks) || (data.data.is_empty() && active_tasks_counter.inserts > 0) {
                Some(done_rx.next().await.unwrap())
            } else {
                select! {
                    task_result = done_rx.next() =>
                        Some(task_result.unwrap()),
                    default =>
                        None,
                }
            };
        match maybe_task_result {
            None =>
                (),
            Some(task_result) => {
                process(task_result?, data, counter, &mut active_tasks_counter)?;
                continue;
            }
        }

        // construct action and run task
        let info_a = wheel_a_pid.info().await
            .map_err(|ero::NoProcError| Error::WheelAGoneDuringInfo)?;
        let info_b = wheel_b_pid.info().await
            .map_err(|ero::NoProcError| Error::WheelBGoneDuringInfo)?;
        if data.data.is_empty() || rng.gen_range(0.0, 1.0) < 0.5 {
            // insert or remove task
            let prob_space = (info_a.wheel_size_bytes + info_b.wheel_size_bytes) as f64;
            let insert_prob_space = (info_a.bytes_free + info_b.bytes_free) as f64;
            let insert_prob = insert_prob_space / prob_space;
            let dice = rng.gen_range(0.0, 1.0);
            if data.data.is_empty() || dice < insert_prob {
                // insert task
                let mut wheel_kv_pid = wheel_kv_pid.clone();
                let blocks_pool = blocks_pool.clone();
                let key_amount = rng.gen_range(1, limits.key_size_bytes);
                let value_amount = rng.gen_range(1, limits.value_size_bytes);

                log::debug!(
                    "{}. performing INSERT with {} bytes key and {} bytes value (dice = {:.3}, prob = {:.3}) | {:?}, active = {:?}",
                    actions_counter,
                    key_amount,
                    value_amount,
                    dice,
                    insert_prob,
                    counter,
                    active_tasks_counter,
                );

                spawn_task(&mut supervisor_pid, done_tx.clone(), async move {
                    let mut key_block = blocks_pool.lend();
                    let mut value_block = blocks_pool.lend();
                    let gen_task = tokio::task::spawn_blocking(move || {
                        let mut rng = rand::thread_rng();
                        key_block.reserve(key_amount);
                        for _ in 0 .. key_amount {
                            key_block.push(rng.gen());
                        }
                        value_block.reserve(value_amount);
                        for _ in 0 .. value_amount {
                            value_block.push(rng.gen());
                        }
                        (key_block.freeze(), value_block.freeze())
                    });
                    let (key_bytes, value_bytes) = gen_task.await
                        .map_err(Error::GenTaskJoin)?;
                    let key = kv::Key { key_bytes, };
                    let value = kv::Value { value_bytes, };
                    match wheel_kv_pid.insert(key.clone(), value.clone()).await {
                        Ok(blockwheel_kv::Inserted { version, }) =>
                            Ok(TaskDone::Insert { key, value, version, }),
                        Err(error) =>
                            Err(Error::Insert(error))
                    }
                });
                active_tasks_counter.inserts += 1;
            } else {
                // remove task
                let (key, value) = loop {
                    let key_index = rng.gen_range(0, data.data.len());
                    let kv::KeyValuePair { key, value_cell, } = &data.data[key_index];
                    match &value_cell.cell {
                        kv::Cell::Value(value) =>
                            break (key, value),
                        kv::Cell::Tombstone =>
                            continue,
                    }
                };

                log::debug!(
                    "{}. performing REMOVE with {} bytes key and {} bytes value (dice = {:.3}, prob = {:.3}) | {:?}, active = {:?}",
                    actions_counter,
                    key.key_bytes.len(),
                    value.value_bytes.len(),
                    dice,
                    1.0 - insert_prob,
                    counter,
                    active_tasks_counter,
                );

                let key = key.clone();
                let mut wheel_kv_pid = wheel_kv_pid.clone();
                spawn_task(&mut supervisor_pid, done_tx.clone(), async move {
                    match wheel_kv_pid.remove(key.clone()).await {
                        Ok(blockwheel_kv::Removed { version, }) =>
                            Ok(TaskDone::Remove { key, version, }),
                        Err(error) =>
                            Err(Error::Remove(error))
                    }
                });
                active_tasks_counter.removes += 1;
            }
        } else {
            // lookup task
            let key_index = rng.gen_range(0, data.data.len());
            let kv::KeyValuePair { key, value_cell, } = &data.data[key_index];
            let version_snapshot = data.current_version;

            let lookup_kind = if rng.gen_range(0.0, 1.0) < 0.5 {
                LookupKind::Single
            } else {
                LookupKind::Range
            };

            log::debug!(
                "{}. performing {:?} LOOKUP with {} bytes key and {} value | {:?}, active = {:?}",
                actions_counter,
                lookup_kind,
                key.key_bytes.len(),
                match &value_cell.cell {
                    kv::Cell::Value(value) =>
                        format!("{} bytes", value.value_bytes.len()),
                    kv::Cell::Tombstone =>
                        "tombstone".to_string(),
                },
                counter,
                active_tasks_counter,
            );

            let key = key.clone();
            let value_cell = value_cell.clone();
            let mut wheel_kv_pid = wheel_kv_pid.clone();

            match lookup_kind {
                LookupKind::Single => {
                    spawn_task(&mut supervisor_pid, done_tx.clone(), async move {
                        match wheel_kv_pid.lookup(key.clone()).await {
                            Ok(None) =>
                                Err(Error::ExpectedValueNotFound { key, value_cell, }),
                            Ok(Some(found_value_cell)) =>
                                Ok(TaskDone::Lookup { key, found_value_cell, version_snapshot, lookup_kind: LookupKind::Single, }),
                            Err(error) =>
                                Err(Error::Lookup(error))
                        }
                    });
                    active_tasks_counter.lookups += 1;
                },
                LookupKind::Range => {
                    spawn_task(&mut supervisor_pid, done_tx.clone(), async move {
                        let mut lookup_range = wheel_kv_pid.lookup_range(key.clone() ..= key.clone()).await
                            .map_err(Error::LookupRange)?;
                        let result = match lookup_range.key_values_rx.next().await {
                            None =>
                                return Err(Error::UnexpectedLookupRangeRxFinish),
                            Some(blockwheel_kv::KeyValueStreamItem::KeyValue(key_value_pair)) =>
                                TaskDone::Lookup {
                                    key: key.clone(),
                                    found_value_cell: key_value_pair.value_cell,
                                    version_snapshot,
                                    lookup_kind: LookupKind::Range,
                                },
                            Some(blockwheel_kv::KeyValueStreamItem::NoMore) =>
                                return Err(Error::ExpectedValueNotFound { key, value_cell, }),
                        };
                        match lookup_range.key_values_rx.next().await {
                            None =>
                                return Err(Error::UnexpectedLookupRangeRxFinish),
                            Some(blockwheel_kv::KeyValueStreamItem::KeyValue(key_value_pair)) =>
                                return Err(Error::UnexpectedValueForLookupRange { key, key_value_pair, }),
                            Some(blockwheel_kv::KeyValueStreamItem::NoMore) =>
                                ()
                        }
                        assert!(lookup_range.key_values_rx.next().await.is_none());
                        Ok(result)
                    });
                    active_tasks_counter.lookups_range += 1;
                },
            }
        }
        actions_counter += 1;
    }

    assert!(done_rx.next().await.is_none());

    let blockwheel_kv::Flushed = wheel_kv_pid.flush().await
        .map_err(Error::Flush)?;
    let wheels::Flushed = wheels_pid.flush().await
        .map_err(|ero::NoProcError| Error::WheelsGoneDuringFlush)?;

    // backwards check with iterator
    let mut checked_blocks = 0;
    let mut checked_entries = 0;
    let mut iter_blocks = wheels_pid.iter_blocks().await
        .map_err(Error::WheelsIterBlocks)?;
    loop {
        match iter_blocks.block_refs_rx.next().await {
            None =>
                return Err(Error::WheelsIterBlocksRxDropped),
            Some(wheels::IterBlocksItem::Block { block_bytes, .. }) => {
                checked_blocks += 1;
                let deserializer = storage::block_deserialize_iter(&block_bytes)
                    .map_err(Error::Storage)?;
                for maybe_entry in deserializer {
                    let entry = maybe_entry
                        .map_err(Error::Storage)?;
                    checked_entries += 1;
                    match data.index.get(entry.key) {
                        None =>
                            return Err(Error::BackwardIterKeyNotFound),
                        Some(..) =>
                            (),
                    }
                }
            },
            Some(wheels::IterBlocksItem::NoMoreBlocks) =>
                break,
        }
    }

    log::info!("FINISHED: blocks checked = {}, entries = {} | {:?}", checked_blocks, checked_entries, counter);
    Ok::<_, Error>(())
}
