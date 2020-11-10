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
    blockwheel,
};

use crate as blockwheel_kv;

#[test]
fn stress() {
    env_logger::init();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    let limits = Limits {
        active_tasks: 1,
        actions: 1024,
        key_size_bytes: 32,
        value_size_bytes: 4096,
    };

    let work_block_size_bytes = 16 * 1024;
    let init_wheel_size_bytes = 2 * 1024 * 1024;

    // let runtime = tokio::runtime::Builder::new_multi_thread()
    //     .build()
    //     .unwrap();

    // let limits = Limits {
    //     active_tasks: 256,
    //     actions: 16384,
    //     key_size_bytes: 32,
    //     value_size_bytes: 4096,
    // };

    // let work_block_size_bytes = 16 * 1024;
    // let init_wheel_size_bytes = 16 * 1024 * 1024;

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
    };

    let version_provider = version::Provider::new();
    let mut data = DataIndex {
        index: HashMap::new(),
        data: Vec::new(),
        current_version: 0,
    };
    let mut counter = Counter::default();

    fs::remove_file(&params.wheel_a.wheel_filename).ok();
    fs::remove_file(&params.wheel_b.wheel_filename).ok();
    runtime.block_on(stress_loop(params.clone(), &version_provider, &mut data, &mut counter, &limits)).unwrap();

    fs::remove_file(&params.wheel_a.wheel_filename).ok();
    fs::remove_file(&params.wheel_b.wheel_filename).ok();
}

#[derive(Clone)]
struct Params {
    wheel_a: blockwheel::Params,
    wheel_b: blockwheel::Params,
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
    inserts: usize,
    removes: usize,
}

impl Counter {
    fn sum(&self) -> usize {
        self.lookups + self.inserts + self.removes
    }

    // fn clear(&mut self) {
    //     self.lookups = 0;
    //     self.inserts = 0;
    //     self.removes = 0;
    // }
}

struct DataIndex {
    index: HashMap<kv::Key, usize>,
    data: Vec<kv::KeyValuePair>,
    current_version: u64,
}

#[derive(Debug)]
enum Error {
    Insert(blockwheel_kv::InsertError),
    Lookup(blockwheel_kv::LookupError),
    Remove(blockwheel_kv::RemoveError),
    ExpectedValueNotFound {
        key: kv::Key,
        value_cell: kv::ValueCell,
    },
    UnexpectedValueFound {
        key: kv::Key,
        expected_value_cell: kv::ValueCell,
        found_value_cell: kv::ValueCell,
    },
    WheelAGoneDuringInfo,
    WheelBGoneDuringInfo,
    WheelsGoneDuringFlush,
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
        wheels_gen_server.run(wheels::Params::default()),
    );

    let has_added = wheels_pid.add(wheels::WheelRef {
        blockwheel_filename: blockwheel_a_filename,
        blockwheel_pid: wheel_a_pid.clone(),
    }).await;
    assert_eq!(has_added, Ok(true));

    let has_added = wheels_pid.add(wheels::WheelRef {
        blockwheel_filename: blockwheel_b_filename,
        blockwheel_pid: wheel_b_pid.clone(),
    }).await;
    assert_eq!(has_added, Ok(true));

    let wheel_kv_gen_server = blockwheel_kv::GenServer::new();
    let wheel_kv_pid = wheel_kv_gen_server.pid();
    supervisor_pid.spawn_link_permanent(
        wheel_kv_gen_server.run(
            supervisor_pid.clone(),
            blocks_pool.clone(),
            version_provider.clone(),
            wheels_pid.clone(),
            blockwheel_kv::Params {
                standalone_search_trees_count: 1,
                ..Default::default()
            },
        ),
    );

    let mut rng = rand::thread_rng();
    let (done_tx, done_rx) = mpsc::channel(0);
    pin_mut!(done_rx);
    let mut active_tasks_counter = Counter::default();
    let mut actions_counter = 0;

    enum TaskDone {
        Lookup { key: kv::Key, found_value_cell: kv::ValueCell, version_snapshot: u64, },
        Insert { key: kv::Key, value: kv::Value, version: u64, },
        Remove { key: kv::Key, version: u64, },
    }

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
                if let Some(&offset) = data.index.get(&key) {
                    data.data[offset] = data_cell;
                } else {
                    let offset = data.data.len();
                    data.data.push(data_cell);
                    data.index.insert(key, offset);
                }
                data.current_version = version;
                counter.inserts += 1;
                active_tasks_counter.inserts -= 1;
            },
            TaskDone::Lookup { key, found_value_cell, version_snapshot, } => {
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
                    unreachable!();
                }
                counter.lookups += 1;
                active_tasks_counter.lookups -= 1;
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

                log::info!(
                    "{}. performing INSERT with {} bytes key and {} bytes value (dice = {:.3}, prob = {:.3}) | {:?}",
                    actions_counter,
                    key_amount,
                    value_amount,
                    dice,
                    insert_prob,
                    counter,
                );

                spawn_task(&mut supervisor_pid, done_tx.clone(), async move {
                    let mut block = blocks_pool.lend();
                    block.resize(key_amount, 0);
                    rand::thread_rng().fill(&mut block[..]);
                    let key = kv::Key { key_bytes: block.freeze(), };
                    let mut block = blocks_pool.lend();
                    block.resize(value_amount, 0);
                    rand::thread_rng().fill(&mut block[..]);
                    let value = kv::Value { value_bytes: block.freeze(), };
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

                log::info!(
                    "{}. performing REMOVE with {} bytes key and {} bytes value (dice = {:.3}, prob = {:.3}) | {:?}",
                    actions_counter,
                    key.key_bytes.len(),
                    value.value_bytes.len(),
                    dice,
                    1.0 - insert_prob,
                    counter,
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

            log::info!(
                "{}. performing LOOKUP with {} bytes key and {} value | {:?}",
                actions_counter,
                key.key_bytes.len(),
                match &value_cell.cell {
                    kv::Cell::Value(value) =>
                        format!("{} bytes", value.value_bytes.len()),
                    kv::Cell::Tombstone =>
                        "tombstone".to_string(),
                },
                counter,
            );

            let key = key.clone();
            let value_cell = value_cell.clone();
            let mut wheel_kv_pid = wheel_kv_pid.clone();
            spawn_task(&mut supervisor_pid, done_tx.clone(), async move {
                match wheel_kv_pid.lookup(key.clone()).await {
                    Ok(None) =>
                        Err(Error::ExpectedValueNotFound { key, value_cell, }),
                    Ok(Some(found_value_cell)) =>
                        Ok(TaskDone::Lookup { key, found_value_cell, version_snapshot, }),
                    Err(error) =>
                        Err(Error::Lookup(error))
                }
            });
            active_tasks_counter.lookups += 1;
        }
        actions_counter += 1;
    }

    assert!(done_rx.next().await.is_none());

    let wheels::Flushed = wheels_pid.flush().await
        .map_err(|ero::NoProcError| Error::WheelsGoneDuringFlush)?;

    Ok::<_, Error>(())
}
