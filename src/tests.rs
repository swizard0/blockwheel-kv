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
    blockwheel,
};

use crate as blockwheel_kv;

#[test]
fn stress() {
    env_logger::init();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let work_block_size_bytes = 16 * 1024;
    let init_wheel_size_bytes = 1 * 1024 * 1024;

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

    let limits = Limits {
        active_tasks: 128,
        // actions: 1024,
        actions: 1024,
        key_size_bytes: 64,
        value_size_bytes: 4096,
    };

    let mut data = DataIndex {
        index: HashMap::new(),
        data: Vec::new(),
    };
    let mut counter = Counter::default();

    fs::remove_file(&params.wheel_a.wheel_filename).ok();
    fs::remove_file(&params.wheel_b.wheel_filename).ok();
    runtime.block_on(stress_loop(params.clone(), &mut data, &mut counter, &limits)).unwrap();

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
    data: Vec<(kv::Key, kv::Value)>,
}

#[derive(Debug)]
enum Error {
    Insert(blockwheel_kv::InsertError),
    Lookup(blockwheel_kv::LookupError),
    ExpectedValueNotFound { key: kv::Key, value: kv::Value, },
    UnexpectedValueFound { key: kv::Key, expected_value: kv::Value, found_value: kv::Value, },
    WheelAGoneDuringInfo,
    WheelBGoneDuringInfo,
    WheelsGoneDuringFlush,
}

async fn stress_loop(
    params: Params,
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
            wheels_pid.clone(),
            blockwheel_kv::Params::default(),
        ),
    );

    let mut rng = rand::thread_rng();
    let (done_tx, done_rx) = mpsc::channel(0);
    pin_mut!(done_rx);
    let mut active_tasks_counter = Counter::default();
    let mut actions_counter = 0;

    enum TaskDone {
        Lookup,
        Insert { key: kv::Key, value: kv::Value, },
        Remove,
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

    fn process(task_done: TaskDone, data: &mut DataIndex, counter: &mut Counter, active_tasks_counter: &mut Counter) {
        match task_done {
            TaskDone::Insert { key, value, } => {
                let offset = data.data.len();
                data.data.push((key.clone(), value));
                data.index.insert(key, offset);
                counter.inserts += 1;
                active_tasks_counter.inserts -= 1;
            },
            TaskDone::Lookup => {
                counter.lookups += 1;
                active_tasks_counter.lookups -= 1;
            }
            TaskDone::Remove => {
                counter.removes += 1;
                active_tasks_counter.removes -= 1;
            },
        }
    }

    loop {
        if actions_counter >= limits.actions {
            std::mem::drop(done_tx);

            while active_tasks_counter.sum() > 0 {
                process(done_rx.next().await.unwrap()?, data, counter, &mut active_tasks_counter);
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
                process(task_result?, data, counter, &mut active_tasks_counter);
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
                        Ok(blockwheel_kv::Inserted) =>
                            Ok(TaskDone::Insert { key, value, }),
                        Err(error) =>
                            Err(Error::Insert(error))
                    }
                });
                active_tasks_counter.inserts += 1;
            } else {
                // remove task
                let key_index = rng.gen_range(0, data.data.len());
                let (key, value) = data.data.swap_remove(key_index);
                data.index.remove(&key);

                log::info!(
                    "{}. performing REMOVE with {} bytes key and {} bytes value (dice = {:.3}, prob = {:.3}) | {:?}",
                    actions_counter,
                    key.key_bytes.len(),
                    value.value_bytes.len(),
                    dice,
                    1.0 - insert_prob,
                    counter,
                );

                let mut wheel_kv_pid = wheel_kv_pid.clone();
                spawn_task(&mut supervisor_pid, done_tx.clone(), async move {

                    Ok(TaskDone::Remove)
                });
                active_tasks_counter.removes += 1;
            }
        } else {
            // lookup task
            let key_index = rng.gen_range(0, data.data.len());
            let (key, value) = data.data[key_index].clone();

            log::info!(
                "{}. performing LOOKUP with {} bytes key and {} bytes value | {:?}",
                actions_counter,
                key.key_bytes.len(),
                value.value_bytes.len(),
                counter,
            );

            let mut wheel_kv_pid = wheel_kv_pid.clone();
            spawn_task(&mut supervisor_pid, done_tx.clone(), async move {
                match wheel_kv_pid.lookup(key.clone()).await {
                    Ok(None) =>
                        Err(Error::ExpectedValueNotFound { key, value, }),
                    Ok(Some(found_value)) if found_value == value =>
                        Ok(TaskDone::Lookup),
                    Ok(Some(found_value)) =>
                        Err(Error::UnexpectedValueFound { key, expected_value: value, found_value, }),
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
