use std::{
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
        actions: 1,
        key_size_bytes: 64,
        value_size_bytes: 4096,
    };

    let mut data = HashMap::new();
    let mut counter = Counter::default();

    runtime.block_on(stress_loop(params.clone(), &mut data, &mut counter, &limits)).unwrap();
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

type DataIndex = HashMap<kv::Key, kv::Value>;

#[derive(Debug)]
enum Error {
    Insert(blockwheel_kv::InsertError),
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

    let wheel_a_gen_server = blockwheel::GenServer::new();
    let wheel_a_pid = wheel_a_gen_server.pid();
    supervisor_pid.spawn_link_permanent(
        wheel_a_gen_server.run(supervisor_pid.clone(), blocks_pool.clone(), params.wheel_a),
    );

    let wheel_b_gen_server = blockwheel::GenServer::new();
    let wheel_b_pid = wheel_b_gen_server.pid();
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
        blockwheel_pid: wheel_a_pid,
    }).await;
    assert_eq!(has_added, Ok(true));


    let wheel_kv_gen_server = blockwheel_kv::GenServer::new();
    let wheel_kv_pid = wheel_kv_gen_server.pid();
    supervisor_pid.spawn_link_permanent(
        wheel_kv_gen_server.run(
            supervisor_pid.clone(),
            blocks_pool.clone(),
            wheels_pid,
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
                data.insert(key, value);
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
        let maybe_task_result = if (active_tasks_counter.sum() >= limits.active_tasks) || (data.is_empty() && active_tasks_counter.inserts > 0) {
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

        // insert task
        let mut wheel_kv_pid = wheel_kv_pid.clone();
        let blocks_pool = blocks_pool.clone();
        let key_amount = rng.gen_range(1, limits.key_size_bytes);
        let value_amount = rng.gen_range(1, limits.value_size_bytes);
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


    //     // construct action and run task
    //     let info = pid.info().await
    //         .map_err(|ero::NoProcError| Error::WheelGoneDuringInfo)?;
    //     if blocks.is_empty() || rng.gen_range(0.0, 1.0) < 0.5 {
    //         // write or delete task
    //         let write_prob = if info.bytes_free * 2 >= info.wheel_size_bytes {
    //             1.0
    //         } else {
    //             (info.bytes_free * 2) as f64 / info.wheel_size_bytes as f64
    //         };
    //         let dice = rng.gen_range(0.0, 1.0);
    //         if blocks.is_empty() || dice < write_prob {
    //             // write task
    //             let mut blockwheel_pid = pid.clone();
    //             let blocks_pool = blocks_pool.clone();
    //             let amount = rng.gen_range(1, limits.block_size_bytes);
    //             spawn_task(&mut supervisor_pid, done_tx.clone(), async move {
    //                 let mut block = blocks_pool.lend();
    //                 block.extend((0 .. amount).map(|_| 0));
    //                 rand::thread_rng().fill(&mut block[..]);
    //                 let block_bytes = block.freeze();
    //                 match blockwheel_pid.write_block(block_bytes.clone()).await {
    //                     Ok(block_id) =>
    //                         Ok(TaskDone::WriteBlock(BlockTank { block_id, block_bytes, })),
    //                     Err(super::WriteBlockError::NoSpaceLeft) =>
    //                         Ok(TaskDone::WriteBlockNoSpace),
    //                     Err(error) =>
    //                         Err(Error::WriteBlock(error))
    //                 }
    //             });
    //             active_tasks_counter.writes += 1;
    //         } else {
    //             // delete task
    //             let block_index = rng.gen_range(0, blocks.len());
    //             let BlockTank { block_id, .. } = blocks.swap_remove(block_index);
    //             let mut blockwheel_pid = pid.clone();
    //             spawn_task(&mut supervisor_pid, done_tx.clone(), async move {
    //                 let Deleted = blockwheel_pid.delete_block(block_id.clone()).await
    //                     .map_err(Error::DeleteBlock)?;
    //                 Ok(TaskDone::DeleteBlock)
    //             });
    //             active_tasks_counter.deletes += 1;
    //         }
    //     } else {
    //         // read task
    //         let block_index = rng.gen_range(0, blocks.len());
    //         let BlockTank { block_id, block_bytes, } = blocks[block_index].clone();
    //         let mut blockwheel_pid = pid.clone();
    //         spawn_task(&mut supervisor_pid, done_tx.clone(), async move {
    //             let block_bytes_read = blockwheel_pid.read_block(block_id.clone()).await
    //                 .map_err(Error::ReadBlock)?;
    //             let expected_crc = block::crc(&block_bytes);
    //             let provided_crc = block::crc(&block_bytes_read);
    //             if expected_crc != provided_crc {
    //                 Err(Error::ReadBlockCrcMismarch { block_id, expected_crc, provided_crc, })
    //             } else {
    //                 Ok(TaskDone::ReadBlock)
    //             }
    //         });
    //         active_tasks_counter.reads += 1;
    //     }
        actions_counter += 1;
    }

    assert!(done_rx.next().await.is_none());

    Ok::<_, Error>(())
}
