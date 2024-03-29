use std::{
    fs,
    fmt,
    sync::{
        mpsc,
        Mutex,
    },
    path::{
        PathBuf,
    },
    collections::{
        HashMap,
    },
};

use rand::{
    Rng,
    SeedableRng,
    rngs::SmallRng,
    distributions::Uniform,
};

use alloc_pool::{
    bytes::{
        BytesPool,
    },
};

use arbeitssklave::{
    komm,
};

use crate::{
    kv,
    job,
    wheels,
    version,
    core::{
        performer_sklave,
    },
};

use crate as blockwheel_kv;

#[test]
fn stress() {
    env_logger::init();

    let limits = Limits {
        active_tasks: 128,
        actions: 512,
        key_size_bytes: 32,
        value_size_bytes: 4096,
        insert_or_remove_prob: 0.5,
    };
    let init_wheel_size_bytes = (limits.key_size_bytes + limits.value_size_bytes) * limits.actions;

    // let limits = Limits {
    //     active_tasks: 1024,
    //     actions: 32768,
    //     key_size_bytes: 32,
    //     value_size_bytes: 4096,
    //     insert_or_remove_prob: 0.5,
    // };
    // let init_wheel_size_bytes = 134217728; // (limits.key_size_bytes + limits.value_size_bytes) * limits.actions / 8;

    let kv = blockwheel_kv::Params {
        ..Default::default()
    };
    let work_block_size_bytes = (limits.key_size_bytes + limits.value_size_bytes) * kv.tree_block_size;

    let wheel_filename_a: PathBuf = "/tmp/blockwheel_kv_a_stress".into();
    let wheel_filename_b: PathBuf = "/tmp/blockwheel_kv_b_stress".into();

    let params = Params {
        wheel_a: blockwheel_fs::Params {
            interpreter: blockwheel_fs::InterpreterParams::FixedFile(blockwheel_fs::FixedFileInterpreterParams {
                wheel_filename: wheel_filename_a.clone(),
                init_wheel_size_bytes,
            }),
            work_block_size_bytes,
            lru_cache_size_bytes: 0,
            defrag_parallel_tasks_limit: 8,
        },
        wheel_b: blockwheel_fs::Params {
            interpreter: blockwheel_fs::InterpreterParams::FixedFile(blockwheel_fs::FixedFileInterpreterParams {
                wheel_filename: wheel_filename_b.clone(),
                init_wheel_size_bytes,
            }),
            work_block_size_bytes,
            lru_cache_size_bytes: 0,
            defrag_parallel_tasks_limit: 8,
        },
        kv,
    };

    let version_provider = version::Provider::from_unix_epoch_seed();
    let mut data = DataIndex {
        index: HashMap::new(),
        alive: HashMap::new(),
        data: Vec::new(),
        streams: HashMap::new(),
    };
    let mut counter = Counter::default();

    log::info!("stage #0: run over an empty base");

    fs::remove_file(&wheel_filename_a).ok();
    fs::remove_file(&wheel_filename_b).ok();
    stress_loop(params.clone(), &version_provider, &mut data, &mut counter, &limits)
        .unwrap();

    log::info!("stage #1: run over an existing base");

    // next load existing wheel and repeat stress
    counter.clear();
    stress_loop(params, &version_provider, &mut data, &mut counter, &limits)
        .unwrap();

    fs::remove_file(&wheel_filename_a).ok();
    fs::remove_file(&wheel_filename_b).ok();
}

#[derive(Clone)]
struct Params {
    wheel_a: blockwheel_fs::Params,
    wheel_b: blockwheel_fs::Params,
    kv: blockwheel_kv::Params,
}

#[derive(Clone, Copy, Default, Debug)]
struct Limits {
    actions: usize,
    active_tasks: usize,
    key_size_bytes: usize,
    value_size_bytes: usize,
    insert_or_remove_prob: f64,
}

#[derive(Clone, Copy, Default, Debug)]
struct Counter {
    lookups_range: usize,
    inserts: usize,
    removes: usize,
    insert_jobs: usize,
}

impl Counter {
    fn sum(&self) -> usize {
        self.lookups_range + self.inserts + self.removes
    }

    fn sum_total(&self) -> usize {
        self.sum() + self.insert_jobs
    }

    fn clear(&mut self) {
        self.lookups_range = 0;
        self.inserts = 0;
        self.removes = 0;
        self.insert_jobs = 0;
    }
}

struct DataIndex {
    index: HashMap<kv::Key, usize>,
    alive: HashMap<kv::Key, usize>,
    data: Vec<kv::KeyValuePair<u64>>,
    streams: HashMap<komm::StreamId, blockwheel_kv::LookupRangeStream<EchoPolicy>>,
}

#[derive(Debug)]
pub enum Error {
    ThreadPool(edeltraud::BuildError),
    Edeltraud(edeltraud::SpawnError),
    WheelRefMeister(blockwheel_fs::Error),
    BlockwheelKvMeister(blockwheel_kv::Error),
    UnexpectedFtdOrder(String),
    ExpectedValueNotFound {
        key: kv::Key,
        value_cell: kv::ValueCell<u64>,
    },
    UnexpectedValueFound {
        key: kv::Key,
        expected_value_cell: kv::ValueCell<u64>,
        found_value_cell: kv::ValueCell<kv::Value>,
    },
    WheelsBuilder(wheels::BuilderError),
    FtdProcessIsLost,
    RequestInfo(blockwheel_kv::Error),
    RequestFlush(blockwheel_kv::Error),
    RequestInsert(blockwheel_kv::Error),
    RequestRemove(blockwheel_kv::Error),
    RequestLookupRange(blockwheel_kv::Error),
    InsertJobCanceled,
}

fn make_wheel_ref<J>(
    params: blockwheel_fs::Params,
    blocks_pool: &BytesPool,
    thread_pool: &edeltraud::Handle<J>,
)
    -> Result<wheels::WheelRef<EchoPolicy>, Error>
where J: From<blockwheel_fs::job::SklaveJob<wheels::WheelEchoPolicy<EchoPolicy>>>,
      J: Send + 'static,
{
    let blockwheel_filename = match &params.interpreter {
        blockwheel_fs::InterpreterParams::FixedFile(interpreter_params) =>
            wheels::WheelFilename::from_path(&interpreter_params.wheel_filename, blocks_pool),
        blockwheel_fs::InterpreterParams::Ram(..) |
        blockwheel_fs::InterpreterParams::Dummy(..) => {
            let mut rng = rand::thread_rng();
            let filename: String = (0 .. 16).map(|_| rng.gen::<char>()).collect();
            wheels::WheelFilename::from_path(&filename, blocks_pool)
        },
    };

    let meister =
        blockwheel_fs::Meister::versklaven(
            params,
            blocks_pool.clone(),
            thread_pool,
        )
        .map_err(Error::WheelRefMeister)?;

    Ok(wheels::WheelRef { blockwheel_filename, meister, })
}

#[derive(Debug)]
struct EchoPolicy;

impl blockwheel_kv::EchoPolicy for EchoPolicy {
    type Info = komm::Rueckkopplung<ReplyOrder, ReplyInfo>;
    type Insert = komm::Rueckkopplung<ReplyOrder, ReplyInsert>;
    type LookupRange = komm::Rueckkopplung<ReplyOrder, ReplyLookupRange>;
    type Remove = komm::Rueckkopplung<ReplyOrder, ReplyRemove>;
    type Flush = komm::Rueckkopplung<ReplyOrder, ReplyFlush>;
}

enum ReplyOrder {
    InfoCancel(komm::UmschlagAbbrechen<ReplyInfo>),
    Info(komm::Umschlag<blockwheel_kv::Info, ReplyInfo>),
    InsertCancel(komm::UmschlagAbbrechen<ReplyInsert>),
    Insert(komm::Umschlag<blockwheel_kv::Inserted, ReplyInsert>),
    LookupRangeCancel(komm::UmschlagAbbrechen<ReplyLookupRange>),
    LookupRange(komm::Umschlag<komm::Streamzeug<kv::KeyValuePair<kv::Value>>, ReplyLookupRange>),
    RemoveCancel(komm::UmschlagAbbrechen<ReplyRemove>),
    Remove(komm::Umschlag<blockwheel_kv::Removed, ReplyRemove>),
    FlushCancel(komm::UmschlagAbbrechen<ReplyFlush>),
    Flush(komm::Umschlag<blockwheel_kv::Flushed, ReplyFlush>),

    InsertJobCancel(komm::UmschlagAbbrechen<InsertJobStamp>),
    InsertJob(komm::Umschlag<Result<(), Error>, InsertJobStamp>),
}

impl fmt::Debug for ReplyOrder {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplyOrder::LookupRange(..) => {
                fmt.debug_tuple("ReplyOrder::LookupRange")
                    .field(&"<contents>")
                    .finish()
            },
            other =>
                other.fmt(fmt),
        }
    }
}

#[derive(Debug)]
struct ReplyInfo;

#[derive(Debug)]
struct ReplyInsert {
    key: kv::Key,
    value_crc: u64,
}

#[derive(Debug)]
struct ReplyLookupRange {
    key: kv::Key,
    value_cell: kv::ValueCell<u64>,
}

#[derive(Debug)]
struct ReplyRemove {
    key: kv::Key,
}

#[derive(Debug)]
struct ReplyFlush;

impl From<komm::UmschlagAbbrechen<ReplyInfo>> for ReplyOrder {
    fn from(v: komm::UmschlagAbbrechen<ReplyInfo>) -> ReplyOrder {
        ReplyOrder::InfoCancel(v)
    }
}

impl From<komm::Umschlag<blockwheel_kv::Info, ReplyInfo>> for ReplyOrder {
    fn from(v: komm::Umschlag<blockwheel_kv::Info, ReplyInfo>) -> ReplyOrder {
        ReplyOrder::Info(v)
    }
}

impl From<komm::UmschlagAbbrechen<ReplyInsert>> for ReplyOrder {
    fn from(v: komm::UmschlagAbbrechen<ReplyInsert>) -> ReplyOrder {
        ReplyOrder::InsertCancel(v)
    }
}

impl From<komm::Umschlag<blockwheel_kv::Inserted, ReplyInsert>> for ReplyOrder {
    fn from(v: komm::Umschlag<blockwheel_kv::Inserted, ReplyInsert>) -> ReplyOrder {
        ReplyOrder::Insert(v)
    }
}

impl From<komm::UmschlagAbbrechen<ReplyLookupRange>> for ReplyOrder {
    fn from(v: komm::UmschlagAbbrechen<ReplyLookupRange>) -> ReplyOrder {
        ReplyOrder::LookupRangeCancel(v)
    }
}

impl From<komm::Umschlag<komm::Streamzeug<kv::KeyValuePair<kv::Value>>, ReplyLookupRange>> for ReplyOrder {
    fn from(v: komm::Umschlag<komm::Streamzeug<kv::KeyValuePair<kv::Value>>, ReplyLookupRange>) -> ReplyOrder {
        ReplyOrder::LookupRange(v)
    }
}

impl From<komm::UmschlagAbbrechen<ReplyRemove>> for ReplyOrder {
    fn from(v: komm::UmschlagAbbrechen<ReplyRemove>) -> ReplyOrder {
        ReplyOrder::RemoveCancel(v)
    }
}

impl From<komm::Umschlag<blockwheel_kv::Removed, ReplyRemove>> for ReplyOrder {
    fn from(v: komm::Umschlag<blockwheel_kv::Removed, ReplyRemove>) -> ReplyOrder {
        ReplyOrder::Remove(v)
    }
}

impl From<komm::UmschlagAbbrechen<ReplyFlush>> for ReplyOrder {
    fn from(v: komm::UmschlagAbbrechen<ReplyFlush>) -> ReplyOrder {
        ReplyOrder::FlushCancel(v)
    }
}

impl From<komm::Umschlag<blockwheel_kv::Flushed, ReplyFlush>> for ReplyOrder {
    fn from(v: komm::Umschlag<blockwheel_kv::Flushed, ReplyFlush>) -> ReplyOrder {
        ReplyOrder::Flush(v)
    }
}

impl From<komm::UmschlagAbbrechen<InsertJobStamp>> for ReplyOrder {
    fn from(v: komm::UmschlagAbbrechen<InsertJobStamp>) -> ReplyOrder {
        ReplyOrder::InsertJobCancel(v)
    }
}

impl From<komm::Umschlag<Result<(), Error>, InsertJobStamp>> for ReplyOrder {
    fn from(v: komm::Umschlag<Result<(), Error>, InsertJobStamp>) -> ReplyOrder {
        ReplyOrder::InsertJob(v)
    }
}

struct Welt {
    ftd_tx: Mutex<mpsc::Sender<ReplyOrder>>,
}

enum Job {
    BlockwheelFs(blockwheel_fs::job::Job<wheels::WheelEchoPolicy<EchoPolicy>>),
    BlockwheelKv(blockwheel_kv::job::Job<EchoPolicy>),
    FtdSklave(arbeitssklave::SklaveJob<Welt, ReplyOrder>),
    Insert(InsertJob),
}

impl From<blockwheel_fs::job::Job<wheels::WheelEchoPolicy<EchoPolicy>>> for Job {
    fn from(job: blockwheel_fs::job::Job<wheels::WheelEchoPolicy<EchoPolicy>>) -> Self {
        Self::BlockwheelFs(job)
    }
}

impl From<blockwheel_fs::job::SklaveJob<wheels::WheelEchoPolicy<EchoPolicy>>> for Job {
    fn from(job: blockwheel_fs::job::SklaveJob<wheels::WheelEchoPolicy<EchoPolicy>>) -> Self {
        Self::BlockwheelFs(job.into())
    }
}

impl From<blockwheel_fs::job::BlockPrepareWriteJob<wheels::WheelEchoPolicy<EchoPolicy>>> for Job {
    fn from(job: blockwheel_fs::job::BlockPrepareWriteJob<wheels::WheelEchoPolicy<EchoPolicy>>) -> Self {
        Self::BlockwheelFs(job.into())
    }
}

impl From<blockwheel_fs::job::BlockPrepareDeleteJob<wheels::WheelEchoPolicy<EchoPolicy>>> for Job {
    fn from(job: blockwheel_fs::job::BlockPrepareDeleteJob<wheels::WheelEchoPolicy<EchoPolicy>>) -> Self {
        Self::BlockwheelFs(job.into())
    }
}

impl From<blockwheel_fs::job::BlockProcessReadJob<wheels::WheelEchoPolicy<EchoPolicy>>> for Job {
    fn from(job: blockwheel_fs::job::BlockProcessReadJob<wheels::WheelEchoPolicy<EchoPolicy>>) -> Self {
        Self::BlockwheelFs(job.into())
    }
}

impl From<blockwheel_kv::job::Job<EchoPolicy>> for Job {
    fn from(job: blockwheel_kv::job::Job<EchoPolicy>) -> Self {
        Self::BlockwheelKv(job)
    }
}

impl From<job::FlushButcherSklaveJob<EchoPolicy>> for Job {
    fn from(job: job::FlushButcherSklaveJob<EchoPolicy>) -> Self {
        Self::BlockwheelKv(job.into())
    }
}

impl From<job::LookupRangeMergeSklaveJob<EchoPolicy>> for Job {
    fn from(job: job::LookupRangeMergeSklaveJob<EchoPolicy>) -> Self {
        Self::BlockwheelKv(job.into())
    }
}

impl From<job::MergeSearchTreesSklaveJob<EchoPolicy>> for Job {
    fn from(job: job::MergeSearchTreesSklaveJob<EchoPolicy>) -> Self {
        Self::BlockwheelKv(job.into())
    }
}

impl From<job::DemolishSearchTreeSklaveJob<EchoPolicy>> for Job {
    fn from(job: job::DemolishSearchTreeSklaveJob<EchoPolicy>) -> Self {
        Self::BlockwheelKv(job.into())
    }
}

impl From<job::PerformerSklaveJob<EchoPolicy>> for Job {
    fn from(job: job::PerformerSklaveJob<EchoPolicy>) -> Self {
        Self::BlockwheelKv(job.into())
    }
}

impl From<arbeitssklave::SklaveJob<Welt, ReplyOrder>> for Job {
    fn from(job: arbeitssklave::SklaveJob<Welt, ReplyOrder>) -> Self {
        Self::FtdSklave(job)
    }
}

impl From<InsertJob> for Job {
    fn from(args: InsertJob) -> Job {
        Job::Insert(args)
    }
}

pub struct JobUnit<J>(edeltraud::JobUnit<J, Job>);

impl<J> From<edeltraud::JobUnit<J, Job>> for JobUnit<J> {
    fn from(job_unit: edeltraud::JobUnit<J, Job>) -> Self {
        Self(job_unit)
    }
}

impl<J> edeltraud::Job for JobUnit<J>
where J: From<blockwheel_fs::job::SklaveJob<wheels::WheelEchoPolicy<EchoPolicy>>>,
      J: From<blockwheel_fs::job::BlockPrepareWriteJob<wheels::WheelEchoPolicy<EchoPolicy>>>,
      J: From<blockwheel_fs::job::BlockPrepareDeleteJob<wheels::WheelEchoPolicy<EchoPolicy>>>,
      J: From<blockwheel_fs::job::BlockProcessReadJob<wheels::WheelEchoPolicy<EchoPolicy>>>,
      J: From<job::FlushButcherSklaveJob<EchoPolicy>>,
      J: From<job::LookupRangeMergeSklaveJob<EchoPolicy>>,
      J: From<job::MergeSearchTreesSklaveJob<EchoPolicy>>,
      J: From<job::DemolishSearchTreeSklaveJob<EchoPolicy>>,
      J: From<job::PerformerSklaveJob<EchoPolicy>>,
      J: From<arbeitssklave::SklaveJob<Welt, ReplyOrder>>,
      J: Send + 'static,
{
    fn run(self) {
        match self.0.job {
            Job::BlockwheelFs(job) => {
                let job_unit = blockwheel_fs::job::JobUnit::from(edeltraud::JobUnit {
                    handle: self.0.handle,
                    job,
                });
                job_unit.run();
            },
            Job::BlockwheelKv(job) => {
                let job_unit = job::JobUnit::from(edeltraud::JobUnit {
                    handle: self.0.handle,
                    job,
                });
                job_unit.run();
            },
            Job::FtdSklave(mut sklave_job) => {
                #[allow(clippy::while_let_loop)]
                loop {
                    match sklave_job.zu_ihren_diensten() {
                        Ok(arbeitssklave::Gehorsam::Machen { mut befehle, }) =>
                            loop {
                                match befehle.befehl() {
                                    arbeitssklave::SklavenBefehl::Mehr {
                                        befehl,
                                        mehr_befehle,
                                    } => {
                                        befehle = mehr_befehle;
                                        let Ok(tx_lock) = befehle.ftd_tx.lock() else {
                                            log::error!("failed to lock mutex in FtdSklave job, terminating");
                                            return;
                                        };
                                        if let Err(mpsc::SendError(befehl)) = tx_lock.send(befehl) {
                                            log::warn!("failed to send back order {befehl:?} in FtdSklave job, terminating");
                                            return;
                                        }
                                    },
                                    arbeitssklave::SklavenBefehl::Ende { sklave_job: next_sklave_job, } => {
                                        sklave_job = next_sklave_job;
                                        break;
                                    },
                                }
                            },
                        Ok(arbeitssklave::Gehorsam::Rasten) =>
                            break,
                        Err(error) => {
                            log::info!("FtdSklave::zu_ihren_diensten terminated with {error:?}");
                            break;
                        },
                    }
                }
            },
            Job::Insert(args) =>
                job_insert(args, &self.0.handle),
        }
    }
}

#[derive(Debug)]
struct InsertJobStamp;

struct InsertJob {
    main: InsertJobMain,
    job_complete: komm::Rueckkopplung<ReplyOrder, InsertJobStamp>,
}

struct InsertJobMain {
    blocks_pool: BytesPool,
    key_amount: usize,
    value_amount: usize,
    blockwheel_kv_meister: blockwheel_kv::Meister<EchoPolicy>,
    ftd_sendegeraet: komm::Sendegeraet<ReplyOrder>,
}

fn job_insert<J>(job: InsertJob, thread_pool: &edeltraud::Handle<J>)
where J: From<performer_sklave::SklaveJob<EchoPolicy>>,
{
    let output = run_job_insert(job.main, thread_pool);
    job.job_complete.commit(output).ok();
}

fn run_job_insert<J>(job: InsertJobMain, thread_pool: &edeltraud::Handle<J>) -> Result<(), Error>
where J: From<performer_sklave::SklaveJob<EchoPolicy>>,
{
    let mut key_block = job.blocks_pool.lend();
    let mut value_block = job.blocks_pool.lend();
    let mut rng = SmallRng::from_entropy();
    key_block.reserve(job.key_amount);
    for _ in 0 .. job.key_amount {
        key_block.push(rng.gen());
    }
    value_block.reserve(job.value_amount);
    for _ in 0 .. job.value_amount {
        value_block.push(rng.gen());
    }
    let key_bytes = key_block.freeze();
    let value_bytes = value_block.freeze();
    let value_crc = blockwheel_fs::block::crc(&value_bytes);

    let key = kv::Key { key_bytes, };
    let value = kv::Value { value_bytes, };

    let stamp = ReplyInsert {
        key: key.clone(),
        value_crc,
    };
    let rueckkopplung = job.ftd_sendegeraet.rueckkopplung(stamp);
    job.blockwheel_kv_meister.insert(key, value, rueckkopplung, thread_pool)
        .map_err(Error::RequestInsert)
}


fn stress_loop(
    params: Params,
    version_provider: &version::Provider,
    data: &mut DataIndex,
    counter: &mut Counter,
    limits: &Limits,
)
    -> Result<(), Error>
{
    let blocks_pool = BytesPool::new();
    let edeltraud = edeltraud::Builder::new()
        .build::<_, JobUnit<_>>()
        .map_err(Error::ThreadPool)?;
    let thread_pool = edeltraud.handle();

    let wheel_ref_a = make_wheel_ref(params.wheel_a, &blocks_pool, &thread_pool)?;
    let wheel_ref_b = make_wheel_ref(params.wheel_b, &blocks_pool, &thread_pool)?;

    let wheels = wheels::WheelsBuilder::new()
        .add_wheel_ref(wheel_ref_a)
        .add_wheel_ref(wheel_ref_b)
        .build()
        .map_err(Error::WheelsBuilder)?;

    let meister =
        blockwheel_kv::Meister::versklaven(
            params.kv,
            blocks_pool.clone(),
            version_provider.clone(),
            wheels,
            &thread_pool,
        )
        .map_err(Error::BlockwheelKvMeister)?;

    let (ftd_tx, ftd_rx) = mpsc::channel();
    let ftd_sklave_meister = arbeitssklave::Freie::new()
        .versklaven(Welt { ftd_tx: Mutex::new(ftd_tx), }, &thread_pool)
        .unwrap();
    let ftd_sendegeraet =
        komm::Sendegeraet::starten(&ftd_sklave_meister, thread_pool.clone());

    let mut rng = SmallRng::from_entropy();
    let p_distribution = Uniform::new(0.0, 1.0);
    let key_distribution = Uniform::new(1, limits.key_size_bytes);
    let value_distribution = Uniform::new(1, limits.value_size_bytes);

    let mut active_tasks_counter = Counter::default();
    let mut actions_counter = 0;

    loop {
        if actions_counter >= limits.actions {
            while active_tasks_counter.sum_total() > 0 {
                log::debug!("terminating, waiting for {} tasks to finish | active = {:?}", active_tasks_counter.sum(), active_tasks_counter);
                process(ftd_rx.recv(), data, counter, &mut active_tasks_counter)?;
            }
            break;
        }

        let maybe_task_result = if (active_tasks_counter.sum() >= limits.active_tasks) ||
            (data.data.is_empty() && active_tasks_counter.inserts > 0)
        {
            Some(ftd_rx.recv())
        } else {
            match ftd_rx.try_recv() {
                Ok(order) =>
                    Some(Ok(order)),
                Err(mpsc::TryRecvError::Empty) =>
                    None,
                Err(mpsc::TryRecvError::Disconnected) =>
                    Some(Err(mpsc::RecvError)),
            }
        };

        match maybe_task_result {
            None =>
                (),
            Some(task_result) => {
                process(task_result, data, counter, &mut active_tasks_counter)?;
                continue;
            }
        }

        // construct action and run task
        if data.data.is_empty() || rng.sample(p_distribution) < limits.insert_or_remove_prob {
            // insert or remove task
            let db_size = limits.actions / 2;
            let insert_prob = 1.0 - (data.alive.len() as f64 / db_size as f64);
            let dice = rng.sample(p_distribution);
            if data.alive.is_empty() || dice < insert_prob {
                // insert task
                let key_amount = rng.sample(key_distribution);
                let value_amount = rng.sample(value_distribution);

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

                let job = InsertJob {
                    main: InsertJobMain {
                        blocks_pool: blocks_pool.clone(),
                        key_amount,
                        value_amount,
                        blockwheel_kv_meister: meister.clone(),
                        ftd_sendegeraet: ftd_sendegeraet.clone(),
                    },
                    job_complete: ftd_sendegeraet.rueckkopplung(InsertJobStamp),
                };
                edeltraud::job(&thread_pool, job)
                    .map_err(Error::Edeltraud)?;
                active_tasks_counter.inserts += 1;
                active_tasks_counter.insert_jobs += 1;
            } else {
                // remove task
                let key = loop {
                    let index = rng.gen_range(0 .. data.alive.len());
                    let &key_index = data.alive
                        .values()
                        .nth(index)
                        .unwrap();
                    let kv::KeyValuePair { key, value_cell, } = &data.data[key_index];
                    match &value_cell.cell {
                        kv::Cell::Value(..) =>
                            break key,
                        kv::Cell::Tombstone =>
                            continue,
                    }
                };

                log::debug!(
                    "{}. performing REMOVE with {} bytes key (dice = {:.3}, prob = {:.3}) | {:?}, active = {:?}",
                    actions_counter,
                    key.key_bytes.len(),
                    dice,
                    1.0 - insert_prob,
                    counter,
                    active_tasks_counter,
                );

                let rueckkopplung =
                    ftd_sendegeraet.rueckkopplung(ReplyRemove { key: key.clone(), });
                meister.remove(key.clone(), rueckkopplung, &thread_pool)
                    .map_err(Error::RequestRemove)?;
                active_tasks_counter.removes += 1;
            }
        } else {
            // lookup range task
            let key_index = rng.gen_range(0 .. data.data.len());
            let kv::KeyValuePair { key, value_cell, } = &data.data[key_index];

            log::debug!(
                "{}. performing LOOKUP_RANGE with {} bytes key and {} value | {:?}, active = {:?}",
                actions_counter,
                key.key_bytes.len(),
                match &value_cell.cell {
                    kv::Cell::Value(..) =>
                        "active",
                    kv::Cell::Tombstone =>
                        "tombstone",
                },
                counter,
                active_tasks_counter,
            );

            let key = key.clone();
            let value_cell = value_cell.clone();

            let rueckkopplung =
                ftd_sendegeraet.rueckkopplung(ReplyLookupRange { key: key.clone(), value_cell: value_cell.clone(), });
            let stream = meister
                .lookup_range(key.clone() ..= key.clone(), rueckkopplung, &thread_pool)
                .map_err(Error::RequestLookupRange)?;
            data.streams.insert(stream.stream_id().clone(), stream);
            active_tasks_counter.lookups_range += 1;
        }
        actions_counter += 1;
    }

    log::info!("DONE, performing flush");

    meister.flush(ftd_sendegeraet.rueckkopplung(ReplyFlush), &thread_pool)
        .map_err(Error::RequestFlush)?;
    match ftd_rx.recv() {
        Ok(ReplyOrder::Flush(komm::Umschlag { inhalt: blockwheel_kv::Flushed, stamp: ReplyFlush, })) =>
            (),
        other_order =>
            return Err(Error::UnexpectedFtdOrder(format!("expecting flush, but got {other_order:?}"))),
    }

    meister.info(ftd_sendegeraet.rueckkopplung(ReplyInfo), &thread_pool)
        .map_err(Error::RequestInfo)?;
    let info = match ftd_rx.recv() {
        Ok(ReplyOrder::Info(komm::Umschlag { inhalt: info, stamp: ReplyInfo, })) =>
            info,
        other_order =>
            return Err(Error::UnexpectedFtdOrder(format!("expecting info, but go {other_order:?}"))),
    };

    assert!(data.streams.is_empty());
    log::info!("FINAL INFO: {info:#?}");

    use std::sync::atomic::Ordering;
    log::info!("JOB_BLOCKWHEEL_FS: {}", job::JOB_BLOCKWHEEL_FS.load(Ordering::SeqCst));
    log::info!("JOB_PERFORMER_SKLAVE: {}", job::JOB_PERFORMER_SKLAVE.load(Ordering::SeqCst));
    log::info!("JOB_LOOKUP_RANGE_MERGE_SKLAVE: {}", job::JOB_LOOKUP_RANGE_MERGE_SKLAVE.load(Ordering::SeqCst));
    log::info!("JOB_FLUSH_BUTCHER_SKLAVE: {}", job::JOB_FLUSH_BUTCHER_SKLAVE.load(Ordering::SeqCst));
    log::info!("JOB_MERGE_SEARCH_TREES_SKLAVE: {}", job::JOB_MERGE_SEARCH_TREES_SKLAVE.load(Ordering::SeqCst));
    log::info!("JOB_DEMOLISH_SEARCH_TREE_SKLAVE: {}", job::JOB_DEMOLISH_SEARCH_TREE_SKLAVE.load(Ordering::SeqCst));

    Ok::<_, Error>(())
}

fn process(
    maybe_recv_order: Result<ReplyOrder, mpsc::RecvError>,
    data: &mut DataIndex,
    counter: &mut Counter,
    active_tasks_counter: &mut Counter,
)
    -> Result<(), Error>
{
    let recv_order = maybe_recv_order
        .map_err(|mpsc::RecvError| Error::FtdProcessIsLost)?;

    match recv_order {
        order @ ReplyOrder::Info(komm::Umschlag { stamp: ReplyInfo, .. }) =>
            Err(Error::UnexpectedFtdOrder(format!("unexpected info in process, got {order:?}"))),
        order @ ReplyOrder::Flush(komm::Umschlag { inhalt: blockwheel_kv::Flushed, stamp: ReplyFlush, }) =>
            Err(Error::UnexpectedFtdOrder(format!("unexpected flush in process, got {order:?}"))),
        ReplyOrder::Insert(komm::Umschlag { inhalt: blockwheel_kv::Inserted { version, }, stamp: ReplyInsert { key, value_crc }, }) => {
            let data_cell = kv::KeyValuePair {
                key: key.clone(),
                value_cell: kv::ValueCell {
                    version,
                    cell: kv::Cell::Value(value_crc),
                },
            };
            if let Some(&offset) = data.index.get(&key) {
                if data.data[offset].value_cell.version < data_cell.value_cell.version {
                    data.data[offset] = data_cell;
                    data.alive.insert(key.clone(), offset);
                }
            } else {
                let offset = data.data.len();
                data.data.push(data_cell);
                data.index.insert(key.clone(), offset);
                data.alive.insert(key.clone(), offset);
            }

            counter.inserts += 1;
            active_tasks_counter.inserts -= 1;
            Ok(())
        },
        ReplyOrder::InsertJob(komm::Umschlag { inhalt: output, stamp: InsertJobStamp, }) => {
            counter.insert_jobs += 1;
            active_tasks_counter.insert_jobs -= 1;
            output
        },
        ReplyOrder::Remove(komm::Umschlag { inhalt: blockwheel_kv::Removed { version, }, stamp: ReplyRemove { key, }, }) => {
            let data_cell = kv::KeyValuePair {
                key: key.clone(),
                value_cell: kv::ValueCell {
                    version,
                    cell: kv::Cell::Tombstone,
                },
            };
            let &offset = data.index.get(&key).unwrap();
            if data.data[offset].value_cell.version < data_cell.value_cell.version {
                data.data[offset] = data_cell;
                data.alive.remove(&key);
            }

            counter.removes += 1;
            active_tasks_counter.removes -= 1;
            Ok(())
        },
        ReplyOrder::LookupRange(komm::Umschlag {
            inhalt: komm::Streamzeug::Zeug {
                zeug: kv::KeyValuePair { value_cell: found_value_cell, .. },
                mehr,
            },
            stamp: ReplyLookupRange {
                key,
                value_cell: kv::ValueCell { version: version_snapshot, .. },
            },
        }) => {
            let stream_id = mehr.stream_id();
            let maybe_stream = data.streams.remove(stream_id);
            assert!(maybe_stream.is_some(), "lookup stream {:?} not found in `data.streams`", mehr);

            let &offset = data.index.get(&key).unwrap();
            let kv::KeyValuePair { value_cell: kv::ValueCell { version: version_current, cell: ref cell_current, }, .. } =
                data.data[offset];
            let kv::ValueCell { version: version_found, cell: ref cell_found, } = found_value_cell;
            if version_found == version_current {
                let is_equal = match (&cell_found, &cell_current) {
                    (kv::Cell::Tombstone, kv::Cell::Tombstone) =>
                        true,
                    (kv::Cell::Value(found_value), kv::Cell::Value(current_crc))
                        if blockwheel_fs::block::crc(&found_value.value_bytes) == *current_crc =>
                        true,
                    _ =>
                        false,
                };
                if is_equal {
                    // everything is up to date
                } else {
                    // version matches, but actual values are not
                    return Err(Error::UnexpectedValueFound {
                        key,
                        expected_value_cell: data.data[offset].value_cell.clone(),
                        found_value_cell,
                    });
                }
            } else if version_snapshot < version_current {
                // deprecated lookup (ignoring)
                log::debug!("deprecated lookup: awaiting version {} but there is {} already", version_snapshot, version_current);
            } else {
                // premature lookup (ignoring, don't want to wait)
                log::debug!(
                    "premature lookup: found version {} but current is still {} (awaiting {})",
                    version_found,
                    version_current,
                    version_snapshot,
                );
            }
            counter.lookups_range += 1;
            active_tasks_counter.lookups_range -= 1;
            Ok(())
        },
        ReplyOrder::LookupRange(komm::Umschlag {
            inhalt: komm::Streamzeug::NichtMehr(mehr),
            stamp: ReplyLookupRange { key, value_cell: kv::ValueCell { version: _version_snapshot, .. }, },
        }) => {
            let stream_id = mehr.stream_id();
            let maybe_stream = data.streams.remove(stream_id);
            assert!(maybe_stream.is_some());

            let &offset = data.index.get(&key).unwrap();
            let kv::KeyValuePair { value_cell: kv::ValueCell { version: _version_current, cell: ref cell_current, }, .. } =
                data.data[offset];
            match cell_current {
                kv::Cell::Tombstone => {
                    log::warn!("deprecated lookup: already removed during lookup_range");
                    Ok(())
                },
                kv::Cell::Value(..) =>
                    Err(Error::ExpectedValueNotFound { key, value_cell: data.data[offset].value_cell.clone(), }),
            }
        },
        order @ ReplyOrder::InfoCancel(komm::UmschlagAbbrechen { .. }) =>
            Err(Error::UnexpectedFtdOrder(format!("{order:?}"))),
        order @ ReplyOrder::InsertCancel(komm::UmschlagAbbrechen { .. }) =>
            Err(Error::UnexpectedFtdOrder(format!("{order:?}"))),
        order @ ReplyOrder::LookupRangeCancel(komm::UmschlagAbbrechen { .. }) =>
            Err(Error::UnexpectedFtdOrder(format!("{order:?}"))),
        order @ ReplyOrder::RemoveCancel(komm::UmschlagAbbrechen { .. }) =>
            Err(Error::UnexpectedFtdOrder(format!("{order:?}"))),
        order @ ReplyOrder::FlushCancel(komm::UmschlagAbbrechen { .. }) =>
            Err(Error::UnexpectedFtdOrder(format!("{order:?}"))),
        ReplyOrder::InsertJobCancel(komm::UmschlagAbbrechen { stamp: InsertJobStamp, }) =>
            Err(Error::InsertJobCanceled),
    }
}
