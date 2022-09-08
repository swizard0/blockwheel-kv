#[forbid(unsafe_code)]

use std::{
    ops::{
        AddAssign,
        RangeBounds,
    },
};

use alloc_pool::{
    bytes::{
        BytesPool,
    },
};

use arbeitssklave::{
    komm,
};

pub mod kv;
pub mod job;
pub mod wheels;
pub mod version;

mod core;
mod storage;
// mod access_policy;

// #[cfg(test)]
// mod tests;

#[derive(Clone, Debug)]
pub struct Params {
    pub butcher_block_size: usize,
    pub tree_block_size: usize,
    pub search_tree_values_inline_size_limit: usize,
    pub search_tree_bootstrap_search_trees_limit: usize,
}

impl Default for Params {
    fn default() -> Params {
        Params {
            butcher_block_size: 128,
            tree_block_size: 32,
            search_tree_values_inline_size_limit: 128,
            search_tree_bootstrap_search_trees_limit: 16,
        }
    }
}

pub trait AccessPolicy: Sized + Send + 'static
where Self::Order: From<komm::UmschlagAbbrechen<Self::Info>>,
      Self::Order: From<komm::Umschlag<Info, Self::Info>>,
      Self::Order: From<komm::UmschlagAbbrechen<Self::FlushAll>>,
      Self::Order: From<komm::Umschlag<Flushed, Self::FlushAll>>,
      Self::Order: From<komm::UmschlagAbbrechen<Self::Insert>>,
      Self::Order: From<komm::Umschlag<Inserted, Self::Insert>>,
      Self::Order: From<komm::UmschlagAbbrechen<Self::Remove>>,
      Self::Order: From<komm::Umschlag<Removed, Self::Remove>>,
      Self::Order: From<komm::UmschlagAbbrechen<Self::LookupRange>>,
      Self::Order: From<komm::Umschlag<KeyValueStreamItem<Self>, Self::LookupRange>>,
      Self::Order: Send + 'static,
      Self::Info: Send + 'static,
      Self::FlushAll: Send + 'static,
      Self::Insert: Send + 'static,
      Self::Remove: Send + 'static,
      Self::LookupRange: Send + 'static,
{
    type Order;
    type Info;
    type FlushAll;
    type Insert;
    type Remove;
    type LookupRange;
}

pub enum KeyValueStreamItem<A> where A: AccessPolicy {
    KeyValue {
        key_value_pair: kv::KeyValuePair<kv::Value>,
        next: LookupRangeStream<A>,
    },
    NoMore,
}

pub struct LookupRangeStream<A> where A: AccessPolicy {
    next: komm::Rueckkopplung<core::performer_sklave::Order<A>, core::performer_sklave::LookupRangeRoute>,
}

impl<A> LookupRangeStream<A> where A: AccessPolicy {
    pub fn next(self, rueckkopplung: komm::Rueckkopplung<A::Order, A::LookupRange>) -> Result<(), komm::Error> {
        self.next.commit(core::performer_sklave::LookupRangeStreamNext { rueckkopplung, })
    }
}

#[derive(Debug)]
pub enum Error {
    PerformerVersklaven(arbeitssklave::Error),
}

pub struct Freie<A> where A: AccessPolicy {
    performer_sklave_freie: arbeitssklave::Freie<core::performer_sklave::Welt<A>, core::performer_sklave::Order<A>>,
}

impl<A> Freie<A> where A: AccessPolicy {
    pub fn new() -> Self {
        Self {
            performer_sklave_freie: arbeitssklave::Freie::new(),
        }
    }

    pub fn versklaven<P>(
        self,
        params: Params,
        blocks_pool: BytesPool,
        version_provider: version::Provider,
        wheels: wheels::Wheels<A>,
        thread_pool: &P,
    )
        -> Result<Meister<A>, Error>
    where P: edeltraud::ThreadPool<job::Job<A>> + Clone + Send + 'static,
    {
        let welt = core::performer_sklave::Welt {
            env: core::performer_sklave::Env {
                params,
                blocks_pool,
                version_provider,
                wheels,
                incoming_orders: Vec::new(),
                delayed_orders: Vec::new(),
            },
            kont: core::performer_sklave::Kont::Initialize,
        };

        let performer_sklave_meister = self
            .performer_sklave_freie
            .versklaven(welt, thread_pool)
            .map_err(Error::PerformerVersklaven)?;

        Ok(Meister {
            performer_sklave_meister,
        })
    }
}

pub struct Meister<A> where A: AccessPolicy {
    performer_sklave_meister: arbeitssklave::Meister<core::performer_sklave::Welt<A>, core::performer_sklave::Order<A>>,
}

impl<A> Clone for Meister<A> where A: AccessPolicy {
    fn clone(&self) -> Self {
        Meister {
            performer_sklave_meister: self.performer_sklave_meister.clone(),
        }
    }
}


//     manager_pid: core::manager::Pid,
// }

// #[derive(Clone)]
// pub struct Pid {
//     manager_pid: core::manager::Pid,
// }

// impl GenServer {
//     pub fn new() -> GenServer {
//         let manager_gen_server = core::manager::GenServer::new();
//         let manager_pid = manager_gen_server.pid();
//         GenServer {
//             manager_gen_server,
//             manager_pid,
//         }
//     }

//     pub fn pid(&self) -> Pid {
//         Pid {
//             manager_pid: self.manager_pid.clone(),
//         }
//     }

//     pub async fn run<P>(
//         self,
//         mut parent_supervisor: SupervisorPid,
//         thread_pool: P,
//         blocks_pool: BytesPool,
//         version_provider: version::Provider,
//         wheels: wheels::Wheels,
//         params: Params,
//     )
//     where P: edeltraud::ThreadPool<job::Job> + Clone,
//     {
//         let manager_params = core::manager::Params {
//             task_restart_sec: params.manager_task_restart_sec,
//             performer_params: core::performer::Params {
//                 butcher_block_size: params.butcher_block_size,
//                 tree_block_size: params.tree_block_size,
//                 bootstrap_search_trees_limit: params.search_tree_bootstrap_search_trees_limit,
//                 values_inline_size_limit: params.search_tree_values_inline_size_limit,
//             },
//             iter_send_buffer: params.iter_send_buffer,
//         };

//         let child_supervisor_gen_server = parent_supervisor.child_supervisor();
//         let child_supervisor_pid = child_supervisor_gen_server.pid();
//         parent_supervisor.spawn_link_permanent(
//             child_supervisor_gen_server.run(),
//         );

//         let manager_task = self.manager_gen_server.run(
//             child_supervisor_pid.clone(),
//             thread_pool,
//             blocks_pool,
//             version_provider,
//             wheels,
//             manager_params,
//         );
//         manager_task.await
//     }
// }

// #[derive(Debug)]
// pub enum InsertError {
//     GenServer(ero::NoProcError),
// }

// #[derive(Debug)]
// pub enum LookupError {
//     GenServer(ero::NoProcError),
// }

// #[derive(Debug)]
// pub enum LookupRangeError {
//     GenServer(ero::NoProcError),
// }

// #[derive(Debug)]
// pub enum RemoveError {
//     GenServer(ero::NoProcError),
// }

// #[derive(Debug)]
// pub enum FlushError {
//     GenServer(ero::NoProcError),
// }

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Inserted {
    pub version: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Removed {
    pub version: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Flushed;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, Debug)]
pub struct Info {
    pub alive_cells_count: usize,
    pub tombstones_count: usize,
}

// pub struct LookupRange {
//     pub key_values_rx: mpsc::Receiver<KeyValueStreamItem>,
// }

// #[derive(Clone)]
// pub enum KeyValueStreamItem {
//     KeyValue(kv::KeyValuePair<kv::Value>),
//     NoMore,
// }

// impl Pid {
//     pub async fn info(&mut self) -> Result<Info, ero::NoProcError> {
//         self.manager_pid.info().await
//     }

//     pub async fn insert(&mut self, key: kv::Key, value: kv::Value) -> Result<Inserted, InsertError> {
//         self.manager_pid.insert(key, value).await
//             .map_err(|core::manager::InsertError::GenServer(ero::NoProcError)| InsertError::GenServer(ero::NoProcError))
//     }

//     pub async fn lookup(&mut self, key: kv::Key) -> Result<Option<kv::ValueCell<kv::Value>>, LookupError> {
//         self.manager_pid.lookup(key).await
//             .map_err(|core::manager::LookupError::GenServer(ero::NoProcError)| LookupError::GenServer(ero::NoProcError))
//     }

//     pub async fn lookup_range<R>(&mut self, range: R) -> Result<LookupRange, LookupRangeError> where R: RangeBounds<kv::Key> {
//         self.manager_pid.lookup_range(range).await
//             .map_err(|core::manager::LookupRangeError::GenServer(ero::NoProcError)| LookupRangeError::GenServer(ero::NoProcError))
//     }

//     pub async fn remove(&mut self, key: kv::Key) -> Result<Removed, RemoveError> {
//         self.manager_pid.remove(key).await
//             .map_err(|core::manager::RemoveError::GenServer(ero::NoProcError)| RemoveError::GenServer(ero::NoProcError))
//     }

//     pub async fn flush(&mut self) -> Result<Flushed, FlushError> {
//         self.manager_pid.flush_all().await
//             .map_err(|core::manager::FlushError::GenServer(ero::NoProcError)| FlushError::GenServer(ero::NoProcError))
//     }
// }

impl AddAssign for Info {
    fn add_assign(&mut self, rhs: Info) {
        self.alive_cells_count += rhs.alive_cells_count;
        self.tombstones_count += rhs.tombstones_count;
    }
}

impl Info {
    pub fn reset(&mut self) {
        self.alive_cells_count = 0;
        self.tombstones_count = 0;
    }
}
