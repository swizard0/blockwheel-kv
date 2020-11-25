use std::{
    time,
    sync::{
        Arc,
        atomic::{
            Ordering,
            AtomicU64,
        },
    },
};

#[derive(Clone)]
pub struct Provider {
    counter: Arc<AtomicU64>,
}

impl Provider {
    pub fn from_unix_epoch_seed() -> Provider {
        let seconds_since_epoch = time::SystemTime::now()
            .duration_since(time::SystemTime::UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0);
        let initial_counter = seconds_since_epoch << 24;
        Provider {
            counter: Arc::new(AtomicU64::new(initial_counter)),
        }
    }

    pub fn obtain(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }
}
