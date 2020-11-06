use std::sync::{
    Arc,
    atomic::{
        Ordering,
        AtomicU64,
    },
};

#[derive(Clone)]
pub struct Provider {
    counter: Arc<AtomicU64>,
}

impl Provider {
    pub fn new() -> Provider {
        Provider {
            counter: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn obtain(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }
}
