use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[derive(Default)]
pub(crate) struct IoDriverMetrics {
    pub(super) fd_count: AtomicU64,
    pub(super) ready_count: AtomicU64,
}

impl IoDriverMetrics {
    pub(crate) fn incr_fd_count(&self) {
        self.fd_count.fetch_add(1, Relaxed);
    }

    pub(crate) fn dec_fd_count(&self) {
        self.fd_count.fetch_sub(1, Relaxed);
    }

    pub(crate) fn incr_ready_count(&self) {
        self.ready_count.fetch_add(1, Relaxed);
    }
}
