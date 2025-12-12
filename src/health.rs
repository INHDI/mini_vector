use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct HealthState {
    last_opensearch_ok: Arc<AtomicI64>,
    config_ok: Arc<AtomicBool>,
}

impl HealthState {
    pub fn new() -> Self {
        let now = now_secs();
        Self {
            last_opensearch_ok: Arc::new(AtomicI64::new(now)),
            config_ok: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn mark_config_ok(&self) {
        self.config_ok.store(true, Ordering::Relaxed);
    }

    pub fn mark_opensearch_ok(&self) {
        self.last_opensearch_ok
            .store(now_secs(), Ordering::Relaxed);
    }

    pub fn is_healthy(&self, threshold_secs: i64) -> bool {
        if !self.config_ok.load(Ordering::Relaxed) {
            return false;
        }
        let last_ok = self.last_opensearch_ok.load(Ordering::Relaxed);
        now_secs() - last_ok <= threshold_secs
    }
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs() as i64
}
