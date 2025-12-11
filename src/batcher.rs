use std::time::{Duration, Instant};

use crate::config::BatchConfig;
use crate::event::Event;

pub struct Batcher {
    pub items: Vec<Event>,
    pub max_size: usize,
    pub timeout: Duration,
    pub last_flush: Instant,
}

impl Batcher {
    pub fn new(config: Option<BatchConfig>) -> Self {
        if let Some(c) = config {
            Self {
                items: Vec::with_capacity(c.max_events),
                max_size: c.max_events,
                timeout: Duration::from_secs(c.timeout_secs),
                last_flush: Instant::now(),
            }
        } else {
            // No batch config provided -> batch size 1 (immediate)
            Self {
                items: Vec::with_capacity(1),
                max_size: 1,
                timeout: Duration::ZERO,
                last_flush: Instant::now(),
            }
        }
    }

    pub fn add(&mut self, event: Event) {
        self.items.push(event);
    }

    pub fn is_full(&self) -> bool {
        self.items.len() >= self.max_size
    }

    pub fn should_flush(&self) -> bool {
        if self.items.is_empty() {
            return false;
        }
        if self.is_full() {
            return true;
        }
        if self.timeout.is_zero() {
            return true;
        }
        self.last_flush.elapsed() >= self.timeout
    }

    pub fn take(&mut self) -> Vec<Event> {
        self.last_flush = Instant::now();
        // Replace current buffer with a new one
        // If we expect consistent capacity, we might want to re-allocate with capacity.
        let items = std::mem::take(&mut self.items);
        // optimization: if we know we want to reuse the buffer, we could swap. 
        // But mem::take gives us the Vec, leaving an empty one.
        // We probably want to reserve capacity for the new one if we are high throughput.
        self.items.reserve(self.max_size);
        items
    }

    pub fn remaining_time(&self) -> Duration {
        if self.items.is_empty() {
            // Nothing to flush, so we can wait indefinitely (or a long time)
            return Duration::from_secs(3600);
        }
        let elapsed = self.last_flush.elapsed();
        if elapsed >= self.timeout {
            Duration::ZERO
        } else {
            self.timeout - elapsed
        }
    }
}

