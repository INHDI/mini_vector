use std::time::{Duration, Instant};

use crate::config::BatchConfig;
use crate::event::EventEnvelope;

pub struct Batch {
    pub events: Vec<EventEnvelope>,
    pub bytes: usize,
}

pub struct Batcher {
    pub items: Vec<EventEnvelope>,
    pub max_events: usize,
    pub max_bytes: usize,
    pub timeout: Duration,
    pub last_flush: Instant,
    pub current_bytes: usize,
}

impl Batcher {
    pub fn new(config: Option<BatchConfig>) -> Self {
        if let Some(c) = config {
            Self {
                items: Vec::with_capacity(c.max_events),
                max_events: c.max_events,
                max_bytes: c.max_bytes,
                timeout: Duration::from_secs(c.timeout_secs),
                last_flush: Instant::now(),
                current_bytes: 0,
            }
        } else {
            // No batch config provided -> batch size 1 (immediate)
            Self {
                items: Vec::with_capacity(1),
                max_events: 1,
                max_bytes: 0,
                timeout: Duration::ZERO,
                last_flush: Instant::now(),
                current_bytes: 0,
            }
        }
    }

    pub fn add(&mut self, event: EventEnvelope) {
        let bytes = estimate_event_size(&event);
        self.current_bytes = self.current_bytes.saturating_add(bytes);
        self.items.push(event);
    }

    pub fn is_full(&self) -> bool {
        if self.items.len() >= self.max_events {
            return true;
        }
        if self.max_bytes > 0 && self.current_bytes >= self.max_bytes {
            return true;
        }
        false
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

    pub fn take(&mut self) -> Batch {
        self.last_flush = Instant::now();
        let bytes = self.current_bytes;
        self.current_bytes = 0;
        let items = std::mem::take(&mut self.items);
        self.items.reserve(self.max_events);
        Batch {
            events: items,
            bytes,
        }
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

fn estimate_event_size(event: &EventEnvelope) -> usize {
    serde_json::to_vec(event).map(|v| v.len()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Event, EventEnvelope};

    fn simple_event() -> EventEnvelope {
        let mut ev = Event::new();
        ev.insert("message", "hello");
        EventEnvelope::new(ev)
    }

    #[test]
    fn flushes_by_event_count() {
        let mut b = Batcher::new(Some(BatchConfig {
            max_events: 2,
            timeout_secs: 10,
            max_bytes: 10_000,
        }));
        b.add(simple_event());
        assert!(!b.should_flush());
        b.add(simple_event());
        assert!(b.should_flush());
        let batch = b.take();
        assert_eq!(batch.events.len(), 2);
    }

    #[test]
    fn flushes_by_bytes() {
        let mut b = Batcher::new(Some(BatchConfig {
            max_events: 10,
            timeout_secs: 10,
            max_bytes: 20, // small
        }));
        b.add(simple_event());
        b.add(simple_event());
        assert!(b.should_flush());
        let batch = b.take();
        assert!(!batch.events.is_empty());
        assert!(batch.bytes > 0);
    }
}
