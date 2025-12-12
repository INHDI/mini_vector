//! Queue abstraction placeholder for future disk buffering support.
//! Provides a trait and config skeleton to plug in an on-disk queue later.

use crate::config::QueueType;
use crate::event::Event;

#[allow(dead_code)]
pub struct QueueConfig {
    pub queue_type: QueueType,
    pub max_events: usize,
    pub max_bytes: usize,
    pub path: Option<String>,
}

#[allow(dead_code)]
pub trait EventQueue: Send + Sync {
    fn push(&mut self, event: Event) -> anyhow::Result<()>;
    fn pop(&mut self) -> anyhow::Result<Option<Event>>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// In-memory queue placeholder; currently we rely on mpsc channels in the pipeline.
#[allow(dead_code)]
pub struct InMemoryQueue;

#[allow(dead_code)]
impl EventQueue for InMemoryQueue {
    fn push(&mut self, _event: Event) -> anyhow::Result<()> {
        anyhow::bail!("InMemoryQueue not implemented; use mpsc channels instead")
    }

    fn pop(&mut self) -> anyhow::Result<Option<Event>> {
        Ok(None)
    }

    fn len(&self) -> usize {
        0
    }
}

/// Disk-backed queue placeholder to be implemented in a later checklist.
#[allow(dead_code)]
pub struct DiskQueue;

#[allow(dead_code)]
impl EventQueue for DiskQueue {
    fn push(&mut self, _event: Event) -> anyhow::Result<()> {
        anyhow::bail!("DiskQueue not implemented yet")
    }

    fn pop(&mut self) -> anyhow::Result<Option<Event>> {
        Ok(None)
    }

    fn len(&self) -> usize {
        0
    }
}
