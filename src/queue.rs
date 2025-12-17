use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tracing::warn;

use crate::config::{QueueType, WhenFull};
use crate::event::{AckToken, Event, EventEnvelope, EventMetadata};

#[derive(Clone)]
#[allow(dead_code)]
pub struct QueueConfig {
    pub queue_type: QueueType,
    pub max_events: usize,
    pub max_bytes: usize,
    pub path: Option<String>,
}

pub enum EnqueueStatus {
    Enqueued,
    Dropped,
}

struct MemoryQueue {
    buf: Mutex<VecDeque<EventEnvelope>>,
    capacity: usize,
    available: Notify,
    space: Notify,
}

impl MemoryQueue {
    fn new(capacity: usize) -> Self {
        Self {
            buf: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            available: Notify::new(),
            space: Notify::new(),
        }
    }

    async fn push_blocking(&self, mut event: EventEnvelope) {
        loop {
            match self.try_push(event) {
                Ok(_) => return,
                Err(ev) => {
                    event = ev;
                }
            }
            self.space.notified().await;
        }
    }

    fn try_push(&self, event: EventEnvelope) -> Result<(), EventEnvelope> {
        let mut guard = self.buf.lock().unwrap();
        if guard.len() >= self.capacity {
            return Err(event);
        }
        guard.push_back(event);
        self.available.notify_one();
        Ok(())
    }

    fn push_drop_oldest(&self, event: EventEnvelope) -> EnqueueStatus {
        let mut guard = self.buf.lock().unwrap();
        if guard.len() >= self.capacity {
            guard.pop_front();
        }
        guard.push_back(event);
        self.available.notify_one();
        EnqueueStatus::Enqueued
    }

    fn pop_nowait(&self) -> Option<EventEnvelope> {
        let mut guard = self.buf.lock().unwrap();
        let ev = guard.pop_front();
        if ev.is_some() {
            self.space.notify_one();
        }
        ev
    }

    #[allow(dead_code)]
    async fn pop(&self) -> Option<EventEnvelope> {
        loop {
            if let Some(ev) = self.pop_nowait() {
                return Some(ev);
            }
            self.available.notified().await;
        }
    }

    fn available_notifier(&self) -> &Notify {
        &self.available
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct Position {
    segment: u64,
    offset: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct QueueState {
    read: Position,
    write: Position,
}

#[derive(Serialize, Deserialize)]
struct DiskEnvelope {
    event: Event,
    metadata: EventMetadata,
}

struct InflightEntry {
    end: Position,
    acked: bool,
}

struct DiskQueueInner {
    state: QueueState,
    write_file: File,
    read_file: Option<File>,
    current_read_segment: u64,
    pending_parents: BTreeMap<Position, AckToken>,
    inflight: BTreeMap<Position, InflightEntry>,
}

struct DiskQueueShared {
    dir: PathBuf,
    max_segment_bytes: u64,
    inner: Mutex<DiskQueueInner>,
    notify: Notify,
}

#[derive(Clone)]
pub struct DiskQueue {
    shared: Arc<DiskQueueShared>,
}

impl DiskQueue {
    pub fn new<P: AsRef<Path>>(path: P, max_segment_bytes: u64) -> anyhow::Result<Self> {
        let dir = path.as_ref().to_path_buf();
        let segments_dir = dir.join("segments");
        fs::create_dir_all(&segments_dir)?;

        let state = load_state(&dir)?;
        let write_path = segment_path(&segments_dir, state.write.segment);
        let write_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&write_path)?;
        let read_file = if state.read.segment <= state.write.segment {
            let rp = segment_path(&segments_dir, state.read.segment);
            Some(
                OpenOptions::new()
                    .create(true)
                    .read(true)
                    .open(rp)?,
            )
        } else {
            None
        };

        let read_segment = state.read.segment;
        let inner = DiskQueueInner {
            state,
            write_file,
            read_file,
            current_read_segment: read_segment,
            pending_parents: BTreeMap::new(),
            inflight: BTreeMap::new(),
        };

        Ok(Self {
            shared: Arc::new(DiskQueueShared {
                dir,
                max_segment_bytes,
                inner: Mutex::new(inner),
                notify: Notify::new(),
            }),
        })
    }

    pub async fn push(&self, envelope: EventEnvelope) -> anyhow::Result<()> {
        let shared = self.shared.clone();
        tokio::task::spawn_blocking(move || {
            let mut inner = shared.inner.lock().unwrap();
            let start = inner.state.write;
            let disk_env = DiskEnvelope {
                event: envelope.event,
                metadata: envelope.metadata,
            };
            let payload = serde_json::to_vec(&disk_env)?;
            let mut hasher = Hasher::new();
            hasher.update(&payload);
            let crc = hasher.finalize();

            inner
                .write_file
                .seek(SeekFrom::Start(start.offset))
                .map_err(anyhow::Error::from)?;
            inner
                .write_file
                .write_all(&(payload.len() as u32).to_le_bytes())
                .map_err(anyhow::Error::from)?;
            inner
                .write_file
                .write_all(&payload)
                .map_err(anyhow::Error::from)?;
            inner
                .write_file
                .write_all(&crc.to_le_bytes())
                .map_err(anyhow::Error::from)?;
            inner.write_file.sync_data().map_err(anyhow::Error::from)?;

            inner.pending_parents.insert(start, envelope.ack);

            let bytes_written = 8 + payload.len() as u64; // len + crc
            let mut next_pos = Position {
                segment: start.segment,
                offset: start.offset + bytes_written,
            };

            if next_pos.offset >= shared.max_segment_bytes {
                let next_segment = start.segment + 1;
                let seg_path = segment_path(&shared.dir.join("segments"), next_segment);
                inner.write_file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .truncate(true)
                    .open(seg_path)?;
                next_pos = Position {
                    segment: next_segment,
                    offset: 0,
                };
            }

            inner.state.write = next_pos;
            persist_state(&shared.dir, &inner.state)?;
            shared.notify.notify_one();
            Ok::<(), anyhow::Error>(())
        })
        .await??;

        Ok(())
    }

    pub async fn try_pop(&self) -> anyhow::Result<Option<EventEnvelope>> {
        let shared = self.shared.clone();
        tokio::task::spawn_blocking(move || {
            let mut inner = shared.inner.lock().unwrap();
            if inner.state.read == inner.state.write {
                return Ok(None);
            }

            let start = inner.state.read;
            ensure_read_file(&mut inner, start.segment, &shared.dir)?;
            let reader = inner
                .read_file
                .as_mut()
                .expect("read_file present after ensure");
            reader
                .seek(SeekFrom::Start(start.offset))
                .map_err(anyhow::Error::from)?;

            let mut len_bytes = [0u8; 4];
            reader.read_exact(&mut len_bytes).map_err(anyhow::Error::from)?;
            let payload_len = u32::from_le_bytes(len_bytes) as usize;
            let mut payload = vec![0u8; payload_len];
            reader.read_exact(&mut payload).map_err(anyhow::Error::from)?;
            let mut crc_bytes = [0u8; 4];
            reader.read_exact(&mut crc_bytes).map_err(anyhow::Error::from)?;
            let crc_read = u32::from_le_bytes(crc_bytes);
            let mut hasher = Hasher::new();
            hasher.update(&payload);
            let crc_now = hasher.finalize();
            if crc_read != crc_now {
                anyhow::bail!("DiskQueue checksum mismatch");
            }

            let disk_env: DiskEnvelope = serde_json::from_slice(&payload)?;
            let record_size = 8 + payload.len() as u64;
            let mut next_pos = Position {
                segment: start.segment,
                offset: start.offset + record_size,
            };

            // If we've reached end of segment and there is a newer segment, advance to it
            if next_pos.offset >= shared.max_segment_bytes {
                next_pos = Position {
                    segment: start.segment + 1,
                    offset: 0,
                };
            }

            let parent = inner.pending_parents.remove(&start);
            inner.inflight.insert(
                start,
                InflightEntry {
                    end: next_pos,
                    acked: false,
                },
            );

            let envelope = EventEnvelope {
                event: disk_env.event,
                metadata: disk_env.metadata,
                ack: AckToken::new(),
            };
            let queue_clone = DiskQueue {
                shared: shared.clone(),
            };
            let parent_ack = parent.unwrap_or_default();
            let ack_start = start;
            envelope.ack.on_complete(move || {
                queue_clone.complete_ack(ack_start);
                parent_ack.ack();
            });

            Ok(Some(envelope))
        })
        .await?
    }

    pub fn notifier(&self) -> &Notify {
        &self.shared.notify
    }

    fn complete_ack(&self, start: Position) {
        let mut inner = match self.shared.inner.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        if let Some(entry) = inner.inflight.get_mut(&start) {
            entry.acked = true;
        }

        let segments_dir = self.shared.dir.join("segments");
        loop {
            let read_pos = inner.state.read;
            let Some(entry) = inner.inflight.get(&read_pos) else {
                break;
            };
            if !entry.acked {
                break;
            }
            let next = entry.end;
            inner.inflight.remove(&read_pos);
            inner.state.read = next;
            if let Err(err) = persist_state(&self.shared.dir, &inner.state) {
                tracing::warn!("DiskQueue persist state failed: {}", err);
            }
            if next.segment > read_pos.segment {
                let old_seg = segment_path(&segments_dir, read_pos.segment);
                let _ = fs::remove_file(old_seg);
            }
        }
    }
}

fn ensure_read_file(
    inner: &mut DiskQueueInner,
    segment: u64,
    root: &Path,
) -> anyhow::Result<()> {
    if inner.read_file.is_none() || inner.current_read_segment != segment {
        let seg_path = segment_path(&root.join("segments"), segment);
        inner.read_file = Some(OpenOptions::new().read(true).open(seg_path)?);
        inner.current_read_segment = segment;
    }
    Ok(())
}

fn segment_path(dir: &Path, segment: u64) -> PathBuf {
    dir.join(format!("{:06}.log", segment))
}

fn load_state(dir: &Path) -> anyhow::Result<QueueState> {
    let state_path = dir.join("state.json");
    if let Ok(mut f) = File::open(&state_path) {
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        let state: QueueState = serde_json::from_slice(&buf)?;
        return Ok(state);
    }
    Ok(QueueState {
        read: Position {
            segment: 0,
            offset: 0,
        },
        write: Position {
            segment: 0,
            offset: 0,
        },
    })
}

fn persist_state(dir: &Path, state: &QueueState) -> anyhow::Result<()> {
    let tmp = dir.join("state.json.tmp");
    let path = dir.join("state.json");
    let data = serde_json::to_vec(state)?;
    fs::write(&tmp, &data)?;
    fs::rename(tmp, path)?;
    Ok(())
}

#[derive(Clone)]
pub struct SinkSender {
    inner: Arc<SinkQueueInner>,
}

pub struct SinkReceiver {
    inner: Arc<SinkQueueInner>,
}

struct SinkQueueInner {
    name: String,
    policy: WhenFull,
    memory: Arc<MemoryQueue>,
    spill: Option<DiskQueue>,
}

impl SinkQueueInner {
    async fn recv(&self) -> Option<EventEnvelope> {
        loop {
            if let Some(spill) = &self.spill {
                if let Ok(Some(ev)) = spill.try_pop().await {
                    return Some(ev);
                }
            }
            if let Some(ev) = self.memory.pop_nowait() {
                return Some(ev);
            }

            match &self.spill {
                Some(spill) => {
                    tokio::select! {
                        _ = self.memory.available_notifier().notified() => {},
                        _ = spill.notifier().notified() => {},
                    }
                }
                None => {
                    self.memory.available_notifier().notified().await;
                }
            }
        }
    }
}

impl SinkSender {
    pub async fn send(&self, event: EventEnvelope) -> EnqueueStatus {
        match self.inner.policy {
            WhenFull::Block => {
                self.inner.memory.push_blocking(event).await;
                EnqueueStatus::Enqueued
            }
            WhenFull::DropNew => match self.inner.memory.try_push(event) {
                Ok(_) => EnqueueStatus::Enqueued,
                Err(ev) => {
                    ev.ack.ack();
                    EnqueueStatus::Dropped
                }
            },
            WhenFull::DropOldest => self.inner.memory.push_drop_oldest(event),
            WhenFull::SpillToDisk => {
                match self.inner.memory.try_push(event) {
                    Ok(_) => EnqueueStatus::Enqueued,
                    Err(ev) => {
                        if let Some(spill) = &self.inner.spill {
                            if let Err(err) = spill.push(ev).await {
                                warn!(
                                    "SinkQueue[{}] spill push failed: {}",
                                    self.inner.name,
                                    err
                                );
                                EnqueueStatus::Dropped
                            } else {
                                EnqueueStatus::Enqueued
                            }
                        } else {
                            ev.ack.ack();
                            EnqueueStatus::Dropped
                        }
                    }
                }
            }
        }
    }
}

impl SinkReceiver {
    pub async fn recv(&self) -> Option<EventEnvelope> {
        self.inner.recv().await
    }
}

pub fn sink_queue(
    name: String,
    capacity: usize,
    policy: WhenFull,
    queue_type: QueueType,
    path: Option<PathBuf>,
    max_segment_bytes: u64,
) -> anyhow::Result<(SinkSender, SinkReceiver)> {
    let memory = Arc::new(MemoryQueue::new(capacity));
    let spill = match (policy, queue_type) {
        (WhenFull::SpillToDisk, QueueType::Disk) => {
            let base = path.unwrap_or_else(|| PathBuf::from("queue"));
            Some(DiskQueue::new(base, max_segment_bytes)?)
        }
        _ => None,
    };

    let inner = Arc::new(SinkQueueInner {
        name,
        policy,
        memory,
        spill,
    });
    Ok((
        SinkSender {
            inner: inner.clone(),
        },
        SinkReceiver { inner },
    ))
}
