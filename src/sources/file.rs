use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::mpsc as std_mpsc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use anyhow;
use glob::glob;
use metrics;
use notify::{Config, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::{broadcast, mpsc};
use tokio::time::{interval, Instant};
use tracing::{error, info, warn};

use crate::config::{ReadFrom, SourceConfig};
use crate::event::{Event, EventEnvelope, Value};
use crate::sources::Source;

#[cfg(target_family = "unix")]
use std::os::unix::fs::MetadataExt;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct FileState {
    dev: u64,
    inode: u64,
    path: String,
    offset: u64,
    size: u64,
    fingerprint: u32,
    updated_at: i64,
}

struct FileStateStore {
    db: sled::Db,
    last_flush: Instant,
    flush_interval: Duration,
}

impl FileStateStore {
    fn open(path: &str, flush_interval: Duration) -> anyhow::Result<Self> {
        let db = sled::open(path)?;
        Ok(Self {
            db,
            last_flush: Instant::now(),
            flush_interval,
        })
    }

    fn load(&self) -> anyhow::Result<HashMap<(u64, u64), FileState>> {
        let mut map = HashMap::new();
        for kv in self.db.iter() {
            let (k, v) = kv?;
            let state: FileState = serde_json::from_slice(&v)?;
            let key = Self::decode_key(&k)?;
            map.insert(key, state);
        }
        Ok(map)
    }

    fn save(&self, state: &FileState) -> anyhow::Result<()> {
        let key = Self::encode_key(state.dev, state.inode);
        let val = serde_json::to_vec(state)?;
        self.db.insert(key, val)?;
        if self.last_flush.elapsed() >= self.flush_interval {
            self.db.flush()?;
        }
        Ok(())
    }

    fn flush(&self) -> anyhow::Result<()> {
        self.db.flush()?;
        Ok(())
    }

    fn encode_key(dev: u64, inode: u64) -> Vec<u8> {
        format!("{}:{}", dev, inode).into_bytes()
    }

    fn decode_key(bytes: &[u8]) -> anyhow::Result<(u64, u64)> {
        let s = std::str::from_utf8(bytes)?;
        let mut parts = s.split(':');
        let dev = parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing dev"))?
            .parse::<u64>()?;
        let inode = parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing inode"))?
            .parse::<u64>()?;
        Ok((dev, inode))
    }
}

#[derive(Clone)]
pub struct FileSource {
    name: String,
    include: Vec<String>,
    read_from: ReadFrom,
    state_path: String,
    flush_interval: Duration,
    scan_interval: Duration,
    fingerprint_bytes: usize,
}

#[derive(Debug)]
struct TrackedFile {
    path: PathBuf,
    dev: u64,
    inode: u64,
    offset: u64,
    fingerprint: u32,
    reader: BufReader<File>,
}

impl FileSource {
    pub fn new(name: String, cfg: SourceConfig) -> Self {
        let read_from = cfg.read_from.unwrap_or(ReadFrom::End);
        let state_path = cfg
            .state_path
            .unwrap_or_else(|| "file_state.db".to_string());
        let flush_interval = Duration::from_secs(cfg.state_flush_secs.unwrap_or(5));
        let scan_interval = Duration::from_secs(cfg.scan_interval_secs.unwrap_or(5));
        let fingerprint_bytes = cfg.fingerprint_bytes.unwrap_or(4096).max(1);

        Self {
            name,
            include: cfg.include,
            read_from,
            state_path,
            flush_interval,
            scan_interval,
            fingerprint_bytes,
        }
    }
}

#[async_trait]
impl Source for FileSource {
    async fn run(
        self: Box<Self>,
        tx: mpsc::Sender<EventEnvelope>,
        mut shutdown: broadcast::Receiver<()>,
    ) {
        info!(
            "FileSource[{}] starting with patterns: {:?}",
            self.name, self.include
        );

        let store = match FileStateStore::open(&self.state_path, self.flush_interval) {
            Ok(s) => s,
            Err(err) => {
                error!("FileSource[{}] failed to open state store: {}", self.name, err);
                return;
            }
        };
        let mut state_cache = match store.load() {
            Ok(m) => m,
            Err(err) => {
                warn!(
                    "FileSource[{}] failed to load state (starting fresh): {}",
                    self.name, err
                );
                HashMap::new()
            }
        };

        // Tracking maps
        let mut tracked: HashMap<(u64, u64), TrackedFile> = HashMap::new();
        let mut path_index: HashMap<PathBuf, (u64, u64)> = HashMap::new();

        // Channel to receive notify events
        let (notify_tx, notify_rx) = std_mpsc::channel();

        // Initialize watcher
        let mut watcher: RecommendedWatcher = match RecommendedWatcher::new(
            notify_tx,
            Config::default().with_poll_interval(Duration::from_secs(1)),
        ) {
            Ok(w) => w,
            Err(e) => {
                error!("Failed to create watcher: {}", e);
                return;
            }
        };

        // Initial scan
        if let Err(err) = initial_scan(
            &self.include,
            &mut watcher,
            &mut tracked,
            &mut path_index,
            &mut state_cache,
            &store,
            self.read_from,
            self.fingerprint_bytes,
        )
        .await
        {
            warn!("FileSource[{}] initial scan error: {}", self.name, err);
        }

        // Bridge std channel to tokio channel
        let (async_tx, mut async_rx) = mpsc::channel::<notify::Event>(200);
        let _watcher_task = std::thread::spawn(move || {
            while let Ok(res) = notify_rx.recv() {
                match res {
                    Ok(event) => {
                        if async_tx.blocking_send(event).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        warn!("Notify error: {}", e);
                    }
                }
            }
        });

        let mut scan_tick = interval(self.scan_interval);

        loop {
            tokio::select! {
                maybe_event = async_rx.recv() => {
                    if let Some(event) = maybe_event {
                        if matches!(event.kind, EventKind::Modify(_) | EventKind::Create(_)) {
                            for path in event.paths {
                                if let Err(err) = refresh_path(
                                    &path,
                                    &mut watcher,
                                    &mut tracked,
                                    &mut path_index,
                                    &mut state_cache,
                                    &store,
                                    self.read_from,
                                    self.fingerprint_bytes,
                                )
                                .await
                                {
                                    warn!("FileSource[{}] refresh {:?} failed: {}", self.name, path, err);
                                }
                                if let Some(key) = path_index.get(&path).copied() {
                                    read_new_lines(&self.name, &path, key, &mut tracked, &tx, &store, &mut state_cache).await;
                                }
                            }
                        }
                    } else {
                        break;
                    }
                }
                _ = scan_tick.tick() => {
                    if let Err(err) = rescan_patterns(
                        &self.include,
                        &mut watcher,
                        &mut tracked,
                        &mut path_index,
                        &mut state_cache,
                        &store,
                        self.read_from,
                        self.fingerprint_bytes,
                    ).await {
                        warn!("FileSource[{}] rescan error: {}", self.name, err);
                    }
                }
                _ = shutdown.recv() => {
                    info!("FileSource[{}] received shutdown signal", self.name);
                    break;
                }
            }
        }

        if let Err(err) = store.flush() {
            warn!("FileSource[{}] state flush failed: {}", self.name, err);
        }
    }
}

async fn initial_scan(
    patterns: &[String],
    watcher: &mut RecommendedWatcher,
    tracked: &mut HashMap<(u64, u64), TrackedFile>,
    path_index: &mut HashMap<PathBuf, (u64, u64)>,
    state_cache: &mut HashMap<(u64, u64), FileState>,
    store: &FileStateStore,
    read_from: ReadFrom,
    fingerprint_bytes: usize,
) -> anyhow::Result<()> {
    for pattern in patterns {
        match glob(pattern) {
            Ok(paths) => {
                for entry in paths.flatten() {
                    if let Err(err) = watcher.watch(&entry, RecursiveMode::NonRecursive) {
                        warn!("Failed to watch file {:?}: {}", entry, err);
                    }
                    if let Err(err) = refresh_path(
                        &entry,
                        watcher,
                        tracked,
                        path_index,
                        state_cache,
                        store,
                        read_from,
                        fingerprint_bytes,
                    )
                    .await
                    {
                        warn!("Failed to track {:?}: {}", entry, err);
                    }
                }
            }
            Err(e) => warn!("Invalid glob pattern '{}': {}", pattern, e),
        }
    }
    Ok(())
}

async fn rescan_patterns(
    patterns: &[String],
    watcher: &mut RecommendedWatcher,
    tracked: &mut HashMap<(u64, u64), TrackedFile>,
    path_index: &mut HashMap<PathBuf, (u64, u64)>,
    state_cache: &mut HashMap<(u64, u64), FileState>,
    store: &FileStateStore,
    read_from: ReadFrom,
    fingerprint_bytes: usize,
) -> anyhow::Result<()> {
    initial_scan(
        patterns,
        watcher,
        tracked,
        path_index,
        state_cache,
        store,
        read_from,
        fingerprint_bytes,
    )
    .await
}

async fn refresh_path(
    path: &Path,
    watcher: &mut RecommendedWatcher,
    tracked: &mut HashMap<(u64, u64), TrackedFile>,
    path_index: &mut HashMap<PathBuf, (u64, u64)>,
    state_cache: &mut HashMap<(u64, u64), FileState>,
    store: &FileStateStore,
    read_from: ReadFrom,
    fingerprint_bytes: usize,
) -> anyhow::Result<()> {
    let meta = match std::fs::metadata(path) {
        Ok(m) => m,
        Err(err) => anyhow::bail!("metadata failed: {}", err),
    };
    let (dev, inode) = file_key(&meta);
    let key = (dev, inode);

    // If already tracked, ensure path index updated
    if tracked.contains_key(&key) {
        path_index.insert(path.to_path_buf(), key);
        return Ok(());
    }

    let mut file = OpenOptions::new().read(true).open(path)?;
    let fingerprint = compute_fingerprint(&mut file, fingerprint_bytes)?;

    let mut offset = match state_cache.get(&key) {
        Some(state) => {
            if state.fingerprint == fingerprint && state.size >= state.offset {
                state.offset
            } else {
                0
            }
        }
        None => match read_from {
            ReadFrom::Beginning => 0,
            ReadFrom::End => meta.len(),
        },
    };

    // Detect truncate/replace
    if meta.len() < offset {
        offset = 0;
    }

    file.seek(SeekFrom::Start(offset))?;
    let reader = BufReader::new(file);

    tracked.insert(
        key,
        TrackedFile {
            path: path.to_path_buf(),
            dev,
            inode,
            offset,
            fingerprint,
            reader,
        },
    );
    path_index.insert(path.to_path_buf(), key);

    // ensure watcher is on path
    if let Err(err) = watcher.watch(path, RecursiveMode::NonRecursive) {
        warn!("Failed to watch file {:?}: {}", path, err);
    }

    // Persist initial state
    let state = FileState {
        dev,
        inode,
        path: path.to_string_lossy().to_string(),
        offset,
        size: meta.len(),
        fingerprint,
        updated_at: now_ts(),
    };
    store.save(&state)?;
    state_cache.insert(key, state);

    info!("Tracking file {:?} (dev={}, inode={}, offset={})", path, dev, inode, offset);
    Ok(())
}

async fn read_new_lines(
    component: &str,
    path: &Path,
    key: (u64, u64),
    tracked: &mut HashMap<(u64, u64), TrackedFile>,
    tx: &mpsc::Sender<EventEnvelope>,
    store: &FileStateStore,
    state_cache: &mut HashMap<(u64, u64), FileState>,
) {
    let Some(file) = tracked.get_mut(&key) else {
        return;
    };

    // Check for truncate
    let meta = match std::fs::metadata(&file.path) {
        Ok(m) => m,
        Err(err) => {
            warn!("FileSource[{}] stat failed for {:?}: {}", component, file.path, err);
            return;
        }
    };

    if meta.len() < file.offset {
        if let Err(err) = file.reader.get_mut().seek(SeekFrom::Start(0)) {
            warn!("FileSource[{}] seek reset failed: {}", component, err);
            return;
        }
        file.offset = 0;
    }

    let mut line = String::new();
    loop {
        match file.reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(read_len) => {
                file.offset += read_len as u64;
                if line.ends_with('\n') {
                    line.pop();
                    if line.ends_with('\r') {
                        line.pop();
                    }
                }

                let mut event = Event::new();
                event.insert("message".to_string(), Value::from(line.clone()));
                event.insert("source".to_string(), Value::from(path.to_string_lossy().to_string()));
                event.insert("log_type".to_string(), Value::from("file"));

                let envelope = EventEnvelope::with_source(event, component.to_string());
                if let Err(e) = tx.send(envelope).await {
                    warn!("Failed to send event from FileSource: {}", e);
                    e.0.ack.ack();
                    break;
                }
                metrics::increment_counter!("events_out", "component" => component.to_string());
                line.clear();
            }
            Err(err) => {
                warn!("Error reading line from {:?}: {}", path, err);
                metrics::increment_counter!(
                    "events_dropped",
                    "component" => component.to_string(),
                    "reason" => "read_error"
                );
                break;
            }
        }
    }

    let state = FileState {
        dev: file.dev,
        inode: file.inode,
        path: path.to_string_lossy().to_string(),
        offset: file.offset,
        size: meta.len(),
        fingerprint: file.fingerprint,
        updated_at: now_ts(),
    };
    if let Err(err) = store.save(&state) {
        warn!("FileSource[{}] failed to persist state: {}", component, err);
    } else {
        state_cache.insert(key, state);
    }
}

fn compute_fingerprint(file: &mut File, max_bytes: usize) -> anyhow::Result<u32> {
    let mut buf = vec![0u8; max_bytes];
    let mut hasher = crc32fast::Hasher::new();
    let read = file.read(&mut buf)?;
    hasher.update(&buf[..read]);
    // Reset cursor
    file.seek(SeekFrom::Start(0))?;
    Ok(hasher.finalize())
}

fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn file_key(meta: &std::fs::Metadata) -> (u64, u64) {
    #[cfg(target_family = "unix")]
    {
        (meta.dev(), meta.ino())
    }
    #[cfg(not(target_family = "unix"))]
    {
        (0, meta.len())
    }
}
