use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::mpsc as std_mpsc;
use std::time::Duration;

use async_trait::async_trait;
use glob::glob;
use notify::{Config, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info, warn};
use metrics;

use crate::event::{Event, Value};
use crate::sources::Source;

pub struct FileSource {
    pub name: String,
    pub include: Vec<String>,
}

impl FileSource {
    pub fn new(name: String, include: Vec<String>) -> Self {
        Self { name, include }
    }
}

#[async_trait]
impl Source for FileSource {
    async fn run(self: Box<Self>, tx: mpsc::Sender<Event>, mut shutdown: broadcast::Receiver<()>) {
        info!("FileSource[{}] starting with patterns: {:?}", self.name, self.include);

        // Map of path -> (File handle, current position)
        let mut readers: HashMap<PathBuf, BufReader<File>> = HashMap::new();
        
        // Channel to receive notify events
        let (notify_tx, notify_rx) = std_mpsc::channel();
        
        // Initialize watcher
        let mut watcher: RecommendedWatcher = match RecommendedWatcher::new(
            notify_tx, 
            Config::default().with_poll_interval(Duration::from_secs(1)) // fallback polling
        ) {
            Ok(w) => w,
            Err(e) => {
                error!("Failed to create watcher: {}", e);
                return;
            }
        };

        // 1. Resolve globs and open initial files
        for pattern in &self.include {
            match glob(pattern) {
                Ok(paths) => {
                    for entry in paths {
                        match entry {
                            Ok(path) => {
                                match File::open(&path) {
                                    Ok(mut file) => {
                                        // Seek to end initially
                                        if let Err(e) = file.seek(SeekFrom::End(0)) {
                                            warn!("Failed to seek file {:?}: {}", path, e);
                                            continue;
                                        }
                                        
                                        if let Err(e) = watcher.watch(&path, RecursiveMode::NonRecursive) {
                                            warn!("Failed to watch file {:?}: {}", path, e);
                                            continue;
                                        }

                                        readers.insert(path.clone(), BufReader::new(file));
                                        info!("Tailing file: {:?}", path);
                                    }
                                    Err(e) => {
                                        warn!("Failed to open file {:?}: {}", path, e);
                                    }
                                }
                            }
                            Err(e) => warn!("Glob error: {}", e),
                        }
                    }
                }
                Err(e) => warn!("Invalid glob pattern '{}': {}", pattern, e),
            }
        }

        if readers.is_empty() {
            warn!("FileSource: No files found to tail.");
        }

        // Bridge std channel to tokio channel
        let (async_tx, mut async_rx) = mpsc::channel::<notify::Event>(100);
        
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

        loop {
            tokio::select! {
                maybe_event = async_rx.recv() => {
                    match maybe_event {
                        Some(event) => {
                             match event.kind {
                                EventKind::Modify(_) => {
                                    for path in event.paths {
                                         if let Some(reader) = readers.get_mut(&path) {
                                             read_new_lines(&self.name, path.to_str().unwrap_or("unknown"), reader, &tx).await;
                                         }
                                    }
                                }
                                _ => {}
                            }
                        }
                        None => break,
                    }
                }
                _ = shutdown.recv() => {
                    info!("FileSource[{}] received shutdown signal", self.name);
                    break;
                }
            }
        }
    }
}

async fn read_new_lines(component: &str, source_name: &str, reader: &mut BufReader<File>, tx: &mpsc::Sender<Event>) {
    let mut line = String::new();
    loop {
        match reader.read_line(&mut line) {
            Ok(0) => break, // EOF reached
            Ok(_len) => {
                if line.ends_with('\n') {
                    line.pop();
                    if line.ends_with('\r') {
                        line.pop();
                    }
                }
                
                let mut event = Event::new();
                event.insert("message".to_string(), Value::from(line.clone()));
                event.insert("source".to_string(), Value::from(source_name.to_string()));
                event.insert("log_type".to_string(), Value::from("file"));
                
                if let Err(e) = tx.send(event).await {
                    error!("Failed to send event from FileSource: {}", e);
                    return;
                }
                metrics::increment_counter!("events_out", "component" => component.to_string());
                
                line.clear();
            }
            Err(e) => {
                warn!("Error reading line from {:?}: {}", source_name, e);
                metrics::increment_counter!("events_dropped", "component" => component.to_string(), "reason" => "read_error");
                break;
            }
        }
    }
}
