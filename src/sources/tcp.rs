use async_trait::async_trait;
use tokio::io::AsyncBufReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tracing::{info, warn, error};
use metrics;

use crate::event::{Event, EventEnvelope};
use crate::sources::Source;

pub struct TcpSource {
    pub name: String,
    pub address: String,
    pub max_length: usize,
}

impl TcpSource {
    pub fn new(name: String, address: String, max_length: usize) -> Self {
        Self { name, address, max_length }
    }

    async fn handle_conn(
        &self,
        stream: TcpStream,
        tx: mpsc::Sender<EventEnvelope>,
        mut shutdown: broadcast::Receiver<()>,
    ) {
        let peer = stream.peer_addr().ok().map(|p| p.to_string()).unwrap_or_else(|| "unknown".into());
        let (read_half, _) = tokio::io::split(stream);
        let reader = tokio::io::BufReader::new(read_half);
        let mut lines = reader.lines();

        loop {
            tokio::select! {
                res = lines.next_line() => {
                    match res {
                        Ok(Some(line)) => {
                            let msg = if line.len() > self.max_length { line[..self.max_length].to_string() } else { line };
                            let mut event = Event::new();
                            event.insert("message", msg);
                            event.insert("source", peer.clone());
                            let envelope = EventEnvelope::with_source(event, self.name.clone());
                            if let Err(err) = tx.send(envelope).await {
                                err.0.ack.ack();
                                break;
                            }
                            metrics::increment_counter!("events_out", "component" => self.name.clone());
                        }
                        Ok(None) => break,
                        Err(err) => {
                            warn!("TcpSource[{}] read error: {}", self.name, err);
                            metrics::increment_counter!("events_dropped", "component" => self.name.clone(), "reason" => "read_error");
                            break;
                        }
                    }
                }
                _ = shutdown.recv() => break,
            }
        }
    }
}

#[async_trait]
impl Source for TcpSource {
    async fn run(
        self: Box<Self>,
        tx: mpsc::Sender<EventEnvelope>,
        mut shutdown: broadcast::Receiver<()>,
    ) {
        let listener = match TcpListener::bind(&self.address).await {
            Ok(l) => l,
            Err(err) => {
                error!("TcpSource[{}] failed to bind {}: {}", self.name, self.address, err);
                return;
            }
        };
        info!("TcpSource[{}] listening on {}", self.name, self.address);

        loop {
            tokio::select! {
                res = listener.accept() => {
                    match res {
                        Ok((stream, _)) => {
                            let tx_conn = tx.clone();
                            let shutdown_conn = shutdown.resubscribe();
                            let src = self.clone();
                            tokio::spawn(async move {
                                src.handle_conn(stream, tx_conn, shutdown_conn).await;
                            });
                        }
                        Err(err) => warn!("TcpSource[{}] accept error: {}", self.name, err),
                    }
                }
                _ = shutdown.recv() => {
                    info!("TcpSource[{}] shutdown", self.name);
                    break;
                }
            }
        }
    }
}

impl Clone for TcpSource {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            address: self.address.clone(),
            max_length: self.max_length,
        }
    }
}
