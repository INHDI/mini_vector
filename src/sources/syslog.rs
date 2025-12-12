use async_trait::async_trait;
use tokio::io::AsyncBufReadExt;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{broadcast, mpsc};
use tracing::{info, warn, error};
use metrics;

use crate::event::Event;
use crate::sources::Source;

pub struct SyslogSource {
    pub name: String,
    pub mode: String,
    pub address: String,
    pub max_length: usize,
}

impl SyslogSource {
    pub fn new(name: String, mode: String, address: String, max_length: usize) -> Self {
        Self { name, mode, address, max_length }
    }

    async fn run_udp(&self, tx: mpsc::Sender<Event>, mut shutdown: broadcast::Receiver<()>) {
        let socket = match UdpSocket::bind(&self.address).await {
            Ok(s) => s,
            Err(err) => {
                error!("SyslogSource[{}] failed to bind {}: {}", self.name, self.address, err);
                return;
            }
        };
        info!("SyslogSource[{}] listening (udp) on {}", self.name, self.address);
        let mut buf = vec![0u8; self.max_length];
        loop {
            tokio::select! {
                res = socket.recv_from(&mut buf) => {
                    match res {
                        Ok((len, peer)) => {
                            let msg = String::from_utf8_lossy(&buf[..len]).to_string();
                            let mut event = Event::new();
                            event.insert("message", msg);
                            event.insert("source", peer.to_string());
                            if tx.send(event).await.is_err() {
                                break;
                            }
                            metrics::increment_counter!("events_out", "component" => self.name.clone());
                        }
                        Err(err) => {
                            warn!("SyslogSource[{}] udp recv error: {}", self.name, err);
                            metrics::increment_counter!("events_dropped", "component" => self.name.clone(), "reason" => "read_error");
                        }
                    }
                }
                _ = shutdown.recv() => {
                    info!("SyslogSource[{}] shutdown", self.name);
                    break;
                }
            }
        }
    }

    async fn handle_tcp_conn(&self, stream: TcpStream, tx: mpsc::Sender<Event>, mut shutdown: broadcast::Receiver<()>) {
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
                            if tx.send(event).await.is_err() {
                                break;
                            }
                            metrics::increment_counter!("events_out", "component" => self.name.clone());
                        }
                        Ok(None) => break,
                        Err(err) => {
                            warn!("SyslogSource[{}] tcp read error: {}", self.name, err);
                            metrics::increment_counter!("events_dropped", "component" => self.name.clone(), "reason" => "read_error");
                            break;
                        }
                    }
                }
                _ = shutdown.recv() => break,
            }
        }
    }

    async fn run_tcp(&self, tx: mpsc::Sender<Event>, mut shutdown: broadcast::Receiver<()>) {
        let listener = match TcpListener::bind(&self.address).await {
            Ok(l) => l,
            Err(err) => {
                error!("SyslogSource[{}] failed to bind {}: {}", self.name, self.address, err);
                return;
            }
        };
        info!("SyslogSource[{}] listening (tcp) on {}", self.name, self.address);
        loop {
            tokio::select! {
                res = listener.accept() => {
                    match res {
                        Ok((stream, _)) => {
                            let tx_conn = tx.clone();
                            let shutdown_conn = shutdown.resubscribe();
                            let src = self.clone();
                            tokio::spawn(async move {
                                src.handle_tcp_conn(stream, tx_conn, shutdown_conn).await;
                            });
                        }
                        Err(err) => {
                            warn!("SyslogSource[{}] accept error: {}", self.name, err);
                        }
                    }
                }
                _ = shutdown.recv() => {
                    info!("SyslogSource[{}] shutdown", self.name);
                    break;
                }
            }
        }
    }
}

impl Clone for SyslogSource {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            mode: self.mode.clone(),
            address: self.address.clone(),
            max_length: self.max_length,
        }
    }
}

#[async_trait]
impl Source for SyslogSource {
    async fn run(self: Box<Self>, tx: mpsc::Sender<Event>, shutdown: broadcast::Receiver<()>) {
        match self.mode.as_str() {
            "udp" => self.run_udp(tx, shutdown).await,
            "tcp" => self.run_tcp(tx, shutdown).await,
            _ => {
                warn!("SyslogSource[{}] unsupported mode '{}'", self.name, self.mode);
            }
        }
    }
}
