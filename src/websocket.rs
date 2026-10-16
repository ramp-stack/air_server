use crate::Connection;
use futures_util::stream::FuturesUnordered;
use futures_util::{SinkExt, StreamExt};
use rustls::RootCertStore;
use rustls::pki_types::ServerName;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio::time::Instant;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{Message, protocol::WebSocketConfig};
use tokio_tungstenite::{
    WebSocketStream, accept_async_with_config, client_async_with_config,
};
use tokio_util::sync::CancellationToken;

type Write = (Vec<u8>, oneshot::Sender<Result<(), String>>);

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// One framed connection accepted by a listener or owned by a client session.
pub struct Socket {
    incoming: mpsc::Receiver<Vec<u8>>,
    outgoing: mpsc::Sender<Write>,
    failure: watch::Receiver<String>,
    cancel: CancellationToken,
}

impl Socket {
    // Reading continues while a write flushes, even when both peers send
    // large messages at once. Protocol credit bounds the buffered messages.
    fn from_websocket(websocket: WebSocketStream<Box<dyn Io>>) -> Self {
        let (mut sink, mut stream) = websocket.split();
        let (incoming, rx) = mpsc::channel(4);
        let (tx, mut outgoing) = mpsc::channel::<Write>(1);
        let (failure, error) = watch::channel("websocket closed".to_string());
        let cancel = CancellationToken::new();
        let pong = Arc::new(Notify::new());
        let reader_cancel = cancel.clone();
        let reader_failure = failure.clone();
        let reader_pong = pong.clone();
        tokio::spawn(async move {
            let reading = async {
                loop {
                    let frame = tokio::time::timeout(
                        Duration::from_secs(90),
                        stream.next(),
                    )
                    .await
                    .map_err(|_| "websocket peer stopped responding")?
                    .ok_or("websocket closed")?
                    .map_err(|error| error.to_string())?;
                    match frame {
                        Message::Binary(bytes) => incoming
                            .send(bytes.to_vec())
                            .await
                            .map_err(|_| "websocket reader dropped")?,
                        Message::Ping(_) => reader_pong.notify_one(),
                        Message::Close(_) => {
                            return Err("websocket peer closed".to_string());
                        }
                        _ => {}
                    }
                }
            };
            let result: Result<(), String> = tokio::select! {
                _ = reader_cancel.cancelled() => return,
                result = reading => result,
            };
            if let Err(error) = result {
                let _ = reader_failure.send(error);
            }
            reader_cancel.cancel();
        });
        let writer_cancel = cancel.clone();
        tokio::spawn(async move {
            let writing = async {
                let mut heartbeat = tokio::time::interval_at(
                    Instant::now() + Duration::from_secs(30),
                    Duration::from_secs(30),
                );
                heartbeat.set_missed_tick_behavior(
                    tokio::time::MissedTickBehavior::Skip,
                );
                loop {
                    let (message, reply) = tokio::select! {
                        command = outgoing.recv() => {
                            let (bytes, reply) =
                                command.ok_or("websocket writer dropped")?;
                            (Some(Message::Binary(bytes.into())), Some(reply))
                        }
                        _ = pong.notified() => (None, None),
                        _ = heartbeat.tick() => {
                            (Some(Message::Ping(Vec::new().into())), None)
                        },
                    };
                    let result =
                        tokio::time::timeout(Duration::from_secs(10), async {
                            match message {
                                Some(message) => sink.send(message).await,
                                None => sink.flush().await,
                            }
                            .map_err(|error| error.to_string())
                        })
                        .await
                        .map_err(|_| "websocket write timed out".to_string())
                        .and_then(|result| result);
                    if let Some(reply) = reply {
                        let _ = reply.send(result.clone());
                    }
                    result?;
                }
            };
            let result: Result<(), String> = tokio::select! {
                _ = writer_cancel.cancelled() => return,
                result = writing => result,
            };
            if let Err(error) = result {
                let _ = failure.send(error);
            }
            writer_cancel.cancel();
        });
        Self { incoming: rx, outgoing: tx, failure: error, cancel }
    }

    pub async fn listen(sockets: mpsc::Sender<Self>) -> Result<(), String> {
        Self::serve(sockets, None).await
    }

    pub async fn listen_tls(sockets: mpsc::Sender<Self>) -> Result<(), String> {
        let cert_path = std::env::var("AIR_TLS_CERT")
            .map_err(|_| "AIR_TLS_CERT is not configured")?;
        let key_path = std::env::var("AIR_TLS_KEY")
            .map_err(|_| "AIR_TLS_KEY is not configured")?;
        let cert_bytes = std::fs::read(cert_path)
            .map_err(|e| format!("read TLS certificate: {e}"))?;
        let key_bytes = std::fs::read(key_path)
            .map_err(|e| format!("read TLS private key: {e}"))?;
        let certs = rustls_pemfile::certs(&mut Cursor::new(cert_bytes))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("parse TLS certificate: {e}"))?;
        let key = rustls_pemfile::private_key(&mut Cursor::new(key_bytes))
            .map_err(|e| format!("parse TLS private key: {e}"))?
            .ok_or("TLS private key is missing")?;
        let _ = rustls::crypto::ring::default_provider().install_default();
        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| format!("configure TLS: {e}"))?;
        let acceptor = TlsAcceptor::from(Arc::new(config));
        Self::serve(sockets, Some(acceptor)).await
    }

    async fn serve(
        sockets: mpsc::Sender<Self>,
        tls: Option<TlsAcceptor>,
    ) -> Result<(), String> {
        let (method, variable, default) = if tls.is_some() {
            ("wss", "AIR_TLS_ADDR", "0.0.0.0:443")
        } else {
            ("ws", "AIR_WS_ADDR", "0.0.0.0:5702")
        };
        let addr = std::env::var(variable).unwrap_or_else(|_| default.into());
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| format!("bind {method} {addr}: {e}"))?;
        println!("air {method}://{addr}");
        let mut pending = FuturesUnordered::new();
        loop {
            tokio::select! {
                _ = sockets.closed() => return Ok(()),
                accepted = listener.accept(), if pending.len() < 32 => {
                    match accepted {
                        Ok((tcp, _)) => {
                            let tls = tls.clone();
                            let handshake = async move {
                                tcp.set_nodelay(true).map_err(|e| e.to_string())?;
                                let io: Box<dyn Io> = match tls {
                                    Some(tls) => Box::new(
                                        tls.accept(tcp).await
                                            .map_err(|e| e.to_string())?,
                                    ),
                                    None => Box::new(tcp),
                                };
                                let config = WebSocketConfig::default()
                                    .max_message_size(Some(16 * 1024 * 1024))
                                    .max_frame_size(Some(16 * 1024 * 1024));
                                let websocket = accept_async_with_config(
                                    io, Some(config),
                                )
                                .await
                                .map_err(|e| e.to_string())?;
                                Ok::<_, String>(Self::from_websocket(websocket))
                            };
                            pending.push(async move {
                                tokio::time::timeout(
                                    Duration::from_secs(5), handshake,
                                ).await
                                .map_err(|_| "websocket handshake timed out")?
                            });
                        }
                        Err(e) => {
                            eprintln!("air {method} accept: {e}");
                            tokio::time::sleep(
                                Duration::from_millis(100),
                            ).await;
                        }
                    }
                }
                result = pending.next(), if !pending.is_empty() => {
                    match result {
                        Some(Ok(socket)) => {
                            if sockets.send(socket).await.is_err() {
                                return Ok(());
                            }
                        }
                        Some(Err(e)) => eprintln!("air {method} handshake: {e}"),
                        None => {}
                    }
                }
            }
        }
    }
}

impl Connection for Socket {
    /// Open a ws/wss URL with an explicit port.
    async fn new(url: &str) -> Result<Self, String> {
        let request = url.into_client_request().map_err(|e| e.to_string())?;
        let secure = match request.uri().scheme_str() {
            Some("ws") => false,
            Some("wss") => true,
            _ => return Err("expected a ws or wss URL".into()),
        };
        let host = request
            .uri()
            .host()
            .ok_or("websocket URL has no host")?
            .trim_matches(['[', ']'])
            .to_owned();
        let port = request
            .uri()
            .port_u16()
            .ok_or("websocket URL requires an explicit valid port")?;
        let tls = if secure {
            let _ = rustls::crypto::ring::default_provider().install_default();
            let mut roots = RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            if let Ok(path) = std::env::var("AIR_TLS_CERT") {
                let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
                for cert in rustls_pemfile::certs(&mut Cursor::new(bytes)) {
                    roots
                        .add(cert.map_err(|e| e.to_string())?)
                        .map_err(|e| e.to_string())?;
                }
            }
            let config = rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();
            Some(TlsConnector::from(Arc::new(config)))
        } else {
            None
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            let tcp = TcpStream::connect((host.as_str(), port))
                .await
                .map_err(|e| e.to_string())?;
            tcp.set_nodelay(true).map_err(|e| e.to_string())?;
            let io: Box<dyn Io> = match tls {
                Some(tls) => {
                    let name = ServerName::try_from(host.to_owned())
                        .map_err(|e| e.to_string())?;
                    Box::new(
                        tls.connect(name, tcp)
                            .await
                            .map_err(|e| e.to_string())?,
                    )
                }
                None => Box::new(tcp),
            };
            // A stored request's response includes a signature and index.
            let config = WebSocketConfig::default()
                .max_message_size(Some(17 * 1024 * 1024))
                .max_frame_size(Some(17 * 1024 * 1024));
            let (websocket, _) =
                client_async_with_config(request, io, Some(config))
                    .await
                    .map_err(|e| e.to_string())?;
            Ok(Self::from_websocket(websocket))
        })
        .await
        .map_err(|_| "websocket connection timed out".to_string())?
    }

    async fn read(&mut self) -> Result<Vec<u8>, String> {
        tokio::select! {
            biased;
            bytes = self.incoming.recv() => bytes
                .ok_or_else(|| self.failure.borrow().clone()),
            _ = self.cancel.cancelled() => Err(self.failure.borrow().clone()),
        }
    }

    async fn write(&mut self, message: Vec<u8>) -> Result<(), String> {
        let (reply, result) = oneshot::channel();
        tokio::select! {
            result = async {
                self.outgoing.send((message, reply)).await
                    .map_err(|_| self.failure.borrow().clone())?;
                result.await.map_err(|_| self.failure.borrow().clone())?
            } => result,
            _ = self.cancel.cancelled() => Err(self.failure.borrow().clone()),
        }
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
