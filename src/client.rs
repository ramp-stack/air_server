use air::{Drain, Id, Identity, Request, Response, Sink, Stream};
use crossfire::{AsyncRx, AsyncTx, AsyncTxTrait, MAsyncTx, mpsc, spsc};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::time::Duration;
use crate::Socket;
use crate::protocol::{ClientMessage, ServerMessage};

/// A connected channel carrying complete, ordered, duplicate-free byte messages.
/// Reads must be cancellation-safe. Report disconnection as an error.
/// The transport owns framing, keepalives, and connection-level deadlines.
/// Writes must make progress while the peer writes, without a concurrent read.
/// After a failed or cancelled write, the owner must discard the connection.
pub trait Connection: Send + Sized {
    fn new(url: &str) -> impl Future<Output = Result<Self, String>> + Send;
    fn read(&mut self) -> impl Future<Output = Result<Vec<u8>, String>> + Send;
    fn write(&mut self, message: Vec<u8>) -> impl Future<Output = Result<(), String>> + Send;
}

/// A resumable Air client. Dropping a response receiver cancels its subscription.
pub struct Client {
    tx: MAsyncTx<mpsc::List<Command>>,
}

impl Client {
    pub async fn new(identity: Identity, method: &str) -> Result<Self, String> {
        match method {
            "ws" | "wss" => {
                let host = identity
                    .connection()
                    .get(method)
                    .and_then(|hosts| hosts.first())
                    .ok_or_else(|| format!("identity has no {method} host"))?;
                let host = host.trim_matches(['[', ']']);
                let authority =
                    if host.contains(':') { format!("[{host}]") } else { host.to_owned() };
                let port = if method == "ws" { 5702 } else { 443 };
                Self::run::<Socket>(identity, format!("{method}://{authority}:{port}/")).await
            }
            _ => Err(format!("unsupported connection method: {method}")),
        }
    }

    pub async fn send(&self, request: Request) -> AsyncRx<spsc::List<Response>> {
        let (tx, rx) = spsc::build(spsc::List::new());
        let _ = self.tx.send((request, tx)).await;
        rx
    }

    async fn run<C: Connection + 'static>(identity: Identity, url: String) -> Result<Self, String> {
        let mut resume = Resume { token: Id::random(), known: false, received: 0, seen: 0 };
        let first =
            resume.connect::<C>(&identity, &url).await.map_err(|error| error.to_string())?;
        let (tx, mut rx): (MAsyncTx<mpsc::List<Command>>, AsyncRx<mpsc::List<Command>>) =
            mpsc::build(mpsc::List::new());
        tokio::spawn(async move {
            let mut initial = Some(first);
            let mut closed = false;
            let mut next = 0u64;
            let mut requests = BTreeMap::<u64, Pending>::new();
            let mut queued = VecDeque::<Command>::new();
            loop {
                if closed && requests.is_empty() && queued.is_empty() { return; }
                let opened = if let Some(connection) = initial.take() {
                    Ok(connection)
                } else {
                    let opening = resume.connect::<C>(&identity, &url);
                    tokio::pin!(opening);
                    loop {
                        tokio::select! {
                            result = &mut opening => break result,
                            command = rx.recv(), if !closed => match command {
                                Ok(command) => queued.push_back(command),
                                Err(_) => closed = true,
                            },
                        }
                        if closed && requests.is_empty() && queued.is_empty() { return; }
                    }
                };
                let result: Result<(), Failure> = async {
                    let rx = &mut rx;
                    let mut session = opened?;
                    if resume.received > next {
                        return Err(Failure::Rejected("server accepted unsent requests".into()));
                    }
                    // The server's durable watermark, not socket delivery,
                    // decides which requests may be transmitted again.
                    let mut sending = resume.received;
                    let mut cancel_sent = BTreeSet::new();
                    let mut cleanup = tokio::time::interval(Duration::from_secs(1));
                    cleanup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    loop {
                        while let Some(command) = queued.pop_front() {
                            let pending = Pending::new(command);
                            if !pending.live() { continue; }
                            requests.insert(next, pending);
                            next = next.checked_add(1).unwrap();
                        }
                        if closed && requests.is_empty() { return Ok(()); }
                        let cancelled: Vec<_> = requests
                            .iter()
                            .filter(|(index, pending)| {
                                **index < resume.received
                                    && !pending.live()
                                    && !cancel_sent.contains(*index)
                            })
                            .map(|(index, _)| *index)
                            .take(256)
                            .collect();
                        if cancel_sent.is_empty() && !cancelled.is_empty() {
                            session
                                .write(&ClientMessage::Cancel { indexes: cancelled.clone() })
                                .await?;
                            cancel_sent.extend(cancelled);
                        }
                        tokio::select! {
                            command = rx.recv(), if !closed => match command {
                                Ok(command) => queued.push_back(command),
                                Err(_) => closed = true,
                            },
                            message = session.read(&identity) => {
                                match message? {
                                    ServerMessage::Received { received } => {
                                        if received < resume.received
                                            || received > sending
                                        {
                                            return Err(Failure::Rejected(
                                                "invalid request receipt".into(),
                                            ));
                                        }
                                        resume.received = received;
                                    }
                                    ServerMessage::Response {
                                        sequence, index, hash, response,
                                    } => {
                                        if sequence > resume.seen
                                            || index >= resume.received
                                        {
                                            return Err(Failure::Rejected(
                                                "invalid response sequence".into(),
                                            ));
                                        }
                                        if sequence == resume.seen {
                                            if let Some(pending) =
                                                requests.get(&index)
                                            {
                                                if pending.hash != hash {
                                                    return Err(Failure::Rejected(
                                                        "response request hash mismatch"
                                                            .into(),
                                                    ));
                                                }
                                                let keep = matches!(
                                                    response,
                                                    Response::Read(_, None),
                                                );
                                                let _ = pending.responder
                                                    .send(response).await;
                                                if !keep { requests.remove(&index); }
                                            }
                                            resume.seen = resume.seen.checked_add(1).unwrap();
                                        }
                                        // Save seen before writing, so a lost
                                        // ACK cannot duplicate local delivery.
                                        session.write(&ClientMessage::Ack {
                                            seen: resume.seen,
                                        }).await?;
                                    }
                                    ServerMessage::Cancelled { indexes } => {
                                        for index in indexes {
                                            if !cancel_sent.remove(&index) {
                                                return Err(Failure::Rejected(
                                                    "unexpected cancellation receipt"
                                                        .into(),
                                                ));
                                            }
                                            requests.remove(&index);
                                        }
                                    }
                                    ServerMessage::Rejected { reason } => {
                                        return Err(Failure::Rejected(reason));
                                    }
                                    _ => {
                                        return Err(Failure::Rejected(
                                            "unexpected session message".into(),
                                        ));
                                    }
                                }
                            }
                            _ = cleanup.tick() => {}
                            _ = std::future::ready(()),
                                if sending < next && sending == resume.received =>
                            {
                                let pending = requests.get(&sending).unwrap();
                                session.write(&ClientMessage::Request {
                                    index: sending, request: pending.request.clone(),
                                }).await?;
                                sending += 1;
                            }
                        }
                    }
                }
                .await;
                match result {
                    Ok(()) => return,
                    Err(Failure::Rejected(error)) => {
                        eprintln!("air session rejected: {error}");
                        return;
                    }
                    Err(error) => eprintln!("air reconnect: {error}"),
                }
                let retry = tokio::time::sleep(Duration::from_secs(1));
                tokio::pin!(retry);
                loop {
                    tokio::select! {
                        _ = &mut retry => break,
                        command = rx.recv(), if !closed => match command {
                            Ok(command) => queued.push_back(command),
                            Err(_) => closed = true,
                        },
                    }
                }
            }
        });
        Ok(Self { tx })
    }
}

type Responder = AsyncTx<spsc::List<Response>>;
type Command = (Request, Responder);

enum Failure { Transport(String), Rejected(String), }

impl From<String> for Failure {
    fn from(error: String) -> Self { Self::Transport(error) }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self { Self::Transport(error) | Self::Rejected(error) => f.write_str(error), }
    }
}

struct Resume {
    token: Id,
    known: bool,
    received: u64,
    seen: u64,
}

impl Resume {
    async fn connect<C: Connection>(&mut self, identity: &Identity, url: &str) -> Result<Session<C>, Failure> {
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut connection = C::new(url).await?;
            let (crypto, hello) = Stream::send(identity, &[]);
            let context = Id::hash(&hello);
            let (mut encrypt, mut decrypt) = crypto.split();
            connection
                .write(postcard::to_allocvec(&hello).map_err(|error| error.to_string())?)
                .await?;
            let challenge =
                match ServerMessage::decode(&connection.read().await?, identity, context)? {
                    ServerMessage::Challenge { challenge } => challenge,
                    _ => { return Err(Failure::Rejected("expected server challenge".into())); }
                };
            connection
                .write(
                    encrypt.encrypt(
                        ClientMessage::Resume {
                            token: self.token,
                            known: self.known,
                            seen: self.seen,
                            challenge,
                        }
                        .encode()?,
                    ),
                )
                .await?;
            let context = Id::hash(&(context, challenge, Id::hash(&self.token)));
            let bytes =
                decrypt.decrypt(connection.read().await?).map_err(|error| error.to_string())?;
            match ServerMessage::decode(&bytes, identity, context)? {
                ServerMessage::Ready { received } => {
                    if received < self.received {
                        return Err(Failure::Rejected(
                            "server lost committed session state".into(),
                        ));
                    }
                    self.received = received;
                    self.known = true;
                    Ok(Session { connection, encrypt, decrypt, context })
                }
                ServerMessage::Rejected { reason } => Err(Failure::Rejected(reason)),
                _ => Err(Failure::Rejected("expected session ready".into())),
            }
        })
        .await
        .map_err(|_| Failure::Transport("session handshake timed out".into()))?
    }
}

struct Session<C: Connection> {
    connection: C,
    encrypt: Sink,
    decrypt: Drain,
    context: Id,
}

impl<C: Connection> Session<C> {
    async fn read(&mut self, identity: &Identity) -> Result<ServerMessage, String> {
        let bytes = self
            .decrypt
            .decrypt(self.connection.read().await?)
            .map_err(|error| error.to_string())?;
        ServerMessage::decode(&bytes, identity, self.context)
    }

    async fn write(&mut self, message: &ClientMessage) -> Result<(), String> {
        self.connection.write(self.encrypt.encrypt(message.encode()?)).await
    }
}

struct Pending {
    request: Request,
    hash: Id,
    responder: Responder,
}

impl Pending {
    fn new((request, responder): Command) -> Self {
        Self { hash: Id::hash(&request), request, responder }
    }

    fn live(&self) -> bool { self.responder.get_rx_count() > 0 }
}
