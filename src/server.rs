use air::{
    DBRequest, DBResponse, Id, KeySignature, PublicKey, Request, Response, Secret, Storage, Stream,
    StreamRequest,
};
use rusqlite::{Connection as SqliteConnection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Notify, Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use std::collections::HashMap;
use std::sync::Arc;
use std::path::Path;
use std::time::Duration;
use crate::protocol::{ClientMessage, ServerMessage};
use crate::{Connection, Socket};

#[derive(Clone)]
pub struct Server;

impl Server {
    pub fn secret(path: &Path) -> Secret {
        let mut db = SqliteConnection::open(path).unwrap();
        db.busy_timeout(Duration::from_secs(5)).unwrap();
        let transaction = db.transaction_with_behavior(TransactionBehavior::Immediate).unwrap();
        transaction
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS identity(
                id INTEGER PRIMARY KEY CHECK(id = 0), secret BLOB NOT NULL
            );",
            )
            .unwrap();
        let saved: Option<Vec<u8>> = transaction
            .query_row("SELECT secret FROM identity WHERE id = 0", [], |row| row.get(0))
            .optional()
            .unwrap();
        let secret =
            saved.map(|bytes| serde_json::from_slice(&bytes).unwrap()).unwrap_or_else(|| {
                let secret = match std::env::var_os("AIR_SECRET_FILE") {
                    Some(path) => serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap(),
                    None => Secret::new(),
                };
                transaction
                    .execute(
                        "INSERT INTO identity(id, secret) VALUES (0, ?1)",
                        [serde_json::to_vec(&secret).unwrap()],
                    )
                    .unwrap();
                secret
            });
        transaction.commit().unwrap();
        secret
    }

    pub async fn start(secret: Secret) {
        println!("AIR server: {}", secret.name());
        let (tx, rx) = mpsc::channel(16);
        let worker =
            StorageWorker { cache: Cache::new(secret.clone()), attachments: HashMap::new() };
        let storage = tokio::spawn(worker.run(rx));
        let (accepted, mut sockets) = mpsc::channel(32);
        let listener = Socket::listen(accepted.clone());
        let secure = async {
            let result = Socket::listen_tls(accepted).await;
            println!("air wss listener stopped: {result:?}");
            std::future::pending::<()>().await;
        };
        let health = Self::health(tx.clone());
        let connections = Arc::new(Semaphore::new(64));
        let sessions = async {
            let mut id = 0u64;
            while let Some(connection) = sockets.recv().await {
                let Ok(permit) = connections.clone().try_acquire_owned() else { continue; };
                id += 1;
                let secret = secret.clone();
                let session = Session { id, connection, requests: tx.clone() };
                tokio::spawn(async move { let _permit = permit; session.run(secret).await; });
            }
        };
        tokio::select! {
            result = storage => panic!("storage worker stopped: {result:?}"),
            result = listener => panic!("ws listener stopped: {result:?}"),
            _ = secure => unreachable!(),
            _ = health => panic!("health listener stopped"),
            _ = sessions => panic!("socket delivery stopped"),
        }
    }

    async fn health(tx: mpsc::Sender<Incoming>) {
        let addr = std::env::var("AIR_HEALTH_ADDR").unwrap_or_else(|_| "127.0.0.1:5701".into());
        let listener = TcpListener::bind(addr).await.expect("bind health listener");
        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(socket) => socket,
                Err(error) => {
                    eprintln!("air health accept: {error}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let (reply, result) = oneshot::channel();
            let healthy = tokio::time::timeout(Duration::from_secs(3), async {
                let mut request = [0; 1024];
                socket.read(&mut request).await.map_err(|_| ())?;
                tx.send(Incoming::Health(reply)).await.map_err(|_| ())?;
                result.await.map_err(|_| ())
            })
            .await
            .is_ok_and(|result| result.is_ok());
            let response: &[u8] = if healthy {
                b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\
                  Connection: close\r\n\r\nok\n"
            } else {
                b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\
                  Connection: close\r\n\r\n"
            };
            let _ = tokio::time::timeout(Duration::from_secs(1), socket.write_all(response)).await;
        }
    }
}

type Answer = Result<Option<ServerMessage>, String>;

#[derive(Serialize, Deserialize)]
enum StoredResponse { Inline(Response), Read(PublicKey), }

enum Action {
    Connect { known: bool, seen: u64, notify: Arc<Notify>, cancel: CancellationToken },
    Message(ClientMessage),
    Next(u64),
}

enum Incoming {
    Health(oneshot::Sender<()>),
    Call { key: Id, owner: u64, action: Action, reply: oneshot::Sender<Answer> },
    Closed { key: Id, owner: u64 },
}

struct Attachment {
    owner: u64,
    notify: Arc<Notify>,
    cancel: CancellationToken,
}

struct Session<C: Connection> {
    id: u64,
    connection: C,
    requests: mpsc::Sender<Incoming>,
}

impl<C: Connection> Session<C> {
    async fn call(&mut self, key: Id, action: Action) -> Answer {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(Incoming::Call { key, owner: self.id, action, reply })
            .await
            .map_err(|_| "storage worker stopped")?;
        result.await.map_err(|_| "storage worker stopped")?
    }

    async fn run(mut self, secret: Secret) {
        let notify = Arc::new(Notify::new());
        let cancel = CancellationToken::new();
        let mut attached = None;
        let result: Result<(), String> = async {
            let (mut encrypt, mut decrypt, key, context, mut cursor) =
                tokio::time::timeout(Duration::from_secs(10), async {
                    let bytes = self.connection.read().await?;
                    let (hello, []) = postcard::take_from_bytes::<StreamRequest>(&bytes)
                        .map_err(|error| error.to_string())?
                    else { return Err("invalid Air handshake".into()); };
                    let hello_hash = Id::hash(&hello);
                    let challenge = Id::random();
                    let message = ServerMessage::Challenge { challenge };
                    self.connection.write(message.encode(&secret, hello_hash)?).await?;
                    let (mut encrypt, mut decrypt) = Stream::receive(&secret, hello).split();
                    let bytes = decrypt
                        .decrypt(self.connection.read().await?)
                        .map_err(|error| error.to_string())?;
                    let ClientMessage::Resume { token, known, seen, challenge: echo } =
                        ClientMessage::decode(&bytes)?
                    else { return Err("expected session resume".into()); };
                    if echo != challenge { return Err("session challenge mismatch".into()); }
                    let key = Id::hash(&token);
                    let context = Id::hash(&(hello_hash, challenge, key));
                    attached = Some(key);
                    let ready = match self
                        .call(
                            key,
                            Action::Connect {
                                known,
                                seen,
                                notify: notify.clone(),
                                cancel: cancel.clone(),
                            },
                        )
                        .await
                    {
                        Ok(Some(message @ ServerMessage::Ready { .. })) => message,
                        Ok(_) => return Err("missing session ready".into()),
                        Err(reason) => {
                            let message = ServerMessage::Rejected { reason: reason.clone() };
                            self.connection
                                .write(encrypt.encrypt(message.encode(&secret, context)?))
                                .await?;
                            return Err(reason);
                        }
                    };
                    self.connection.write(encrypt.encrypt(ready.encode(&secret, context)?)).await?;
                    Ok((encrypt, decrypt, key, context, seen))
                })
                .await
                .map_err(|_| "session handshake timed out")??;
            let mut more = true;
            let mut awaiting_ack = false;
            loop {
                if cancel.is_cancelled() { break; }
                if more && !awaiting_ack {
                    let message = self
                        .call(key, Action::Next(cursor))
                        .await
                        .unwrap_or_else(|reason| Some(ServerMessage::Rejected { reason }));
                    more = message.is_some();
                    if let Some(message) = message {
                        self.connection
                            .write(encrypt.encrypt(message.encode(&secret, context)?))
                            .await?;
                        match message {
                            ServerMessage::Response { sequence, .. } => {
                                cursor = sequence + 1;
                                awaiting_ack = true;
                            }
                            ServerMessage::Rejected { .. } => break,
                            _ => return Err("invalid outbox message".into()),
                        }
                    }
                }
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = notify.notified() => more = true,
                    _ = std::future::ready(()), if more && !awaiting_ack => {},
                    incoming = self.connection.read() => {
                        let bytes = decrypt.decrypt(incoming?)
                            .map_err(|error| error.to_string())?;
                        let message = ClientMessage::decode(&bytes)?;
                        let acknowledged = matches!(&message,
                            ClientMessage::Ack { seen } if *seen == cursor);
                        let reply = self.call(key, Action::Message(message))
                            .await.unwrap_or_else(|reason| {
                                Some(ServerMessage::Rejected { reason })
                            });
                        if let Some(reply) = reply {
                            self.connection.write(encrypt.encrypt(
                                reply.encode(&secret, context)?,
                            )).await?;
                            if matches!(reply, ServerMessage::Rejected { .. }) { break; }
                        }
                        if acknowledged { awaiting_ack = false; }
                        more = true;
                    }
                }
            }
            Ok(())
        }
        .await;
        cancel.cancel();
        if let Some(key) = attached {
            let _ = self.requests.send(Incoming::Closed { key, owner: self.id }).await;
        }
        if let Err(error) = result { eprintln!("air session: {error}"); }
    }
}

struct StorageWorker {
    cache: Cache,
    attachments: HashMap<Id, Attachment>,
}

impl StorageWorker {
    async fn run(mut self, mut rx: mpsc::Receiver<Incoming>) {
        while let Some(incoming) = rx.recv().await {
            match incoming {
                Incoming::Health(reply) => {
                    if self.cache.db.query_row("SELECT 1", [], |row| row.get::<_, i64>(0)).is_ok() {
                        let _ = reply.send(());
                    }
                }
                Incoming::Closed { key, owner } => {
                    if self.attachments.get(&key).is_some_and(|entry| entry.owner == owner) {
                        self.attachments.remove(&key);
                    }
                }
                Incoming::Call { key, owner, action, reply } => {
                    let answer = match action {
                        Action::Connect { known, seen, notify, cancel } => {
                            self.cache.connect(key, known, seen).map(|received| {
                                if let Some(old) = self
                                    .attachments
                                    .insert(key, Attachment { owner, notify, cancel })
                                { old.cancel.cancel(); }
                                Some(ServerMessage::Ready { received })
                            })
                        }
                        action => {
                            if self.attachments.get(&key).is_none_or(|entry| entry.owner != owner)
                            {
                                Err("session connection was replaced".into())
                            } else {
                                match action {
                                    Action::Next(sequence) => self.cache.next(key, sequence),
                                    Action::Message(message) => {
                                        let changed =
                                            matches!(&message, ClientMessage::Request { .. },);
                                        let result = self.cache.message(key, message);
                                        if changed && result.is_ok() {
                                            for entry in self.attachments.values() {
                                                entry.notify.notify_one();
                                            }
                                        }
                                        result
                                    }
                                    Action::Connect { .. } => unreachable!(),
                                }
                            }
                        }
                    };
                    let _ = reply.send(answer);
                }
            }
        }
        for entry in self.attachments.values() { entry.cancel.cancel(); }
    }
}

struct Cache {
    db: SqliteConnection,
    secret: Secret,
}

impl Cache {
    fn new(secret: Secret) -> Self {
        let path = std::env::var("AIR_STORAGE").unwrap_or_else(|_| "STORAGE.db".into());
        let db = SqliteConnection::open(path).expect("open storage");
        db.busy_timeout(Duration::from_secs(5)).expect("storage timeout");
        db.pragma_update(None, "journal_mode", "WAL").expect("enable WAL");
        db.pragma_update(None, "synchronous", "FULL").expect("enable durable commits");
        db.pragma_update(None, "foreign_keys", "ON").expect("enable foreign keys");
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS private(
                key BLOB NOT NULL PRIMARY KEY,
                key_signature BLOB NOT NULL,
                timestamp BLOB NOT NULL,
                payload BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sessions(
                key BLOB NOT NULL PRIMARY KEY,
                received INTEGER NOT NULL CHECK(received >= 0),
                next_event INTEGER NOT NULL CHECK(next_event >= 0),
                seen INTEGER NOT NULL CHECK(seen >= 0)
            );
            CREATE TABLE IF NOT EXISTS requests(
                session BLOB NOT NULL REFERENCES sessions(key),
                request_index INTEGER NOT NULL CHECK(request_index >= 0),
                hash BLOB NOT NULL,
                read_key BLOB,
                done INTEGER CHECK(done >= 0),
                PRIMARY KEY(session, request_index)
            );
            CREATE INDEX IF NOT EXISTS waiting_reads ON requests(read_key)
                WHERE read_key IS NOT NULL;
            CREATE TABLE IF NOT EXISTS outbox(
                session BLOB NOT NULL REFERENCES sessions(key),
                sequence INTEGER NOT NULL CHECK(sequence >= 0),
                request_index INTEGER NOT NULL CHECK(request_index >= 0),
                hash BLOB NOT NULL,
                response BLOB NOT NULL,
                PRIMARY KEY(session, sequence)
            );",
        )
        .expect("create storage tables");
        Self { db, secret }
    }

    fn connect(&mut self, key: Id, known: bool, seen: u64) -> Result<u64, String> {
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| error.to_string())?;
        let received: Option<i64> = tx
            .query_row("SELECT received FROM sessions WHERE key = ?1", [key.as_ref()], |row| {
                row.get(0)
            })
            .optional()
            .map_err(|error| error.to_string())?;
        let received = match received {
            Some(received) => received,
            None => {
                if known || seen != 0 { return Err("server has no retained session state".into()); }
                tx.execute(
                    "INSERT INTO sessions(key, received, next_event, seen)
                     VALUES (?1, 0, 0, 0)",
                    [key.as_ref()],
                )
                .map_err(|error| error.to_string())?;
                0
            }
        };
        Self::ack(
            &tx,
            key.as_ref(),
            i64::try_from(seen).map_err(|_| "response acknowledgement out of range")?,
        )?;
        tx.commit().map_err(|error| error.to_string())?;
        Ok(u64::try_from(received).unwrap())
    }

    fn message(&mut self, key: Id, message: ClientMessage) -> Answer {
        let secret = self.secret.clone();
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| error.to_string())?;
        let reply = match message {
            ClientMessage::Request { index, request } => {
                let index = i64::try_from(index).map_err(|_| "request index out of range")?;
                if index == i64::MAX { return Err("request sequence exhausted".into()); }
                let received: i64 = tx
                    .query_row(
                        "SELECT received FROM sessions WHERE key = ?1",
                        [key.as_ref()],
                        |row| row.get(0),
                    )
                    .map_err(|error| error.to_string())?;
                let hash = postcard::to_allocvec(&Id::hash(&request))
                    .map_err(|error| error.to_string())?;
                if index < received {
                    let previous: Option<Vec<u8>> = tx
                        .query_row(
                            "SELECT hash FROM requests
                         WHERE session = ?1 AND request_index = ?2",
                            params![key.as_ref(), index],
                            |row| row.get(0),
                        )
                        .optional()
                        .map_err(|error| error.to_string())?;
                    if previous.as_deref() != Some(hash.as_slice()) {
                        return Err("request index is retired or has another payload".into());
                    }
                    Some(ServerMessage::Received { received: u64::try_from(received).unwrap() })
                } else { if index != received { return Err("request sequence has a gap".into()); }
                    let record_key = match &request {
                        Request::Read(key) => *key,
                        Request::Create(signed) => signed.key,
                    };
                    let mut failure = None;
                    let results =
                        Storage::new(secret).handle(0, index as u64, request, |request| {
                            match Self::store(&tx, &request) {
                                Ok(response) => response,
                                Err(error) => { failure = Some(error); None }
                            }
                        });
                    if let Some(error) = failure { return Err(error); }
                    let response = results
                        .into_values()
                        .next()
                        .ok_or("storage produced no response")?
                        .payload
                        .1;
                    tx.execute(
                        "INSERT INTO requests(
                            session, request_index, hash, read_key, done
                         ) VALUES (?1, ?2, ?3, NULL, NULL)",
                        params![key.as_ref(), index, &hash],
                    )
                    .map_err(|error| error.to_string())?;
                    if matches!(&response, Response::Read(_, None)) {
                        let count: i64 = tx
                            .query_row(
                                "SELECT COUNT(*) FROM requests
                             WHERE session = ?1 AND read_key IS NOT NULL",
                                [key.as_ref()],
                                |row| row.get(0),
                            )
                            .map_err(|error| error.to_string())?;
                        if count >= 256 { return Err("session pending-read limit reached".into()); }
                        let read_key = postcard::to_allocvec(&record_key)
                            .map_err(|error| error.to_string())?;
                        tx.execute(
                            "UPDATE requests SET read_key = ?3
                             WHERE session = ?1 AND request_index = ?2",
                            params![key.as_ref(), index, read_key],
                        )
                        .map_err(|error| error.to_string())?;
                    }
                    Self::append(&tx, key.as_ref(), (index, &hash, &response, record_key))?;
                    if matches!(&response, Response::Created(_)) {
                        let created = record_key;
                        let (time, signature, payload) =
                            Self::store(&tx, &DBRequest::Read(created))?
                                .ok_or("created record is missing")?;
                        let value = Response::Read(time, Some((signature, payload)));
                        let read_key =
                            postcard::to_allocvec(&created).map_err(|error| error.to_string())?;
                        let pending = {
                            let mut statement = tx
                                .prepare(
                                    "SELECT session, request_index, hash
                                 FROM requests WHERE read_key = ?1",
                                )
                                .map_err(|error| error.to_string())?;
                            let rows = statement
                                .query_map([read_key], |row| {
                                    Ok((
                                        row.get::<_, Vec<u8>>(0)?,
                                        row.get::<_, i64>(1)?,
                                        row.get::<_, Vec<u8>>(2)?,
                                    ))
                                })
                                .map_err(|error| error.to_string())?;
                            rows.collect::<rusqlite::Result<Vec<_>>>()
                                .map_err(|error| error.to_string())?
                        };
                        for (session, index, hash) in pending {
                            Self::append(&tx, &session, (index, &hash, &value, created))?;
                        }
                    }
                    tx.execute(
                        "UPDATE sessions SET received = ?2 WHERE key = ?1",
                        params![key.as_ref(), received + 1],
                    )
                    .map_err(|error| error.to_string())?;
                    Some(ServerMessage::Received { received: (received + 1) as u64 })
                }
            }
            ClientMessage::Ack { seen } => {
                Self::ack(
                    &tx,
                    key.as_ref(),
                    i64::try_from(seen).map_err(|_| "response acknowledgement out of range")?,
                )?;
                None
            }
            ClientMessage::Cancel { indexes } => {
                let received: i64 = tx
                    .query_row(
                        "SELECT received FROM sessions WHERE key = ?1",
                        [key.as_ref()],
                        |row| row.get(0),
                    )
                    .map_err(|error| error.to_string())?;
                for index in &indexes {
                    let index = i64::try_from(*index).map_err(|_| "cancel index out of range")?;
                    if index >= received {
                        return Err("cannot cancel an unreceived request".into());
                    }
                    tx.execute(
                        "DELETE FROM requests WHERE session = ?1
                         AND request_index = ?2 AND done IS NULL",
                        params![key.as_ref(), index],
                    )
                    .map_err(|error| error.to_string())?;
                }
                Some(ServerMessage::Cancelled { indexes })
            }
            ClientMessage::Resume { .. } => { return Err("session is already attached".into()); }
        };
        tx.commit().map_err(|error| error.to_string())?;
        Ok(reply)
    }

    fn next(&self, key: Id, sequence: u64) -> Answer {
        let sequence = i64::try_from(sequence).map_err(|_| "response index out of range")?;
        let row: Option<(i64, i64, Vec<u8>, Vec<u8>)> = self
            .db
            .query_row(
                "SELECT sequence, request_index, hash, response FROM outbox
             WHERE session = ?1 AND sequence >= ?2
             ORDER BY sequence LIMIT 1",
                params![key.as_ref(), sequence],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(|error| error.to_string())?;
        match row {
            Some((sequence, index, hash, response)) => {
                let stored: StoredResponse =
                    postcard::from_bytes(&response).map_err(|error| error.to_string())?;
                let response = match stored {
                    StoredResponse::Inline(response) => response,
                    StoredResponse::Read(record_key) => {
                        let (time, signature, payload) =
                            Self::store(&self.db, &DBRequest::Read(record_key))?
                                .ok_or("saved response record is missing")?;
                        Response::Read(time, Some((signature, payload)))
                    }
                };
                Ok(Some(ServerMessage::Response {
                    sequence: u64::try_from(sequence).unwrap(),
                    index: u64::try_from(index).unwrap(),
                    hash: postcard::from_bytes(&hash).map_err(|error| error.to_string())?,
                    response,
                }))
            }
            None => Ok(None),
        }
    }

    fn append(db: &SqliteConnection, session: &[u8], event: (i64, &[u8], &Response, PublicKey)) -> Result<(), String> {
        let (index, hash, response, record_key) = event;
        // Values in private are immutable. A reference preserves the exact
        // result without copying large payloads into every subscriber's outbox.
        let stored = if matches!(response, Response::Read(_, Some(_))) {
            StoredResponse::Read(record_key)
        } else {
            StoredResponse::Inline(response.clone())
        };
        let bytes = postcard::to_allocvec(&stored).map_err(|error| error.to_string())?;
        let sequence: i64 = db
            .query_row("SELECT next_event FROM sessions WHERE key = ?1", [session], |row| {
                row.get(0)
            })
            .map_err(|error| error.to_string())?;
        assert!(sequence < i64::MAX);
        if !matches!(response, Response::Read(_, None)) {
            db.execute(
                "UPDATE requests SET done = ?3, read_key = NULL
                 WHERE session = ?1 AND request_index = ?2",
                params![session, index, sequence],
            )
            .map_err(|error| error.to_string())?;
        }
        // Each active read reserves a future terminal event. Completing it
        // exchanges that reservation for a saved result, so an offline reader
        // cannot exhaust its quota and block another session's valid Create.
        let used: i64 = db
            .query_row(
                "SELECT (SELECT COUNT(*) FROM outbox WHERE session = ?1) +
                (SELECT COUNT(*) FROM requests
                 WHERE session = ?1 AND read_key IS NOT NULL)",
                [session],
                |row| row.get(0),
            )
            .map_err(|error| error.to_string())?;
        if used >= 512 { return Err("session unacknowledged-response limit reached".into()); }
        db.execute(
            "INSERT INTO outbox(
                session, sequence, request_index, hash, response
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![session, sequence, index, hash, bytes],
        )
        .map_err(|error| error.to_string())?;
        db.execute(
            "UPDATE sessions SET next_event = ?2 WHERE key = ?1",
            params![session, sequence + 1],
        )
        .map_err(|error| error.to_string())?;
        Ok(())
    }

    fn ack(db: &SqliteConnection, session: &[u8], seen: i64) -> Result<(), String> {
        let (previous, next): (i64, i64) = db
            .query_row("SELECT seen, next_event FROM sessions WHERE key = ?1", [session], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .map_err(|error| error.to_string())?;
        if seen < previous || seen > next { return Err("invalid response acknowledgement".into()); }
        db.execute("UPDATE sessions SET seen = ?2 WHERE key = ?1", params![session, seen])
            .map_err(|error| error.to_string())?;
        db.execute(
            "DELETE FROM outbox WHERE session = ?1 AND sequence < ?2",
            params![session, seen],
        )
        .map_err(|error| error.to_string())?;
        db.execute("DELETE FROM requests WHERE session = ?1 AND done < ?2", params![session, seen])
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    fn store(db: &SqliteConnection, request: &DBRequest) -> Result<DBResponse, String> {
        let key = match request {
            DBRequest::Read(key) => key,
            DBRequest::Write(_, signed) => &signed.key,
        };
        let key = postcard::to_allocvec(key).map_err(|error| error.to_string())?;
        let row: Option<(Vec<u8>, Vec<u8>, Vec<u8>)> = db
            .query_row(
                "SELECT timestamp, key_signature, payload
             FROM private WHERE key = ?1",
                [&key],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|error| error.to_string())?;
        let previous = match row {
            Some((time, signature, payload)) => Some((
                postcard::from_bytes::<u64>(&time).map_err(|error| error.to_string())?,
                postcard::from_bytes::<KeySignature>(&signature)
                    .map_err(|error| error.to_string())?,
                payload,
            )),
            None => None,
        };
        if previous.is_none() && let DBRequest::Write(time, signed) = request {
            db.execute(
                "INSERT INTO private(
                    key, key_signature, timestamp, payload
                 ) VALUES (?1, ?2, ?3, ?4)",
                params![
                    key,
                    postcard::to_allocvec(&signed.signature).map_err(|error| error.to_string())?,
                    postcard::to_allocvec(time).map_err(|error| error.to_string())?,
                    &signed.payload,
                ],
            ).map_err(|error| error.to_string())?;
        }
        Ok(previous)
    }
}
