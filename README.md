# Air server and clients

The transport is based on the original client, server, and websocket in
[`ramp-stack/air` at `176a96b`](https://github.com/ramp-stack/air/tree/176a96b06a92f654aed34f3803231f218e747cdb/src).
AIR 4 supplies the identity, encryption, request, and storage types. Version 2 of this
crate uses those public types while preserving the existing transport protocol. The
manifest keeps a sibling `../air` path for development and a registry version for releases.

## Client

```rust
use air_server::Client;

let client = Client::new(identity, "wss").await?;
let responses = client.send(request).await;
```

`Client` owns Air encryption, response verification, pending requests, and
session recovery. Construction opens the initial connection, completes the
session handshake, and starts the background loop. Initial errors are
returned to the caller. Share the client through `Arc<Client>` if needed.

The client takes only the first bare host under the requested method in
the Identity, using port 5702 for WS or 443 for WSS. Hosts contain no scheme,
path, query, or port override. Missing or unsupported methods fail.
Later hosts are never tried, including after disconnection. The Identity
and selected URL remain fixed for the client's lifetime.

`Connection` is the public byte-message trait with asynchronous `new(url)`,
`read`, and `write` methods. It requires complete, ordered, duplicate-free
messages within each live connection and cancellation-safe reads. Writes
must progress while the peer writes, without requiring a concurrent
application read. A failed or cancelled write retires the connection.
The transport owns framing, keepalives, and connection deadlines.

Both WS and WSS use `Socket`. Its constructor accepts a complete URL with
an explicit port and opens only the transport. Socket reads and writes run
independently, with bounded buffering, so simultaneous large messages do
not deadlock. TLS verifies certificates and hostnames; `AIR_TLS_CERT` can
add private trusted certificates without disabling verification.

## Session recovery

The session protocol uses only `Connection` byte messages. It has no HTTP
headers or WebSocket-specific handshake fields. Another ordered, reliable
message transport can implement the same trait, including one built over
UDP with its own ordering and duplicate suppression.

Each connection establishes fresh Air encryption:

1. Client sends a serialized `StreamRequest`.
2. Server returns a fresh, signed challenge bound to that request.
3. Client sends an encrypted resume token, the challenge, and its next
   expected response sequence number.
4. Server authenticates the exchange, replaces any older attachment, and
   returns the next request number it has not durably accepted.

The challenge is deliberately signed but unencrypted. Replaying a captured
`StreamRequest` must not make the server encrypt different challenges under
the same stream key and counter. The server checks the challenge before
sending any encrypted response or replacing a session. Subsequent server
messages are signed in the context of this handshake and logical session.
The random resume token is sent only inside Air encryption; SQLite stores
its hash.

Requests have stable sequence numbers. The server commits their effects,
acceptance counters, and recoverable responses in one SQLite transaction
before sending a receipt. After a disconnect, the client resends only
requests at or beyond the server's durable acceptance counter. Earlier
requests recover their original saved responses: an accepted Create still
returns its original Created result, even if its receipt or reply was lost.

Responses have a separate sequence. The client advances its acknowledgment
after putting a response into the response channel, before writing the ACK
to the socket. Losing an ACK therefore does not duplicate delivery within
that client instance. This acknowledgment does not mean the application
has processed the response.

Pending reads remain registered in SQLite across disconnects and server
restarts. A later Create saves their final responses atomically with the
new record. A reconnect does not repeat an already delivered Read(None).
Dropping a response receiver cancels its pending subscription; cancellation
is acknowledged and retried after disconnection.

To bound buffering, each connection allows one request awaiting a durable
receipt, one response awaiting an ACK, and one cancellation batch awaiting
confirmation. Other requests can queue in the client. This adds a receipt
round trip between request transmissions. Request and response traffic can
still proceed in both directions concurrently.

Session recovery survives transport loss and server process restart with
the same SQLite database and server identity. Client tokens, pending
requests, and delivery counters currently live in memory: restarting the
client process creates a new session. There is no automatic reset of a
known session when its server state is missing or has regressed. A rejected
session stops its worker and closes its response channels; subsequent
`send` calls return closed receivers rather than panicking.

Acknowledged responses and completed request entries are removed. Session
counters and pending subscriptions remain durable; there is no automatic
session expiration. Retain and back up the complete SQLite database,
including session tables, rather than copying only application records.

## Server

Run the server with:

```sh
cargo run --release
```

At startup it loads its identity from the SQLite file selected by `AIR_STORAGE` (default
`STORAGE.db`), creates and stores a new secret only when the identity row is missing, and prints
its public AIR name for connections. Identity initialization uses a transaction so concurrent
first starts cannot generate different saved identities. A corrupt stored identity is an error;
it is never silently replaced. Keep this database to retain the server's name.

To preserve an existing deployment's JSON identity, set `AIR_SECRET_FILE` on the first startup.
It is imported only when the SQLite identity is absent; later starts use the saved SQLite secret.
`Server::secret(path)` also exposes this initialization for other AIR services.

| Variable | Default / purpose |
| --- | --- |
| `AIR_STORAGE` | `STORAGE.db`; SQLite with WAL and FULL synchronous commits |
| `AIR_WS_ADDR` | `0.0.0.0:5702` |
| `AIR_TLS_ADDR` | `0.0.0.0:443` |
| `AIR_TLS_CERT` | PEM certificate chain for direct WSS |
| `AIR_TLS_KEY` | PEM private key for direct WSS |
| `AIR_HEALTH_ADDR` | `127.0.0.1:5701` |

`Server::start` runs `Socket::listen` and `Socket::listen_tls`, starting a
`Session<C: Connection>` for each accepted socket. Both listeners share
the same storage worker and 64-connection limit. Sessions communicate
exclusively through the trait. Replacing a session fences queued writes,
acknowledgments, and disconnect notifications from its older connection.

The server starts WS and attempts WSS. Missing TLS configuration, an
invalid certificate/key, or a busy TLS port is logged while WS remains
available. Direct WSS reads its certificate at startup; restart after
renewal. If a reverse proxy owns port 443, choose another direct TLS port
or terminate TLS at the proxy.

Sockets send heartbeats every 30 seconds, allow 90 seconds without an
incoming frame, and bound writes and session setup to 10 seconds.
The server accepts messages up to 16 MiB; clients allow 17 MiB responses.
There are at most 256 unresolved reads per logical session. Each session
has 512 response slots, counting both saved events and a reserved final
event for each pending read. New requests that exceed these limits are
rejected without a partial commit.

Saved successful reads reference immutable application records; payloads
are stored once. This keeps an offline subscriber's queued results small
and ensures it cannot exhaust reply capacity and block another client's
valid Create. A missing referenced record is an error, never Read(None).

The new session envelope and handshake break compatibility with older Air
and Orange clients. Both peers must use this protocol; there is no legacy
fallback. The underlying Air Request/Response types and stored application
record format are unchanged.

See [production operations](deploy/README.md) for the existing AWS
deployment. These local source changes do not update that deployment.
