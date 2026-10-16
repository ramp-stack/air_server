# Air production operations

Deployed 2026-10-16. Preferred endpoint: `wss://air.orange.me/`. Health endpoint: `https://air.orange.me/healthz`.

## Resources

| Resource | Value |
| --- | --- |
| AWS account / region | 893776744185 / us-east-1 |
| CloudFormation stack | air-production |
| Instance | i-04c791a379ebbb344, t3.medium, us-east-1a |
| Elastic IP | 100.63.226.238, eipalloc-05c98dfe3dc3a7da3 |
| Encrypted data disk | vol-0abb60140dd55fb87, gp3, initially 100 GiB |
| Database | /var/lib/air/STORAGE.db |
| Backup bucket | air-production-893776744185-us-east-1 |
| Continuous replica | s3://air-production-893776744185-us-east-1/sqlite/production |
| Server identity | Secrets Manager: air/production/server-identity |
| Logs | CloudWatch: /air/production, 30-day retention |
| Alert topic | arn:aws:sns:us-east-1:893776744185:air-production-alerts |
| Initial encrypted production snapshot | snap-03f6d4ce0a5c3706b |
| Original server snapshot | snap-047239931ad418a69, us-east-2 |

The instance and data volume are protected from routine accidental deletion. The data volume, backup bucket, identity secret, logs and Elastic IP use CloudFormation retention policies. Root and data disks are encrypted. Administration uses AWS Systems Manager; the new server has no public SSH port. HTTP 80 serves certificate validation and redirects. TLS/WSS uses 443. Port 5702 serves the new protocol over WS; prefer WSS. Both transports now require the resumable-session client protocol. Older Orange/TestFlight builds must be updated before they can connect.

## Durability and capacity

There is exactly one active SQLite writer. WAL mode and `synchronous=FULL` commit before a successful write response. Litestream 0.5.17 replicates to S3 with a one-second sync interval. Remote backups are asynchronous, so an entire disk/AZ loss can lose recent commits that have not reached S3. This setup does not guarantee zero data loss or uninterrupted service during an AZ outage.

Daily EBS snapshots retain 35 recovery points. Litestream snapshots retain 35 days; older point-in-time precision depends on compaction. S3 versioning retains noncurrent object versions for 90 days. The server role cannot delete object versions. Initial recovery snapshots listed above are retained separately.

Every minute, the monitor checks HTTPS plus the storage actor, verifies remote backup sync, emits metrics and checks capacity. At 70% disk usage it requests 25% growth, capped at 500 GiB, then expands XFS after the EBS modification is usable. Growth is permanent and increases charges. AWS volume modification cooldowns can delay repeated expansion. An 80% usage alarm requests attention if growth is insufficient. CPU/RAM do not autoscale; this is a single-instance SQLite deployment. Expected initial baseline is approximately $45–55/month, plus variable backups, traffic, logs, and any CPU surplus credits. The old instance is stopped; its retained disk and snapshot still incur storage charges.

## Services and alerts

Use EC2 → Connect → Session Manager for the new instance. Useful commands:

```sh
sudo systemctl status air-server litestream nginx amazon-cloudwatch-agent
sudo systemctl list-timers 'air-*'
sudo tail -100 /var/log/air/server.log
sudo tail -100 /var/log/air/litestream.log
sudo tail -100 /var/log/air/monitor.log
curl --fail https://air.orange.me/healthz
```

Systemd restarts the application after failure. An EC2 system-status alarm requests instance recovery. Route 53 probes the public HTTPS endpoint. CloudWatch alarms cover public reachability, application health, backup failures, disk usage, high CPU, system failure and certificate expiry within 14 days. The email subscription for `caleb@orange.me` was verified as **Confirmed** in AWS SNS on 2026-10-16 (subscription ID `4ebcf314-861a-4570-a188-d21786d9686b`). Alarm state transitions are sent to that topic.

The Let's Encrypt certificate is checked for renewal twice daily by `air-certificate-renew.timer` using a webroot challenge; successful renewal reloads nginx. The distribution's separate `certbot-renew.timer` is intentionally disabled. Renewal was tested with `certbot renew --dry-run`. Caleb elected to give Dot the separate monitoring instruction file himself. No direct Dot integration or Codex chat monitoring automation was configured from this chat.

## Recovery

1. Fence the old writer before starting another one. If an instance is unreachable, stop it in EC2 and verify it is stopped; do not assume an unreachable process cannot write.
2. Prefer recovering the existing instance or attaching its intact retained data volume to a replacement in the same availability zone. Keep the mount at `/var/lib/air`. The service refuses to start when the mount or database is missing.
3. If the database or volume is lost, restore to a **new file**, inspect it, and only then promote it while application and Litestream services are stopped:

```sh
sudo systemctl stop air-monitor.timer air-monitor.service
sudo systemctl stop nginx air-server litestream
sudo /usr/local/bin/litestream restore \
  -o /var/lib/air/RESTORED.db \
  s3://air-production-893776744185-us-east-1/sqlite/production
sudo /opt/air/bin/verify_db /var/lib/air/RESTORED.db
```

Preserve the existing database, WAL, SHM and `.STORAGE.db-litestream` directory together in a recovery archive before promoting a replacement. Never overwrite an open SQLite database or reuse stale Litestream metadata against a different restored file. Restore ownership `air:air`, mode `0600`, then start the application and Litestream and verify database integrity and remote sync. Start nginx and test WSS create/read before restarting `air-monitor.timer`. Keep the timer and any running `air-monitor.service` stopped throughout recovery so the monitor cannot restart the writer. A restore can need substantial temporary disk space. Do not copy a live `.db` file by itself; use SQLite online backup, Litestream, or a volume snapshot that includes the WAL.

For an AZ outage, restore a regional EBS snapshot or the S3 backup into a new volume in another AZ and use a corresponding subnet. Reassociate the Elastic IP only after fencing the original writer. There is no automatic cross-AZ failover in this deployment.

## Code and deployments

Only `air_server` source was changed. The deployed `air` dependency exactly
matches the existing local library. The server uses durable SQLite
initialization, supervised storage work, bounded queues, and handshake/write
deadlines. Request acceptance, record changes, and recoverable responses commit
atomically. Pending reads and response state persist across reconnects and
server restarts. Limits: 64 concurrent connections, 16 MiB messages, 256
unresolved reads per logical session, and 512 response slots including
reservations for pending reads. The original data's largest payload was
10,969,989 bytes.

The client retains its resume token and pending requests in memory. Recovery
works across transport disconnects and server restarts, but client process
restart does not preserve that state. Restoring an older database can cause a
client to reject lost or regressed session state; do not silently reset its
token and replay requests as a recovery shortcut.

The process reads its identity from `AIR_SECRET_FILE`; it is no longer embedded in `src/main.rs`. On AWS the file is `/etc/air/identity.json`, fetched from Secrets Manager and readable only by root and the service group. Keep credentials out of Git. Local development also needs an appropriate identity file; setting a different identity changes which clients can authenticate the server.

`air-production.cloudformation.json` records this existing stack. The retained identity resource was imported with its existing value. **Do not create a fresh stack from this template without explicitly preserving/importing the existing identity and database**: a randomly generated identity would break protocol authentication. For updates, use the existing stack and inspect the change set. After automatic disk growth, reconcile the template's data-volume size before making volume changes.

`configure-host.py` writes the systemd, nginx, Litestream and monitoring configuration on an already bootstrapped host. It does not provision resources or activate an empty database. It must be run as root on the production host, not on a developer laptop. Compile against the unchanged sibling `air` dependency with the checked-in lockfile. Install an updated binary at a new filename and atomically rename it before restarting the service. Keep a previous binary for investigation and controlled rollback. After the new protocol accepts sessions, do not run the old protocol binary as a writer: it cannot maintain durable subscriptions or the response outbox. Never roll back the database to an older copy after accepting new writes.

## Transition server

Original instance: `i-0b535de2607f02596` in us-east-2, former address `3.15.27.196`. Its old application was stopped and its original database frozen read-only. Nginx forwarded WS/WSS to the new server during DNS propagation. After public DNS and WSS verification, with no active connections on the old host, the old instance was stopped on 2026-10-16 at approximately 17:37 UTC. Its disk and snapshot remain available. Its old public IP was not an Elastic IP and is no longer reserved. The old database is only a historical recovery copy; restarting its application would create divergent data. Preserve its snapshot rather than using its stale database as an active writer.

## Initial deployment verification

- Original SQLite integrity and every stored payload signature: 5,382 records passed.
- Local WS/WSS application integration test and all-target compilation passed.
- Trusted TLS and server signatures verified over the public endpoint.
- A 231-byte test file was created, read back exactly, and read after automatic crash recovery.
- Staging S3 restore matched every record by logical SHA-256.
- Production S3 restore verified all 5,383 records, including the test write.
- Certificate renewal dry run succeeded; encrypted initial snapshot completed.
- Seven CloudWatch alarms reached OK after deployment.

## Resumable-session release: 2026-10-16

Release `session-20261016` became active at approximately 21:00 UTC. The
binary and source are retained at `/opt/air/releases/session-20261016`.
Its SHA-256 is
`5d53391bf81b40b095a62f56be30c8b1f3199f0bbcadd628bfb2bfbe7bd3a21f`.
Release metadata is in `session-20261016.json` alongside this document.

The migration added session, request, and response-outbox tables in place.
All 5,390 existing records were preserved byte for byte; their logical
SHA-256 before and after was
`4d2274f72b63f7645135bf87017771cb46332da8a0cfac6ed865b588f75e0d20`.
The server identity, data volume, domain, and TLS configuration were retained.
Nginx continues to terminate public WSS on 443 and forwards to the application's
WS listener at `127.0.0.1:5703`; native application TLS is not enabled on this
host. Public WS remains on 5702.

Before cutover, a consistent SQLite backup was uploaded to
`s3://air-production-893776744185-us-east-1/sqlite/pre-session-20261016/STORAGE.db`
with its manifest. Encrypted snapshot `snap-09fcb5599d16ea741` completed.
The old binary and frozen database are retained under
`/var/lib/air/deploy-session-20261016/cutover` for recovery investigation.

Production verification passed:

- Public WSS read original records, including the 10,969,989-byte payload.
- A WSS test deliberately disconnected after durable request acceptance;
  reconnect recovered the original creation response and exact 101-byte data.
- Public WS read that same record successfully.
- The latest S3 replica restored to a separate database containing all 5,391
  records, including the new test record, plus four durable sessions.
- SQLite integrity, foreign keys, every payload signature, all original
  records, and restored session/outbox consistency checks passed.
- Application, nginx, Litestream, and monitoring were active; the application
  reported zero automatic restarts and all seven CloudWatch alarms were OK.

The new protocol requires updated clients, including Orange. This release does
not provide wire compatibility with earlier Orange/TestFlight builds.
