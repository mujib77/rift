# Rift

PostgreSQL CDC pipeline in a single Go binary.
No Kafka. No JVM. No complexity.

---

## The problem

Most CDC pipelines look like this:

```
Postgres → Debezium → Kafka → Kafka Connect → Destination
```

That's 4 systems to operate, monitor, and debug.
Most teams don't need that complexity.

## What Rift does

```
Postgres → Rift → Destination
```

One binary. One config file. Done.

---

## How it works

Rift connects to your Postgres database using logical
replication and streams every INSERT, UPDATE, DELETE
to your destinations in real time.

If a destination goes down, Rift doesn't lose events.
It writes them to a local embedded disk queue (BoltDB)
and automatically drains them when the destination
comes back up.

No Kafka needed for that resilience.

---

## Quickstart

**1. Enable logical replication in postgresql.conf**

```
wal_level = logical
```

**2. Create rift.yaml**

```yaml
source:
  type: postgres
  url: postgres://user:pass@localhost:5432/mydb?replication=database
  slot: rift_slot
  publication: rift_pub

destinations:
  - name: my-webhook
    type: webhook
    url: https://myapp.com/webhook/changes
    headers:
      Authorization: Bearer your-token

queue:
  enabled: true
  path: ./rift-queue
  max_size_mb: 1000
```

**3. Run**

```bash
go run main.go
```

That's it. Every database change streams to your webhook.

---

## What gets streamed

Every event includes:

```json
{
  "table": "users",
  "operation": "INSERT",
  "data": {
    "id": "1",
    "name": "Mujib",
    "email": "mujib@example.com"
  },
  "lsn": "0/16C752F8",
  "timestamp": "2026-05-20T14:32:00Z"
}
```

---

## Disk queue

When a destination goes offline Rift flips into
air-gap mode automatically.

Events are written to a local BoltDB file instead
of being dropped. When the destination comes back,
Rift drains the queue and resumes normal operation.

No events lost. No manual intervention needed.

---

## Destinations

| Type    | Status |
|---------|--------|
| Webhook | ✅ v0.1.0 |
| HTTP    | ✅ v0.1.0 |
| Postgres | 🔜 v0.3.0 |
| Redis   | 🔜 v0.3.0 |

---

## Roadmap

```
v0.1.0  ✅  WAL reading + webhook destination
v0.2.0  ✅  Embedded disk queue — resilience without Kafka
v0.3.0  →   Postgres + Redis destinations
v0.4.0  →   JS filtering — drop events at source
v0.5.0  →   DDL tracking — handle schema changes
v1.0.0  →   Production ready Debezium alternative
```

---

## Requirements

- PostgreSQL 12+ with wal_level = logical
- Go 1.26+

---

## License

MIT