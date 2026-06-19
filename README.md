# Rift

Production-grade PostgreSQL CDC pipeline in a single Go binary.
No Kafka. No JVM. Just Rift.

## What is Rift?

Rift reads every change from your PostgreSQL database in real time
using logical replication and streams them to your destinations —
webhooks, HTTP endpoints, and more.

## Why Rift?

Most CDC tools require Kafka + Debezium + JVM = massive overhead.
Rift is a single Go binary with zero external dependencies.

## Quickstart

**1. Create rift.yaml**
\`\`\`yaml
source:
  type: postgres
  url: postgres://user:pass@localhost:5432/mydb?replication=database
  slot: rift_slot
  publication: rift_pub

destinations:
  - name: my-webhook
    type: webhook
    url: https://myapp.com/webhook
\`\`\`

**2. Run**
\`\`\`bash
go run main.go
\`\`\`

## Roadmap

\`\`\`
v0.1.0  ✅  WAL reading + webhook destination
v0.2.0  →   Embedded disk queue (BoltDB) — resilience without Kafka
v0.3.0  →   Multiple destinations (Postgres, Redis, HTTP)
v0.4.0  →   JS filtering at source
v0.5.0  →   DDL schema tracking
v1.0.0  →   Production ready — Debezium alternative
\`\`\`

## License
MIT