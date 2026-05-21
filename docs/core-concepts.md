# Core Concepts

## 1. What CDC is

CDC is the process of detecting row-level changes in a database and turning them into events that other systems can consume.

Typical change types:

- insert
- update
- delete

Why it matters:

- keeps downstream systems in sync without batch jobs
- reduces load compared with repeated polling
- enables near-real-time integrations and analytics

## 2. Log-based CDC vs polling and triggers

For this project, the important distinction is:

- polling asks the database repeatedly what changed
- triggers push custom logic into the database
- log-based CDC reads the database's own replication log

Debezium is primarily a log-based CDC platform. That matters because it is usually:

- lower latency
- less invasive to application code
- more complete for reconstructing change history

## 3. What Debezium does

Debezium connectors read database change logs such as:

- PostgreSQL WAL
- MySQL binlog
- SQL Server transaction log

They usually work in two phases:

1. initial snapshot of existing table data
2. continuous streaming of new changes

One of the first things the learning project should make visible is the difference between snapshot events and live-streamed change events.

## 4. Debezium event model

A Debezium change event is more than "the row changed." It carries data and metadata that downstream consumers need.

The main parts to understand:

- `before`: row state before the change
- `after`: row state after the change
- `op`: operation type such as create, update, delete, read
- `source`: database, table, transaction, and log-position metadata
- `ts_ms`: event timestamp

High-level example:

```json
{
  "before": { "id": 42, "status": "PENDING" },
  "after": { "id": 42, "status": "PAID" },
  "op": "u",
  "source": { "table": "orders" },
  "ts_ms": 1770000000000
}
```

## 5. Debezium runtime pieces

The minimum useful mental model is:

- source database
- Kafka Connect worker
- Debezium connector
- Kafka topics
- consumers or sink services

Important internal state:

- connector offsets: how Debezium remembers where it left off
- schema history: how connector state tracks schema changes

Without those two concepts, restart behavior is hard to reason about.

## 6. Delivery semantics and ordering

A learning project should treat these as first-class ideas:

- event delivery is generally at-least-once, so duplicates must be tolerated
- ordering is usually meaningful per table partition or per key, not globally
- delete handling often includes tombstone-style records depending on configuration

That means downstream consumers should be written to be:

- idempotent
- replay-safe
- tolerant of restarts and rebalances

## 7. Schema evolution

CDC is not only about data changes. Table definitions change too.

Examples:

- add a column
- rename a column
- change nullable constraints

This is where event schema, consumer compatibility, and connector configuration start to matter. Even in a learning project, one intentional schema change should be part of the exercises.

## 8. CDC does not replace domain design

Raw table changes are useful, but they are not always ideal business events.

That leads to an important advanced concept for later phases:

- raw CDC from operational tables
- outbox pattern for explicit domain events

For the first project iteration, raw CDC is the right starting point. The outbox pattern should be a later extension once the basic pipeline is understood.
