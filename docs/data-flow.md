# Data Flow Deep Dive

## Purpose

This document explains how data actually moves through the learning project:

- where the flow starts
- which component initiates each interaction
- what is synchronous vs asynchronous
- what is pull-based vs push-based
- what state each component keeps to make the pipeline resumable

The goal is to make the CDC pipeline mechanically clear before we build it.

## System View

The learning project has two planes:

- data plane: business row changes moving from PostgreSQL to Kafka to downstream consumers
- control plane: connector config, offsets, schema history, consumer-group state, and UI inspection

High-level view:

```text
                Control plane
    +---------------------------------------+
    | connector config, offsets, status,    |
    | schema history, consumer positions    |
    +---------------------------------------+

                  Data plane
App -> PostgreSQL -> Debezium -> Kafka -> Consumers -> Derived state
                                  ^
                                  |
                              Kafka UI
                         (read-only observer)
```

## Components And Their Roles

### Application or SQL client

Starts business changes by issuing `INSERT`, `UPDATE`, and `DELETE` statements.

### PostgreSQL

Owns the source tables and records committed changes in WAL.

### Debezium connector

Reads a snapshot of source tables at startup if configured, then streams committed changes from PostgreSQL logical replication and converts them into Debezium event envelopes.

### Kafka Connect

Hosts the Debezium connector task and manages connector lifecycle, config, offsets, and status.

### Kafka

Stores change events durably in topics and lets multiple consumer groups read them independently.

### CDC inspector

Consumes raw Debezium events and prints them in a readable form.

### Projection consumer

Consumes raw Debezium events and updates a derived read model such as `order_summary`.

### Kafka UI

Reads Kafka metadata and messages for inspection. It does not participate in business processing.

## The Two Real Starting Points

There is no single universal "start." There are two:

### 1. Connector bootstrap start

This starts when we create the Debezium connector.

Flow:

1. a client sends connector configuration to Kafka Connect
2. Kafka Connect starts the Debezium task
3. Debezium inspects the source schema
4. Debezium snapshots existing rows if snapshot mode is enabled

This is how the system begins observing an existing database.

### 2. Live business-change start

This starts when an application transaction commits in PostgreSQL.

Flow:

1. application updates a row
2. PostgreSQL records the change in WAL
3. transaction commits
4. Debezium can now capture that committed change

For ongoing CDC, the database commit is the true beginning of the data plane.

## Sequence Example: `orders.status` Changes To `PAID`

Assume:

- table: `orders`
- row key: `id = 101`
- before status: `PENDING`
- after status: `PAID`

### End-to-end sequence

```text
1. App/SQL client
   -> UPDATE orders SET status = 'PAID' WHERE id = 101;

2. PostgreSQL
   -> writes the row change into WAL
   -> commits the transaction

3. Debezium
   -> receives the committed change from logical replication
   -> builds a Debezium event envelope

4. Kafka Connect
   -> produces the event to the Kafka topic for orders

5. Kafka
   -> persists the record in the topic partition

6. CDC inspector consumer
   -> polls Kafka
   -> logs the raw event

7. Projection consumer
   -> polls Kafka
   -> upserts derived state, such as order_summary

8. Kafka UI
   -> can read the topic, offsets, and consumer-group lag
```

### Sequence diagram

```text
App            PostgreSQL         Debezium/Connect         Kafka         Inspector      Projection      Kafka UI
 |                  |                    |                  |               |               |              |
 | UPDATE order     |                    |                  |               |               |              |
 |----------------->|                    |                  |               |               |              |
 |                  | write WAL + commit |                  |               |               |              |
 |                  |------------------->| replication msg  |               |               |              |
 |                  |                    | build event      |               |               |              |
 |                  |                    |----------------->| append topic  |               |              |
 |                  |                    |                  |-------------->| poll records  |              |
 |                  |                    |                  |------------------------------>| poll records |
 |                  |                    |                  |<-------------------------------------------->|
 |                  |                    |                  |   metadata/messages/groups read by UI        |
```

## What Happens At Each Boundary

### Boundary 1: Application -> PostgreSQL

Who initiates it:

- application or SQL client

Mode:

- push
- synchronous request/response

What completes here:

- the database accepts or rejects the write
- from the caller's perspective, success means the transaction committed

What does not complete here:

- Kafka publication
- downstream projection updates

That means the application can succeed even if Debezium or Kafka is temporarily down. Recovery depends on WAL retention and connector state, not on the application retrying the write.

### Boundary 2: PostgreSQL -> Debezium during snapshot

Who initiates it:

- Debezium

Mode:

- pull

What happens:

- Debezium queries source tables
- Debezium emits snapshot records that represent current row state

This is not a live change stream yet. It is state capture by direct reads.

### Boundary 3: PostgreSQL -> Debezium during streaming

Who initiates it:

- Debezium first opens the replication connection
- PostgreSQL then emits committed changes on that connection

Mode:

- push-like stream after subscription

What happens:

- Debezium listens on a long-lived logical replication stream
- PostgreSQL sends committed changes in WAL order

This is not polling. Debezium does not repeatedly ask, "anything new?" It subscribes once, then the server delivers changes over the established stream.

### Boundary 4: Debezium -> Kafka

Who initiates it:

- Kafka Connect / Debezium

Mode:

- push
- asynchronous relative to the source transaction

What happens:

- Debezium transforms the database change into a Kafka record
- Kafka Connect produces the record to the table topic

Typical topic shape:

- one topic per captured table
- example: `dbserver1.public.orders`

### Boundary 5: Kafka -> Consumers

Who initiates it:

- each consumer group

Mode:

- pull

What happens:

- consumers call `poll()`
- Kafka returns available records for assigned partitions
- each group tracks its own offsets independently

Kafka does not push records into your consumer code. The consumer asks for records.

### Boundary 6: Consumers -> Their sinks

Who initiates it:

- the consumer after reading a Kafka record

Mode:

- push

What happens:

- inspector writes logs or terminal output
- projection consumer writes to its read-model store

The important downstream design rule is idempotency. At-least-once delivery means the same change can be seen again after retries or restarts.

### Boundary 7: Kafka UI -> Kafka

Who initiates it:

- Kafka UI

Mode:

- pull
- read-only

What happens:

- reads topic metadata
- reads message payloads
- reads consumer-group offsets and lag

Kafka UI is an observer, not a participant in business flow.

## Push vs Pull Table

| Interaction | Mode | Why |
| --- | --- | --- |
| App -> PostgreSQL | Push | application initiates the write |
| Debezium snapshot -> PostgreSQL tables | Pull | connector reads current table contents |
| PostgreSQL replication -> Debezium | Push-like stream | server emits changes after connector subscribes |
| Debezium -> Kafka | Push | connector produces records |
| Consumer -> Kafka | Pull | consumer polls records |
| Consumer -> sink | Push | consumer writes side effects |
| Kafka UI -> Kafka | Pull | UI reads metadata and messages |

## Synchronous vs Asynchronous Boundaries

| Interaction | Sync or async | Practical meaning |
| --- | --- | --- |
| App -> PostgreSQL commit | Synchronous to caller | caller waits for transaction result |
| PostgreSQL -> Debezium stream | Asynchronous to caller | happens after commit, outside app request |
| Debezium -> Kafka | Asynchronous to caller | Kafka publication is decoupled from the SQL request |
| Kafka -> consumer processing | Asynchronous to caller | downstream processing happens later |
| Consumer -> projection DB | Synchronous within consumer logic | consumer usually waits for sink write before committing its offset |

The key architectural point is that the original application request is only directly coupled to the source database commit. Everything after that is decoupled.

## What State Makes Recovery Possible

### PostgreSQL WAL

Stores committed database changes in order. Debezium depends on this log remaining available long enough to resume consumption.

### Replication slot

Tracks how far the consumer of the logical replication stream has progressed.

### Kafka Connect offsets

Track where the connector has progressed in the source stream.

### Debezium schema history

Tracks relevant schema evolution so Debezium can interpret row changes correctly across table changes.

### Kafka topic offsets

Track where each consumer group has progressed in the topic.

Without these state stores, restart behavior becomes guesswork.

## Data Plane vs Control Plane

### Data plane

The business change path:

1. application writes to `orders`
2. PostgreSQL commits and records WAL
3. Debezium captures the committed change
4. Kafka stores the change event
5. consumers read and apply the event

### Control plane

The coordination path:

1. connector config is submitted to Kafka Connect REST
2. Kafka Connect persists config and status
3. Debezium persists schema history
4. consumers commit offsets
5. Kafka UI reads metadata and state

Mixing these two paths leads to confusion. A consumer offset commit is not business data. A connector status update is not CDC payload.

## The Exact Meaning Of "Debezium Captures A Change"

It helps to be precise about what Debezium is and is not doing.

Debezium is not:

- inside the application transaction
- intercepting SQL before commit
- polling a table with `SELECT updated_at > ...`

Debezium is:

- downstream of the committed transaction
- reading durable database change information
- converting low-level change records into structured events

That means CDC is derived from committed database state, not from application intent.

## Event Shape At The Kafka Boundary

For the `orders.status` update, the event conceptually looks like:

```json
{
  "before": {
    "id": 101,
    "status": "PENDING"
  },
  "after": {
    "id": 101,
    "status": "PAID"
  },
  "op": "u",
  "source": {
    "table": "orders"
  }
}
```

The consumer should treat this as a fact about database state transition, not necessarily as a business-domain event with rich intent.

## Failure Scenarios To Keep In Mind

### Application succeeds, Debezium is temporarily down

The row change is still in PostgreSQL. Debezium can catch up later if WAL and connector state are intact.

### Debezium publishes to Kafka, consumer is down

The event stays in Kafka until the consumer resumes and polls it.

### Projection consumer processes the event twice

This must not corrupt derived state. The projection logic must be idempotent.

### Kafka UI is unavailable

Observability is reduced, but business flow can continue. Kafka UI is not on the critical data path.

## What To Validate In The Learning Project

Use the project to validate these claims directly:

1. a committed SQL update appears in Kafka only after commit
2. snapshot records and live-stream records can be distinguished
3. a stopped consumer can later catch up from Kafka
4. a stopped Debezium connector can later catch up from PostgreSQL/WAL
5. replaying a topic does not break the projection if the consumer is idempotent

## Recommended Build Order

Build the project in the same order as the data path:

1. PostgreSQL with sample tables
2. Kafka
3. Kafka Connect with Debezium
4. Kafka UI
5. a writer path that changes source rows
6. a raw-event inspector consumer
7. a projection consumer

That order makes it easy to test each handoff in isolation.
