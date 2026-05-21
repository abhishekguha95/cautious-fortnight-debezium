# Architecture

## Scope

Keep the first version small, local, and observable. The project should optimize for understanding, not production completeness.

Recommended source domain:

- `customers`
- `orders`
- `order_items`

These tables are enough to demonstrate inserts, updates, deletes, foreign keys, and status transitions.

## High-Level Flow

```text
App / SQL scripts
        |
        v
  PostgreSQL (source DB)
        |
   logical replication / WAL
        |
        v
Debezium Connector on Kafka Connect
        |
        v
     Kafka topics
        |
   +----+--------------------+------------------+
   |                         |                  |
   v                         v                  v
Kafka UI                CDC inspector     Projection consumer
(browse topics,         (raw event        (builds read model)
 offsets, messages)      viewer)
```

For the detailed interaction mechanics, see [Data Flow Deep Dive](data-flow.md).

## Recommended Components

### 1. Source database

Use PostgreSQL as the source database because it is a strong fit for a first Debezium project:

- well-documented CDC setup
- clear WAL and logical replication model
- easy local setup with Docker

Responsibilities:

- hold the operational tables
- generate inserts, updates, and deletes
- expose replication data for Debezium

### 2. Kafka broker

Use a single-node Kafka setup in KRaft mode for local learning.

Responsibilities:

- store change events durably
- expose per-topic replay
- let multiple consumers observe the same change stream independently

### 3. Kafka Connect and Debezium

This is the CDC engine of the project.

Responsibilities:

- perform the initial snapshot
- stream ongoing changes from PostgreSQL
- publish one topic per captured table
- track offsets and schema history

### 4. Kafka UI

Add a Kafka web UI to the local stack so the project is easier to inspect while learning.

Recommended choice:

- Provectus Kafka UI

Use it to inspect:

- Debezium-created topics
- raw change-event payloads
- connector-related internal topics
- consumer lag and group state

### 5. CDC inspector

Create a very small consumer whose only job is to print or expose raw Debezium events in a readable way.

This is important because it gives direct visibility into:

- `c`, `u`, `d`, `r` operation types
- snapshot vs streaming events
- delete behavior
- transaction ordering

### 6. Projection consumer

Create a second consumer that turns raw CDC events into a read model.

Examples:

- `order_summary` table
- customer order counters
- latest order status view

This teaches the core downstream pattern:

- consume event
- apply idempotent update
- materialize query-friendly state

### 7. Optional API or UI

A thin API or simple UI is useful, but it should not be the first priority. The learning value is in the data flow.

If added later, it should:

- trigger sample writes
- show raw events
- show the projected read model

## Suggested Project Phases

### Phase 1: Raw CDC pipeline

Build the minimum working flow:

- PostgreSQL
- Kafka
- Kafka Connect
- Debezium PostgreSQL connector
- sample tables
- scripted inserts and updates

Success criteria:

- initial snapshot appears in Kafka
- new row changes appear after snapshot

### Phase 2: Event inspection

Add Kafka UI and a raw CDC consumer.

Success criteria:

- can browse Debezium topics and messages from the browser
- can inspect payload shape
- can distinguish create, update, delete
- can observe source metadata

### Phase 3: Read-model projection

Add a second consumer that maintains a derived table or store.

Success criteria:

- downstream state updates correctly from CDC events
- duplicate processing does not corrupt the projection

### Phase 4: Failure and restart behavior

Test the operational parts most teams ignore at first.

Exercises:

- restart Kafka Connect
- restart the consumer
- replay from an older offset
- confirm idempotent projection behavior

### Phase 5: Schema evolution

Change the schema intentionally.

Examples:

- add `customer_tier`
- add `order_total`

Success criteria:

- connector continues working
- consumers handle the change safely

### Phase 6: Outbox extension

After raw CDC is understood, add an outbox table and compare:

- raw row-change events
- explicit business events

This is the right point to discuss when CDC alone is enough and when a proper event contract is better.

## Recommended Non-Functional Principles

Even for a learning project, design around these ideas:

- observability: logs and readable event inspection matter more than polish
- replayability: consumers should be safe to rerun
- isolation: source DB and projection store should be conceptually separate
- minimalism: one source DB, one connector, two small consumers is enough

## Initial Architecture Decision

For the first implementation, the best tradeoff is:

- PostgreSQL as source
- Kafka as event backbone
- Kafka Connect with Debezium PostgreSQL connector
- Kafka UI for topic and consumer inspection
- one raw-event inspector consumer
- one projection consumer
- Docker Compose to run everything locally
- SQL scripts or a tiny writer service to generate changes

This is small enough to understand fully and large enough to cover the important CDC concepts.

## Next Build Step

The next practical step is to scaffold the repo around this architecture:

- `docker-compose.yml` for PostgreSQL, Kafka, Kafka Connect, and Kafka UI
- `db/init` scripts for sample tables
- `connectors/postgres-source.json` for Debezium config
- `producer` or `seed` scripts to generate changes
- `inspector` consumer
- `projection` consumer
