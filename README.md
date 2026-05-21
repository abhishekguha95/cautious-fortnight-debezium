# cautious-fortnight-debezium

Local learning project for exploring change data capture (CDC) with Debezium end to end.

The project is meant to make the full pipeline visible:

- mutate rows in a source database
- capture committed changes from the database log
- publish them to Kafka
- inspect the raw events
- consume them into a derived read model
- observe operational behavior such as snapshots, retries, deletes, and schema changes

## Planned Stack

- PostgreSQL as the source database
- Kafka as the event backbone
- Kafka Connect with the Debezium PostgreSQL connector
- Kafka UI for topic and consumer inspection
- a raw CDC inspector consumer
- a projection consumer

## Docs

- [Docs Index](docs/README.md)
- [Core Concepts](docs/core-concepts.md)
- [Architecture](docs/architecture.md)
- [Data Flow Deep Dive](docs/data-flow.md)

## Current Focus

The repo is still in the design/scaffolding phase.

The next practical build step is to add:

- `docker-compose.yml` for PostgreSQL, Kafka, Kafka Connect, and Kafka UI
- `db/init` scripts for sample tables
- a Debezium connector config
- a seed or writer path to generate source changes
- an inspector consumer
- a projection consumer
