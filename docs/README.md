# Docs Index

This repo uses a small doc set instead of a single large README.

## Suggested Reading Order

1. [Core Concepts](core-concepts.md)
2. [Architecture](architecture.md)
3. [Data Flow Deep Dive](data-flow.md)

## Document Guide

### [Core Concepts](core-concepts.md)

Use this first if you want the CDC and Debezium mental model:

- what CDC is
- why log-based CDC matters
- what a Debezium event contains
- offsets, schema history, delivery semantics, and schema evolution

### [Architecture](architecture.md)

Use this when you want the proposed learning-project shape:

- source domain
- main components
- high-level flow
- project phases
- architecture decisions and next build steps

### [Data Flow Deep Dive](data-flow.md)

Use this when you want the mechanics of one change moving through the system:

- where the flow really starts
- push vs pull boundaries
- synchronous vs asynchronous boundaries
- control plane vs data plane
- recovery state and failure behavior
