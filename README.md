# QuestDB ADBC driver

A read-oriented Rust ADBC driver for QuestDB using the official `questdb-rs` QWP-over-WebSocket reader. Query batches are delivered incrementally as Arrow `RecordBatch` values.

## Status

Early development. The initial implementation supports query execution, Arrow streaming, and cooperative cancellation when the result reader is advanced. Ingestion, parameter binding, metadata discovery, and prepared statements are not implemented yet. The QWP reader requires QuestDB 10.0 or newer and Rust 1.91.1.

## Build

```sh
cargo test
cargo build --release --features ffi
```

The `ffi` feature exports `AdbcQuestdbInit` and `AdbcDriverInit` for ADBC driver managers.

## Connect

The canonical database URI is a WebSocket URL such as `ws://localhost:9000` or `wss://questdb.example:9000`. Configure it with the standard ADBC URI option. ADBC username and password options are passed to the official QWP reader.

Rust consumers can initialize the driver with `QuestDbDriver::default()`, configure a database with `OptionDatabase::Uri`, open a connection, set SQL on a statement, and call `Statement::execute()` to receive a streaming Arrow reader.

## Connection and streaming design

Concurrency and streaming semantics follow the official PostgreSQL C ADBC driver. An ADBC connection owns exactly one native QWP `Reader` (the analog of the PG driver's `PGconn* conn_`), opened lazily on its first execution and reused by later queries. An in-flight result checks the reader out exclusively — a second execution on the connection (including re-executing the same statement) returns an `InvalidState` error until the active result finishes or is dropped, matching ADBC's buffer-or-error rule for open result sets. Re-executing a statement invalidates its previous result set, which reports a `Cancelled` error the next time it is advanced. After a failed process restart or torn-down transport, the next execution reconnects lazily.

The native cursor mutably borrows the checked-out reader; a self-referential cell safely holds both inside the `Send + 'static` Arrow result reader, replacing the PG driver's `TupleReader`. Batches are pulled directly on `next()` — no background worker or batch buffer, honoring the pull-based `GetNext` contract. Statement or connection cancellation is recognized when an affected stream is next advanced: the query ends server-side through the cursor's CANCEL frame and a bounded drain, so the native connection itself stays reusable (the role of `PGcancel`). Arrow decoding and type conversion remain in the official QuestDB client.

An empty result is supported when QuestDB sends its schema-bearing initial (possibly zero-row) result batch. A stream that ends before the server supplies a schema is reported as an error, since QWP schema is carried by a result batch.

## Development tree

```text
.docker/
.github/
src/
  lib.rs
  options.rs
  reader.rs
  schema.rs
  statement.rs
  writer.rs
tests/
```
