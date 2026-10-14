# QuestDB ADBC driver

A read-oriented Rust ADBC driver for QuestDB using the official `questdb-rs` QWP-over-WebSocket reader. Query batches are delivered incrementally as Arrow `RecordBatch` values.

## Status

Early development. The initial implementation supports query execution and Arrow streaming. Ingestion, parameter binding, metadata discovery, cancellation APIs, and prepared statements are not implemented yet. The QWP reader requires QuestDB 10.0 or newer and Rust 1.91.1.

## Build

```sh
cargo test
cargo build --release --features ffi
```

The `ffi` feature exports `AdbcQuestdbInit` and `AdbcDriverInit` for ADBC driver managers.

## Connect

The canonical database URI is a WebSocket URL such as `ws://localhost:9000` or `wss://questdb.example:9000`. Configure it with the standard ADBC URI option. ADBC username and password options are passed to the official QWP reader.

Rust consumers can initialize the driver with `QuestDbDriver::default()`, configure a database with `OptionDatabase::Uri`, open a connection, set SQL on a statement, and call `Statement::execute()` to receive a streaming Arrow reader.

## Streaming design

The native client's `Cursor` borrows its QWP reader, whereas ADBC returns an owned `Send + 'static` reader. Each query therefore runs on a worker thread and sends at most one pending batch through a bounded channel. Dropping the ADBC result reader disconnects the channel; the worker then drops the cursor, which cancels/tears down an unfinished query. Arrow decoding and type conversion remain in the official QuestDB client.

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
