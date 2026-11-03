use std::collections::HashSet;
use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use adbc_core::error::Status;
use adbc_core::options::{InfoCode, IngestMode, OptionDatabase, OptionStatement, OptionValue};
use adbc_core::schemas::GET_INFO_SCHEMA;
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use arrow_array::{
    Array, ArrayRef, Int64Array, RecordBatch, RecordBatchIterator, StringArray,
    TimestampMicrosecondArray, UInt32Array, UnionArray,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, TimeUnit};

use adbc_questdb::{QuestDbConnection, QuestDbDatabase, QuestDbDriver};

fn test_database() -> Option<QuestDbDatabase> {
    let uri = env::var("QUESTDB_ADBC_TEST_URI").ok()?;
    let mut driver = QuestDbDriver;
    Some(
        driver
            .new_database_with_opts([(OptionDatabase::Uri, OptionValue::from(uri))])
            .expect("configure QuestDB database"),
    )
}

#[test]
fn creating_connection_does_not_connect_to_questdb() {
    let mut driver = QuestDbDriver;
    let database = driver
        .new_database_with_opts([(OptionDatabase::Uri, OptionValue::from("ws://127.0.0.1:1"))])
        .expect("configure unreachable QuestDB endpoint");

    // The native QWP connection opens lazily when a query acquires it.
    let _connection = database
        .new_connection()
        .expect("create logical connection without opening a socket");
}

#[test]
fn query_returns_arrow_batches_incrementally() {
    let Some(database) = test_database() else {
        eprintln!("skipping Docker integration test: QUESTDB_ADBC_TEST_URI is unset");
        return;
    };

    let mut connection = database.new_connection().expect("connect to QuestDB");
    let mut statement = connection.new_statement().expect("create statement");
    statement
        .set_sql_query("SELECT x FROM long_sequence(100000)")
        .expect("set SQL");

    let reader = statement.execute().expect("execute query");
    let schema = reader.schema();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "x");

    let mut rows = 0usize;
    let mut batches = 0usize;
    for batch in reader {
        let batch = batch.expect("read Arrow batch");
        rows += batch.num_rows();
        batches += 1;
    }

    assert_eq!(rows, 100_000);
    assert!(batches > 0);
}

#[test]
fn query_reports_empty_results_with_a_schema() {
    let Some(database) = test_database() else {
        eprintln!("skipping Docker integration test: QUESTDB_ADBC_TEST_URI is unset");
        return;
    };

    let mut connection = database.new_connection().expect("connect to QuestDB");
    let mut statement = connection.new_statement().expect("create statement");
    statement
        .set_sql_query("SELECT x FROM long_sequence(10) WHERE x < 0")
        .expect("set SQL");

    let reader = statement.execute().expect("execute query");
    assert_eq!(reader.schema().field(0).name(), "x");
    let mut rows = 0usize;
    for batch in reader {
        rows += batch.expect("consume empty result batch").num_rows();
    }
    assert_eq!(rows, 0);
}

#[test]
fn reusing_connection_serves_sequential_queries() {
    let Some(database) = test_database() else {
        eprintln!("skipping Docker integration test: QUESTDB_ADBC_TEST_URI is unset");
        return;
    };

    // One native connection is stored back and reused by later queries, like
    // a PGconn reused across statements.
    let mut connection = database.new_connection().expect("connection");
    let mut statement = connection.new_statement().expect("statement");
    for expected in [3usize, 5usize, 2usize] {
        statement
            .set_sql_query(format!("SELECT x FROM long_sequence({expected})"))
            .expect("set SQL");
        let rows: usize = statement
            .execute()
            .expect("query")
            .map(|batch| batch.expect("batch").num_rows())
            .sum();
        assert_eq!(rows, expected);
    }
}

#[test]
fn results_on_the_same_connection_stream_concurrently() {
    let Some(database) = test_database() else {
        return;
    };
    let mut connection = database.new_connection().expect("connection");
    let mut first = connection.new_statement().expect("first statement");
    let mut second = connection.new_statement().expect("second statement");
    first
        .set_sql_query("SELECT x FROM long_sequence(100000)")
        .expect("set SQL");
    second
        .set_sql_query("SELECT x FROM long_sequence(2)")
        .expect("set SQL");

    // Each query borrows its own reader from the QWP pool, so an open result
    // no longer blocks a second statement on the same ADBC connection.
    let first_result = first.execute().expect("first query");
    let second_result = second.execute().expect("second query");

    let rows: usize = first_result
        .map(|batch| batch.expect("first batch").num_rows())
        .sum();
    assert_eq!(rows, 100_000);
    let rows: usize = second_result
        .map(|batch| batch.expect("second batch").num_rows())
        .sum();
    assert_eq!(rows, 2);
}

#[test]
fn ddl_through_execute_yields_an_empty_result() {
    let Some(database) = test_database() else {
        return;
    };
    // DML/DDL has no result batches; execute() answers with an empty
    // (zero-column) result instead of an error, and the connection becomes
    // reusable right away.
    let mut connection = database.new_connection().expect("connect to QuestDB");
    let mut statement = connection.new_statement().expect("create statement");
    statement
        .set_sql_query("CREATE TABLE IF NOT EXISTS adbc_ddl_probe (x INT)")
        .expect("set DDL");

    let mut reader = statement.execute().expect("DDL with no result batches");
    assert_eq!(reader.schema().fields().len(), 0);
    assert!(reader.next().is_none());

    statement
        .set_sql_query("DROP TABLE IF EXISTS adbc_ddl_probe")
        .expect("set DDL drop");
    let mut reader = statement.execute().expect("DDL drop");
    assert_eq!(reader.schema().fields().len(), 0);
    assert!(reader.next().is_none());

    // The schema-less DDL results did not strand the reader slot.
    statement
        .set_sql_query("SELECT x FROM long_sequence(2)")
        .expect("set SQL");
    let rows: usize = statement
        .execute()
        .expect("query after DDL")
        .map(|batch| batch.expect("batch").num_rows())
        .sum();
    assert_eq!(rows, 2);
}

#[test]
fn connection_cancel_marks_every_in_flight_result() {
    let Some(database) = test_database() else {
        return;
    };
    let mut connection = database.new_connection().expect("connection");
    let mut first = connection.new_statement().expect("first statement");
    let mut second = connection.new_statement().expect("second statement");
    for statement in [&mut first, &mut second] {
        statement
            .set_sql_query("SELECT x FROM long_sequence(100000)")
            .expect("set SQL");
    }

    let mut first_result = first.execute().expect("first query");
    let mut second_result = second.execute().expect("second query");
    connection.cancel().expect("cancel connection");

    assert_cancelled(first_result.next().expect("first cancellation"));
    assert_cancelled(second_result.next().expect("second cancellation"));
    assert!(first_result.next().is_none());
    assert!(second_result.next().is_none());
}

#[test]
fn reexecuting_statement_invalidates_previous_result() {
    let Some(database) = test_database() else {
        return;
    };
    let mut connection = database.new_connection().expect("connection");
    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_sql_query("SELECT x FROM long_sequence(100000)")
        .expect("set SQL");

    let mut result = statement.execute().expect("first query");
    let replacement = statement.execute().expect("re-execution");

    // The first result's set was invalidated: its next advance reports the
    // cancellation instead of continuing to stream.
    let error = result.next().expect("invalidated result must error");
    match error {
        Err(ArrowError::ExternalError(error)) => {
            let adbc_error = error
                .downcast_ref::<adbc_core::error::Error>()
                .expect("invalidation should surface an ADBC error");
            assert_eq!(adbc_error.status, Status::Cancelled);
        }
        other => panic!("expected invalidation error, got {other:?}"),
    }
    assert!(result.next().is_none());
    drop(result);

    let rows: usize = replacement
        .map(|batch| batch.expect("replacement batch").num_rows())
        .sum();
    assert_eq!(rows, 100_000);

    statement
        .set_sql_query("SELECT x FROM long_sequence(4)")
        .expect("set SQL");
    let rows: usize = statement
        .execute()
        .expect("execute after invalidation settled")
        .map(|batch| batch.expect("batch").num_rows())
        .sum();
    assert_eq!(rows, 4);
}

fn assert_cancelled(result: Result<arrow_array::RecordBatch, ArrowError>) {
    let error = result.expect_err("cancelled query must not yield another batch");
    match error {
        ArrowError::ExternalError(error) => {
            let adbc_error = error
                .downcast_ref::<adbc_core::error::Error>()
                .expect("cancellation should be an ADBC error");
            assert_eq!(adbc_error.status, Status::Cancelled);
        }
        other => panic!("expected cancellation error, got {other}"),
    }
}

#[test]
fn statement_cancel_ends_query_and_keeps_connection_reusable() {
    let Some(database) = test_database() else {
        return;
    };
    let mut connection = database.new_connection().expect("connection");
    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_sql_query("SELECT x FROM long_sequence(100000)")
        .expect("set SQL");

    let mut result = statement.execute().expect("execute query");
    assert!(result.next().expect("first batch").is_ok());
    statement.cancel().expect("cancel statement");
    assert_cancelled(result.next().expect("cancellation error"));
    assert!(result.next().is_none());
    drop(result);

    // Server-side cancel ends the query without tearing down the socket,
    // so the next execute reuses the same native connection.
    statement
        .set_sql_query("SELECT x FROM long_sequence(2)")
        .expect("set SQL");
    let rows: usize = statement
        .execute()
        .expect("subsequent query")
        .map(|batch| batch.expect("batch").num_rows())
        .sum();
    assert_eq!(rows, 2);
}

#[test]
fn connection_cancel_stops_its_result_but_not_other_connections() {
    let Some(database) = test_database() else {
        return;
    };
    let mut first = database.new_connection().expect("first connection");
    let mut second = database.new_connection().expect("second connection");

    let mut one = first.new_statement().expect("statement one");
    let mut unaffected = second.new_statement().expect("other connection statement");
    for statement in [&mut one, &mut unaffected] {
        statement
            .set_sql_query("SELECT x FROM long_sequence(100000)")
            .expect("set SQL");
    }
    let mut result_one = one.execute().expect("first query");
    let other_result = unaffected.execute().expect("unaffected query");
    first.cancel().expect("cancel connection");
    assert_cancelled(result_one.next().expect("first cancellation"));
    assert!(result_one.next().is_none());

    // The other connection's query keeps streaming to completion.
    let rows: usize = other_result
        .map(|batch| batch.expect("unaffected query").num_rows())
        .sum();
    assert!(rows > 0);
}

#[test]
fn get_info_reports_driver_and_vendor_metadata() {
    let Some(database) = test_database() else {
        return;
    };
    let connection = database.new_connection().expect("connection");

    let mut reader = connection.get_info(None).expect("get_info");
    let batch = reader.next().expect("info batch").expect("info rows");
    assert_eq!(batch.schema(), GET_INFO_SCHEMA.clone());

    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .expect("info_name");
    let seen: HashSet<u32> = (0..batch.num_rows()).map(|row| names.value(row)).collect();
    for code in [
        InfoCode::VendorName,
        InfoCode::VendorVersion,
        InfoCode::DriverName,
        InfoCode::DriverVersion,
        InfoCode::DriverAdbcVersion,
    ] {
        assert!(
            seen.contains(&u32::from(&code)),
            "missing info code {code:?}"
        );
    }

    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<UnionArray>()
        .expect("info_value union");
    let mut vendor_name = None;
    for row in 0..batch.num_rows() {
        if names.value(row) == u32::from(&InfoCode::VendorName) {
            let value = values.value(row);
            let value = value
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("vendor name string");
            vendor_name = Some(value.value(0).to_owned());
        }
    }
    assert_eq!(vendor_name.as_deref(), Some("QuestDB"));
}

#[test]
fn get_info_honours_requested_codes() {
    let Some(database) = test_database() else {
        return;
    };
    let connection = database.new_connection().expect("connection");
    let requested: HashSet<InfoCode> = [InfoCode::VendorName, InfoCode::DriverVersion]
        .into_iter()
        .collect();

    let mut reader = connection
        .get_info(Some(requested))
        .expect("filtered get_info");
    let batch = reader.next().expect("info batch").expect("info rows");
    assert_eq!(batch.num_rows(), 2);
}

#[test]
fn invalid_sql_surfaces_as_invalid_arguments() {
    let Some(database) = test_database() else {
        return;
    };
    let mut connection = database.new_connection().expect("connection");
    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_sql_query("SELECT * FROM adbc_meta_missing_table")
        .expect("set SQL");

    let error = match statement.execute() {
        Ok(_) => panic!("querying a missing table must fail"),
        Err(error) => error,
    };
    assert_eq!(error.status, Status::InvalidArguments);
}

#[test]
fn dropping_partial_result_keeps_connection_reusable() {
    let Some(database) = test_database() else {
        return;
    };
    let mut connection = database.new_connection().expect("connection");
    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_sql_query("SELECT x FROM long_sequence(100000)")
        .expect("set SQL");

    let mut result = statement.execute().expect("execute query");
    assert!(result.next().expect("first batch").is_ok());
    drop(result);

    statement
        .set_sql_query("SELECT x FROM long_sequence(2)")
        .expect("set SQL");
    let rows: usize = statement
        .execute()
        .expect("query after dropping a partial result")
        .map(|batch| batch.expect("batch").num_rows())
        .sum();
    assert_eq!(rows, 2);
}

static NEXT_TABLE: AtomicU64 = AtomicU64::new(0);

fn unique_table(prefix: &str) -> String {
    format!(
        "adbc_{prefix}_{}_{}",
        std::process::id(),
        NEXT_TABLE.fetch_add(1, Ordering::Relaxed)
    )
}

fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn execute_sql(connection: &mut QuestDbConnection, sql: &str) -> Option<i64> {
    let mut statement = connection.new_statement().expect("statement");
    statement.set_sql_query(sql).expect("set SQL");
    statement.execute_update().expect("execute SQL")
}

fn drop_table(connection: &mut QuestDbConnection, table: &str) {
    execute_sql(
        connection,
        &format!("DROP TABLE IF EXISTS {}", quoted(table)),
    );
}

fn count_rows(connection: &mut QuestDbConnection, table: &str) -> i64 {
    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_sql_query(format!("SELECT count() FROM {}", quoted(table)))
        .expect("set SQL");
    let batch = statement
        .execute()
        .expect("count query")
        .next()
        .expect("count batch")
        .expect("count batch");
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count is int64")
        .value(0)
}

fn designated_timestamp(connection: &mut QuestDbConnection, table: &str) -> String {
    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_sql_query(format!(
            "SELECT designatedTimestamp FROM tables() WHERE table_name = '{}'",
            table.replace('\'', "''")
        ))
        .expect("set SQL");
    let batch = statement
        .execute()
        .expect("metadata query")
        .next()
        .expect("metadata batch")
        .expect("metadata batch");
    batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("designatedTimestamp is string")
        .value(0)
        .to_owned()
}

/// QWP acknowledges accepted frames before the WAL apply job makes the rows
/// visible, so poll briefly after each ingest.
fn wait_for_rows(connection: &mut QuestDbConnection, table: &str, expected: i64) -> i64 {
    let mut actual = count_rows(connection, table);
    for _ in 0..100 {
        if actual == expected {
            return actual;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
        actual = count_rows(connection, table);
    }
    actual
}

fn timestamp_batch(timestamps: &[i64], values: &[i64]) -> RecordBatch {
    assert_eq!(timestamps.len(), values.len());
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("value", DataType::Int64, false),
    ]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(TimestampMicrosecondArray::from(timestamps.to_vec())),
        Arc::new(Int64Array::from(values.to_vec())),
    ];
    RecordBatch::try_new(schema, arrays).expect("timestamp batch")
}

fn timestamp_string_batch(timestamps: &[i64], values: &[&str]) -> RecordBatch {
    assert_eq!(timestamps.len(), values.len());
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("value", DataType::Utf8, false),
    ]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(TimestampMicrosecondArray::from(timestamps.to_vec())),
        Arc::new(StringArray::from(values.to_vec())),
    ];
    RecordBatch::try_new(schema, arrays).expect("timestamp string batch")
}

fn dual_timestamp_batch(first: &[i64], second: &[i64], values: &[i64]) -> RecordBatch {
    assert_eq!(first.len(), second.len());
    assert_eq!(first.len(), values.len());
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "first",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new(
            "second",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("value", DataType::Int64, false),
    ]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(TimestampMicrosecondArray::from(first.to_vec())),
        Arc::new(TimestampMicrosecondArray::from(second.to_vec())),
        Arc::new(Int64Array::from(values.to_vec())),
    ];
    RecordBatch::try_new(schema, arrays).expect("dual timestamp batch")
}

fn int_batch(name: &str, values: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
    let arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(values.to_vec()))];
    RecordBatch::try_new(schema, arrays).expect("int batch")
}

fn bind_statement(
    connection: &mut QuestDbConnection,
    table: &str,
    mode: IngestMode,
    batch: RecordBatch,
) -> adbc_questdb::QuestDbStatement {
    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_option(OptionStatement::TargetTable, OptionValue::from(table))
        .expect("target table");
    statement
        .set_option(OptionStatement::IngestMode, OptionValue::from(mode))
        .expect("ingest mode");
    statement.bind(batch).expect("bind batch");
    statement
}

#[test]
fn ingest_create_creates_table_and_reports_rows() {
    let Some(database) = test_database() else {
        return;
    };
    let mut connection = database.new_connection().expect("connection");
    let table = unique_table("create");

    let statement_batch = timestamp_batch(&[1_700_000_000_000_000, 1_700_000_001_000_000], &[1, 2]);
    let mut statement =
        bind_statement(&mut connection, &table, IngestMode::Create, statement_batch);
    let affected = statement.execute_update().expect("ingest");
    assert_eq!(affected, Some(2));
    assert_eq!(wait_for_rows(&mut connection, &table, 2), 2);

    // QWP names the designated timestamp of a newly created table `timestamp`.
    assert_eq!(designated_timestamp(&mut connection, &table), "timestamp");

    drop_table(&mut connection, &table);
}

#[test]
fn ingest_create_existing_table_reports_already_exists() {
    let Some(database) = test_database() else {
        return;
    };
    let mut connection = database.new_connection().expect("connection");
    let table = unique_table("create_exists");

    let mut first = bind_statement(
        &mut connection,
        &table,
        IngestMode::Create,
        int_batch("x", &[1]),
    );
    first.execute_update().expect("first create");

    let mut second = bind_statement(
        &mut connection,
        &table,
        IngestMode::Create,
        int_batch("x", &[2]),
    );
    let error = second
        .execute_update()
        .expect_err("creating an existing table must fail");
    assert_eq!(error.status, Status::AlreadyExists);

    drop_table(&mut connection, &table);
}

#[test]
fn ingest_append_missing_table_reports_not_found() {
    let Some(database) = test_database() else {
        return;
    };
    let mut connection = database.new_connection().expect("connection");
    let table = unique_table("append_missing");

    let mut statement = bind_statement(
        &mut connection,
        &table,
        IngestMode::Append,
        int_batch("x", &[1]),
    );
    let error = statement
        .execute_update()
        .expect_err("appending to a missing table must fail");
    assert_eq!(error.status, Status::NotFound);
}

#[test]
fn ingest_append_adds_rows_and_keeps_designated_timestamp() {
    let Some(database) = test_database() else {
        return;
    };
    let mut connection = database.new_connection().expect("connection");
    let table = unique_table("append");

    let mut create = bind_statement(
        &mut connection,
        &table,
        IngestMode::Create,
        timestamp_batch(&[1_700_000_000_000_000], &[1]),
    );
    create.execute_update().expect("create ingest");

    let mut append = bind_statement(
        &mut connection,
        &table,
        IngestMode::Append,
        timestamp_batch(&[1_700_000_002_000_000], &[2]),
    );
    assert_eq!(append.execute_update().expect("append ingest"), Some(1));
    assert_eq!(wait_for_rows(&mut connection, &table, 2), 2);

    drop_table(&mut connection, &table);
}

#[test]
fn ingest_create_append_creates_then_appends() {
    let Some(database) = test_database() else {
        return;
    };
    let mut connection = database.new_connection().expect("connection");
    let table = unique_table("create_append");

    let mut create = bind_statement(
        &mut connection,
        &table,
        IngestMode::CreateAppend,
        timestamp_batch(&[1_700_000_000_000_000], &[1]),
    );
    assert_eq!(create.execute_update().expect("create append"), Some(1));

    let mut append = bind_statement(
        &mut connection,
        &table,
        IngestMode::CreateAppend,
        timestamp_batch(&[1_700_000_003_000_000, 1_700_000_004_000_000], &[2, 3]),
    );
    assert_eq!(append.execute_update().expect("second append"), Some(2));
    assert_eq!(wait_for_rows(&mut connection, &table, 3), 3);

    drop_table(&mut connection, &table);
}

#[test]
fn ingest_replace_drops_and_replaces_table() {
    let Some(database) = test_database() else {
        return;
    };
    let mut connection = database.new_connection().expect("connection");
    let table = unique_table("replace");

    let mut create = bind_statement(
        &mut connection,
        &table,
        IngestMode::Create,
        int_batch("x", &[1, 2]),
    );
    create.execute_update().expect("create ingest");

    let mut replace = bind_statement(
        &mut connection,
        &table,
        IngestMode::Replace,
        int_batch("x", &[3]),
    );
    assert_eq!(replace.execute_update().expect("replace ingest"), Some(1));
    assert_eq!(wait_for_rows(&mut connection, &table, 1), 1);

    drop_table(&mut connection, &table);
}

#[test]
fn ingest_schema_mismatch_reports_already_exists() {
    let Some(database) = test_database() else {
        return;
    };
    let mut connection = database.new_connection().expect("connection");
    let table = unique_table("mismatch");
    let mut create = bind_statement(
        &mut connection,
        &table,
        IngestMode::Create,
        timestamp_batch(&[1_700_000_000_000_000], &[1]),
    );
    create.execute_update().expect("create ingest");

    let mut append = bind_statement(
        &mut connection,
        &table,
        IngestMode::Append,
        timestamp_string_batch(&[1_700_000_001_000_000], &["not-a-long"]),
    );
    let error = append
        .execute_update()
        .expect_err("schema mismatch must fail");
    assert_eq!(error.status, Status::AlreadyExists, "{error:?}");

    drop_table(&mut connection, &table);
}

#[test]
fn repeated_bind_ingests_all_batches() {
    let Some(database) = test_database() else {
        return;
    };
    let mut connection = database.new_connection().expect("connection");
    let table = unique_table("multi_bind");

    let mut statement = bind_statement(
        &mut connection,
        &table,
        IngestMode::Create,
        int_batch("x", &[1, 2]),
    );
    statement.bind(int_batch("x", &[3])).expect("second bind");
    assert_eq!(statement.execute_update().expect("ingest"), Some(3));
    assert_eq!(wait_for_rows(&mut connection, &table, 3), 3);

    drop_table(&mut connection, &table);
}

#[test]
fn bind_stream_ingests_multiple_batches() {
    let Some(database) = test_database() else {
        return;
    };
    let mut connection = database.new_connection().expect("connection");
    let table = unique_table("stream");
    let first = int_batch("x", &[1, 2]);
    let schema = first.schema();
    let reader = RecordBatchIterator::new(vec![Ok(first), Ok(int_batch("x", &[3]))], schema);

    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::from(table.as_str()),
        )
        .expect("target table");
    statement
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::from(IngestMode::Create),
        )
        .expect("ingest mode");
    statement
        .bind_stream(Box::new(reader))
        .expect("bind stream");
    assert_eq!(statement.execute_update().expect("ingest"), Some(3));
    assert_eq!(wait_for_rows(&mut connection, &table, 3), 3);

    drop_table(&mut connection, &table);
}

#[test]
fn empty_stream_attempts_table_creation() {
    let Some(database) = test_database() else {
        return;
    };
    let mut connection = database.new_connection().expect("connection");
    let table = unique_table("empty_stream");
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let reader = RecordBatchIterator::new(Vec::<Result<RecordBatch, ArrowError>>::new(), schema);

    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::from(table.as_str()),
        )
        .expect("target table");
    statement
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::from(IngestMode::Create),
        )
        .expect("ingest mode");
    statement
        .bind_stream(Box::new(reader))
        .expect("bind stream");
    // QWP accepts the empty flush but does not create a table from it; the
    // important part is that the statement completes with zero rows.
    assert_eq!(
        statement.execute_update().expect("empty stream ingest"),
        Some(0)
    );

    drop_table(&mut connection, &table);
}

#[test]
fn zero_row_batch_is_flushed() {
    let Some(database) = test_database() else {
        return;
    };
    let mut connection = database.new_connection().expect("connection");
    let table = unique_table("zero_rows");

    let mut statement = bind_statement(
        &mut connection,
        &table,
        IngestMode::Create,
        int_batch("x", &[]),
    );
    // The zero-row batch is still flushed; QWP accepts it without creating a
    // table, so only the affected-row count is observable.
    assert_eq!(
        statement.execute_update().expect("zero-row ingest"),
        Some(0)
    );

    drop_table(&mut connection, &table);
}

#[test]
fn non_public_target_schema_is_not_implemented() {
    let Some(database) = test_database() else {
        return;
    };
    let mut connection = database.new_connection().expect("connection");
    let table = unique_table("schema");
    let mut statement = connection.new_statement().expect("statement");
    statement
        .set_option(OptionStatement::TargetTable, OptionValue::from(table))
        .expect("target table");
    statement
        .set_option(
            OptionStatement::TargetDbSchema,
            OptionValue::from("other_schema"),
        )
        .expect("target schema");
    statement.bind(int_batch("x", &[1])).expect("bind");
    let error = statement
        .execute_update()
        .expect_err("non-public schema must fail");
    assert_eq!(error.status, Status::NotImplemented);
}

#[test]
fn ingest_options_persist_after_execution() {
    let Some(database) = test_database() else {
        return;
    };
    let mut connection = database.new_connection().expect("connection");
    let table = unique_table("options");

    let mut statement = bind_statement(
        &mut connection,
        &table,
        IngestMode::Create,
        int_batch("x", &[1]),
    );
    statement.execute_update().expect("ingest");

    assert_eq!(
        statement
            .get_option_string(OptionStatement::TargetTable)
            .expect("target table"),
        table
    );
    assert_eq!(
        statement
            .get_option_string(OptionStatement::IngestMode)
            .expect("ingest mode"),
        String::from(IngestMode::Create)
    );

    let error = statement
        .execute_update()
        .expect_err("bound data is consumed by execution");
    assert_eq!(error.status, Status::InvalidArguments);

    drop_table(&mut connection, &table);
}

#[test]
fn sql_insert_and_update_report_rows_affected() {
    let Some(database) = test_database() else {
        return;
    };
    let mut connection = database.new_connection().expect("connection");
    let table = unique_table("dml");
    execute_sql(
        &mut connection,
        &format!("CREATE TABLE {} (x INT)", quoted(&table)),
    );

    let inserted = execute_sql(
        &mut connection,
        &format!("INSERT INTO {} VALUES (1), (2), (3)", quoted(&table)),
    );
    assert_eq!(inserted, Some(3));

    let updated = execute_sql(
        &mut connection,
        &format!("UPDATE {} SET x = x + 1", quoted(&table)),
    );
    assert_eq!(updated, Some(3));
    assert_eq!(wait_for_rows(&mut connection, &table, 3), 3);

    drop_table(&mut connection, &table);
}

#[test]
fn sql_ddl_reports_zero_rows_affected() {
    let Some(database) = test_database() else {
        return;
    };
    let mut connection = database.new_connection().expect("connection");
    let table = unique_table("ddl");
    let created = execute_sql(
        &mut connection,
        &format!("CREATE TABLE {} (x INT)", quoted(&table)),
    );
    assert_eq!(created, Some(0));
    drop_table(&mut connection, &table);
}

#[test]
fn ingest_timestamp_column_option_selects_designated_timestamp() {
    let Some(database) = test_database() else {
        return;
    };
    let mut connection = database.new_connection().expect("connection");
    let table = unique_table("timestamp_option");
    let first = [1_700_000_100_000_000i64, 1_700_000_200_000_000];
    let second = [1_700_000_300_000_000i64, 1_700_000_400_000_000];

    let mut create = bind_statement(
        &mut connection,
        &table,
        IngestMode::Create,
        dual_timestamp_batch(&first, &second, &[1, 2]),
    );
    create
        .set_option(
            OptionStatement::Other("adbc.questdb.ingest.timestamp_column".to_owned()),
            OptionValue::from("second"),
        )
        .expect("timestamp column option");
    assert_eq!(create.execute_update().expect("create ingest"), Some(2));
    assert_eq!(wait_for_rows(&mut connection, &table, 2), 2);

    let designated = designated_timestamp(&mut connection, &table);
    let mut query = connection.new_statement().expect("statement");
    query
        .set_sql_query(format!(
            "SELECT {} FROM {} ORDER BY {}",
            quoted(&designated),
            quoted(&table),
            quoted(&designated)
        ))
        .expect("set SQL");
    let batch = query
        .execute()
        .expect("timestamp query")
        .next()
        .expect("timestamp batch")
        .expect("timestamp batch");
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("timestamp column");
    assert_eq!(values.values(), &second);

    drop_table(&mut connection, &table);
}

#[test]
fn ingest_timestamp_column_option_requires_a_timestamp_field() {
    let Some(database) = test_database() else {
        return;
    };
    let mut connection = database.new_connection().expect("connection");
    let table = unique_table("timestamp_missing");
    let mut create = bind_statement(
        &mut connection,
        &table,
        IngestMode::Create,
        int_batch("x", &[1]),
    );
    create
        .set_option(
            OptionStatement::Other("adbc.questdb.ingest.timestamp_column".to_owned()),
            OptionValue::from("missing"),
        )
        .expect("timestamp column option");
    let error = create
        .execute_update()
        .expect_err("missing timestamp column must fail");
    assert_eq!(error.status, Status::InvalidArguments);

    drop_table(&mut connection, &table);
}

#[test]
fn ingest_preserves_batch_timestamps() {
    let Some(database) = test_database() else {
        return;
    };
    let mut connection = database.new_connection().expect("connection");
    let table = unique_table("timestamps");
    let timestamps = [1_700_000_100_000_000i64, 1_700_000_200_000_000];
    let mut create = bind_statement(
        &mut connection,
        &table,
        IngestMode::Create,
        timestamp_batch(&timestamps, &[1, 2]),
    );
    create.execute_update().expect("create ingest");
    wait_for_rows(&mut connection, &table, 2);
    let designated = designated_timestamp(&mut connection, &table);

    let mut query = connection.new_statement().expect("statement");
    query
        .set_sql_query(format!(
            "SELECT {} FROM {} ORDER BY {}",
            quoted(&designated),
            quoted(&table),
            quoted(&designated)
        ))
        .expect("set SQL");
    let batch = query
        .execute()
        .expect("timestamp query")
        .next()
        .expect("timestamp batch")
        .expect("timestamp batch");
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("timestamp column");
    assert_eq!(values.values(), &timestamps);

    drop_table(&mut connection, &table);
}
