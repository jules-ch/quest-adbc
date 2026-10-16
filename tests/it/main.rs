use std::env;

use adbc_core::error::Status;
use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Statement};
use arrow_schema::ArrowError;

use adbc_questdb::{QuestDbDatabase, QuestDbDriver};

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
fn active_result_blocks_same_connection_until_released() {
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

    // One query in flight per connection, like libpq: the second execute is
    // refused with InvalidState while the first result is still open.
    let result = first.execute().expect("first query");
    let error = match second.execute() {
        Ok(_) => panic!("an active result must prevent a second execute"),
        Err(error) => error,
    };
    assert_eq!(error.status, Status::InvalidState);
    drop(result);

    let rows: usize = second
        .execute()
        .expect("query after first result released")
        .map(|batch| batch.expect("batch").num_rows())
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
    let error = match statement.execute() {
        Ok(_) => panic!("an active result must prevent re-execution"),
        Err(error) => error,
    };
    assert_eq!(error.status, Status::InvalidState);

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
    drop(result);

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
