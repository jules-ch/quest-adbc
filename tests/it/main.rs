use std::env;

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Statement};

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
