//! ADBC driver for QuestDB's QWP-over-WebSocket protocol.
//!
//! Supports streaming query results and bulk ingestion through QWP. SQL DML and
//! DDL executed with [`adbc_core::Statement::execute_update`] report the row
//! counts sent by QuestDB, while [`adbc_core::Statement::execute`] streams Arrow
//! batches incrementally.

mod arrow_cursor;
mod cancellation;
mod connection;
mod database;
mod error;
mod info;
mod options;
mod qwp_connection;
mod reader;
mod statement;
mod writer;

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Driver, Optionable};

pub use connection::QuestDbConnection;
pub use database::QuestDbDatabase;
pub use statement::QuestDbStatement;

#[cfg(feature = "ffi")]
adbc_ffi::export_driver!(AdbcQuestdbInit, QuestDbDriver);

#[derive(Default)]
pub struct QuestDbDriver;

impl Driver for QuestDbDriver {
    type DatabaseType = QuestDbDatabase;

    fn new_database(&mut self) -> adbc_core::error::Result<Self::DatabaseType> {
        Ok(QuestDbDatabase::default())
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> adbc_core::error::Result<Self::DatabaseType> {
        let mut database = QuestDbDatabase::default();
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        Ok(database)
    }
}
