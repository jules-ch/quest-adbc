//! Read-focused ADBC driver for QuestDB's QWP-over-WebSocket protocol.

mod arrow_cursor;
mod cancellation;
mod connection;
mod database;
mod error;
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
