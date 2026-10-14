//! Read-focused ADBC driver for QuestDB's QWP-over-WebSocket protocol.

mod options;
mod reader;
mod schema;
mod statement;
mod writer;

use std::collections::HashMap;

use adbc_core::error::{Error, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable};
use arrow_array::RecordBatchReader;
use arrow_schema::Schema;

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

#[derive(Default)]
pub struct QuestDbDatabase {
    uri: Option<String>,
    username: Option<String>,
    password: Option<String>,
    options: HashMap<String, String>,
}

impl Database for QuestDbDatabase {
    type ConnectionType = QuestDbConnection;

    fn new_connection(&self) -> adbc_core::error::Result<Self::ConnectionType> {
        self.new_connection_with_opts([])
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> adbc_core::error::Result<Self::ConnectionType> {
        let conf = reader::build_conf(
            self.uri.as_deref(),
            self.username.as_deref(),
            self.password.as_deref(),
        )?;
        let mut connection = QuestDbConnection {
            conf,
            options: HashMap::new(),
        };
        for (key, value) in opts {
            connection.set_option(key, value)?;
        }
        Ok(connection)
    }
}

impl Optionable for QuestDbDatabase {
    type Option = OptionDatabase;

    fn set_option(
        &mut self,
        key: Self::Option,
        value: OptionValue,
    ) -> adbc_core::error::Result<()> {
        let option_name = key.as_ref().to_owned();
        match key {
            OptionDatabase::Uri => {
                let uri = options::option_string(value, key.as_ref())?;
                // Validate URI eagerly, before the connection is opened.
                reader::build_conf(Some(&uri), None, None)?;
                self.uri = Some(uri);
                Ok(())
            }
            OptionDatabase::Username => {
                self.username = Some(options::option_string(value, key.as_ref())?);
                Ok(())
            }
            OptionDatabase::Password => {
                self.password = Some(options::option_string(value, key.as_ref())?);
                Ok(())
            }
            OptionDatabase::Other(name) => {
                self.options
                    .insert(name, options::option_string(value, &option_name)?);
                Ok(())
            }
            _ => Err(options::unsupported(key.as_ref())),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
        match key {
            OptionDatabase::Uri | OptionDatabase::Username | OptionDatabase::Password => {
                Err(Error::with_message_and_status(
                    "readback of database URI and credentials is omitted",
                    Status::NotFound,
                ))
            }
            OptionDatabase::Other(name) => self.options.get(&name).cloned().ok_or_else(|| {
                Error::with_message_and_status(format!("option {name:?} not set"), Status::NotFound)
            }),
            _ => Err(options::unsupported(key.as_ref())),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> adbc_core::error::Result<Vec<u8>> {
        self.get_option_string(key).map(String::into_bytes)
    }

    fn get_option_int(&self, key: Self::Option) -> adbc_core::error::Result<i64> {
        Err(Error::with_message_and_status(
            format!("option {:?} is not an integer", key.as_ref()),
            Status::InvalidArguments,
        ))
    }

    fn get_option_double(&self, key: Self::Option) -> adbc_core::error::Result<f64> {
        Err(Error::with_message_and_status(
            format!("option {:?} is not a double", key.as_ref()),
            Status::InvalidArguments,
        ))
    }
}

pub struct QuestDbConnection {
    conf: String,
    options: HashMap<String, String>,
}

impl Connection for QuestDbConnection {
    type StatementType = QuestDbStatement;

    fn new_statement(&mut self) -> adbc_core::error::Result<Self::StatementType> {
        Ok(QuestDbStatement::new(self.conf.clone()))
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        // ADBC result readers own their query worker. Dropping the reader
        // interrupts consumption and closes an unfinished cursor.
        Ok(())
    }

    fn get_info(
        &self,
        _codes: Option<std::collections::HashSet<InfoCode>>,
    ) -> adbc_core::error::Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(options::unsupported("get_info"))
    }

    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> adbc_core::error::Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(options::unsupported("get_objects"))
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> adbc_core::error::Result<Schema> {
        schema::unsupported_schema()
    }

    fn get_table_types(
        &self,
    ) -> adbc_core::error::Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(options::unsupported("get_table_types"))
    }

    fn get_statistic_names(
        &self,
    ) -> adbc_core::error::Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(options::unsupported("get_statistic_names"))
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> adbc_core::error::Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(options::unsupported("get_statistics"))
    }

    fn commit(&mut self) -> adbc_core::error::Result<()> {
        Err(options::read_only_error())
    }

    fn rollback(&mut self) -> adbc_core::error::Result<()> {
        Err(options::read_only_error())
    }

    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> adbc_core::error::Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(options::unsupported("read_partition"))
    }
}

impl Optionable for QuestDbConnection {
    type Option = OptionConnection;

    fn set_option(
        &mut self,
        key: Self::Option,
        value: OptionValue,
    ) -> adbc_core::error::Result<()> {
        let option_name = key.as_ref().to_owned();
        match key {
            OptionConnection::AutoCommit => {
                let value = options::option_string(value, key.as_ref())?;
                if value == "true" {
                    Ok(())
                } else {
                    Err(options::read_only_error())
                }
            }
            OptionConnection::ReadOnly => {
                let value = options::option_string(value, key.as_ref())?;
                if value == "true" {
                    Ok(())
                } else {
                    Err(Error::with_message_and_status(
                        "QuestDB ADBC connection is read-only",
                        Status::InvalidArguments,
                    ))
                }
            }
            OptionConnection::Other(name) => {
                let value = options::option_string(value, &option_name)?;
                self.options.insert(name, value);
                Ok(())
            }
            other => Err(options::unsupported(other.as_ref())),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
        match key {
            OptionConnection::AutoCommit => Ok("true".into()),
            OptionConnection::ReadOnly => Ok("true".into()),
            OptionConnection::Other(name) => self.options.get(&name).cloned().ok_or_else(|| {
                Error::with_message_and_status(format!("option {name:?} not set"), Status::NotFound)
            }),
            other => Err(options::unsupported(other.as_ref())),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> adbc_core::error::Result<Vec<u8>> {
        self.get_option_string(key).map(String::into_bytes)
    }

    fn get_option_int(&self, key: Self::Option) -> adbc_core::error::Result<i64> {
        Err(Error::with_message_and_status(
            format!("option {:?} is not an integer", key.as_ref()),
            Status::InvalidArguments,
        ))
    }

    fn get_option_double(&self, key: Self::Option) -> adbc_core::error::Result<f64> {
        Err(Error::with_message_and_status(
            format!("option {:?} is not a double", key.as_ref()),
            Status::InvalidArguments,
        ))
    }
}
