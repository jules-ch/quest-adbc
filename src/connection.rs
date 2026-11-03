use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::error::{Error, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue};
use adbc_core::{Connection, Optionable};
use arrow_array::RecordBatchReader;
use arrow_schema::Schema;

use crate::info;
use crate::options;
use crate::qwp_connection::QwpConnection;
use crate::statement::QuestDbStatement;

/// An ADBC connection backed by one logical QWP connection.
pub struct QuestDbConnection {
    qwp_connection: Arc<QwpConnection>,
    options: HashMap<String, String>,
}

impl QuestDbConnection {
    pub(crate) fn new(qwp_connection: Arc<QwpConnection>) -> Self {
        Self {
            qwp_connection,
            options: HashMap::new(),
        }
    }
}

impl Connection for QuestDbConnection {
    type StatementType = QuestDbStatement;

    fn new_statement(&mut self) -> adbc_core::error::Result<Self::StatementType> {
        Ok(QuestDbStatement::new(Arc::clone(&self.qwp_connection)))
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        // Marks the connection's in-flight result, if any; its stream ends
        // the query server-side when it is next advanced.
        self.qwp_connection.cancel_in_flight();
        Ok(())
    }

    fn get_info(
        &self,
        codes: Option<std::collections::HashSet<InfoCode>>,
    ) -> adbc_core::error::Result<Box<dyn RecordBatchReader + Send + 'static>> {
        info::get_info(&self.qwp_connection, codes)
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
        Err(options::unsupported("get_table_schema"))
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
