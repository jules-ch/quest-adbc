use std::collections::HashMap;

use adbc_core::error::{Error, Status};
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Optionable, Statement};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;

use crate::reader::QuestDbBatchReader;
use crate::writer;

pub struct QuestDbStatement {
    pub(crate) conf: String,
    sql: Option<String>,
    options: HashMap<String, String>,
}

impl QuestDbStatement {
    pub(crate) fn new(conf: String) -> Self {
        Self {
            conf,
            sql: None,
            options: HashMap::new(),
        }
    }
}

impl Statement for QuestDbStatement {
    fn bind(&mut self, _batch: RecordBatch) -> adbc_core::error::Result<()> {
        writer::not_implemented()
    }

    fn bind_stream(
        &mut self,
        _reader: Box<dyn RecordBatchReader + Send>,
    ) -> adbc_core::error::Result<()> {
        writer::not_implemented()
    }

    fn execute(&mut self) -> adbc_core::error::Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let sql = self.sql.clone().ok_or_else(|| {
            Error::with_message_and_status(
                "SQL query must be set before execution",
                Status::InvalidState,
            )
        })?;
        Ok(Box::new(QuestDbBatchReader::execute(
            self.conf.clone(),
            sql,
        )?))
    }

    fn execute_update(&mut self) -> adbc_core::error::Result<Option<i64>> {
        writer::not_implemented()
    }

    fn execute_schema(&mut self) -> adbc_core::error::Result<Schema> {
        Err(Error::with_message_and_status(
            "schema-only execution is not implemented",
            Status::NotImplemented,
        ))
    }

    fn execute_partitions(&mut self) -> adbc_core::error::Result<adbc_core::PartitionedResult> {
        Err(Error::with_message_and_status(
            "partitioned execution is not implemented",
            Status::NotImplemented,
        ))
    }

    fn get_parameter_schema(&self) -> adbc_core::error::Result<Schema> {
        Err(Error::with_message_and_status(
            "parameter binding is not implemented",
            Status::NotImplemented,
        ))
    }

    fn prepare(&mut self) -> adbc_core::error::Result<()> {
        Err(Error::with_message_and_status(
            "prepared statements are not implemented",
            Status::NotImplemented,
        ))
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> adbc_core::error::Result<()> {
        self.sql = Some(query.as_ref().to_owned());
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> adbc_core::error::Result<()> {
        Err(Error::with_message_and_status(
            "Substrait plans are not implemented",
            Status::NotImplemented,
        ))
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        // Dropping the returned ADBC reader drops its channel receiver; the
        // worker observes that and drops the QWP cursor to terminate the stream.
        Ok(())
    }
}

impl Optionable for QuestDbStatement {
    type Option = OptionStatement;

    fn set_option(
        &mut self,
        key: Self::Option,
        value: OptionValue,
    ) -> adbc_core::error::Result<()> {
        let option_name = key.as_ref().to_owned();
        match key {
            OptionStatement::Other(name) => {
                self.options
                    .insert(name, crate::options::option_string(value, &option_name)?);
                Ok(())
            }
            other => Err(crate::options::unsupported(other.as_ref())),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
        match key {
            OptionStatement::Other(name) => self.options.get(&name).cloned().ok_or_else(|| {
                Error::with_message_and_status(format!("option {name:?} not set"), Status::NotFound)
            }),
            other => Err(crate::options::unsupported(other.as_ref())),
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
