use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::error::{Error, Status};
use adbc_core::options::{IngestMode, OptionStatement, OptionValue};
use adbc_core::{Optionable, Statement};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{Schema, SchemaRef};
use questdb::egress::Terminal;

use crate::cancellation::CancellationToken;
use crate::qwp_connection::QwpConnection;
use crate::reader::QuestDbArrowStreamReader;
use crate::writer;

/// QuestDB-specific override for the column used as the designated timestamp.
pub(crate) const TIMESTAMP_COLUMN_OPTION: &str = "adbc.questdb.ingest.timestamp_column";

#[derive(Clone, Debug)]
pub(crate) struct IngestOptions {
    /// ADBC `adbc.ingest.target_db_schema`.
    pub(crate) target_schema: Option<String>,
    /// ADBC `adbc.ingest.target_table`.
    pub(crate) target_table: Option<String>,
    /// ADBC `adbc.ingest.mode`; defaults to `Create`.
    pub(crate) mode: IngestMode,
    /// QuestDB-specific designated-timestamp column override.
    pub(crate) timestamp_column: Option<String>,
}

impl Default for IngestOptions {
    fn default() -> Self {
        Self {
            target_schema: None,
            target_table: None,
            mode: IngestMode::Create,
            timestamp_column: None,
        }
    }
}

#[derive(Debug)]
pub(crate) enum StatementMode {
    Unset,
    SqlQuery(String),
    BulkIngest(IngestOptions),
}

/// One-shot data bound for a bulk ingest. Consumed by `execute_update`.
#[derive(Default)]
pub(crate) enum BoundData {
    #[default]
    None,
    Batches {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    },
    Stream {
        schema: SchemaRef,
        reader: Box<dyn RecordBatchReader + Send>,
    },
}

pub struct QuestDbStatement {
    connection: Arc<QwpConnection>,
    /// The most recent execution's cancellation token, kept until replaced
    /// by the next execute. `Statement::cancel` marks it; a second execute
    /// invalidates it so its stream ends on the next advance.
    query_cancel_token: Option<Arc<CancellationToken>>,
    mode: StatementMode,
    bound: BoundData,
    options: HashMap<String, String>,
}

impl QuestDbStatement {
    pub(crate) fn new(connection: Arc<QwpConnection>) -> Self {
        Self {
            connection,
            query_cancel_token: None,
            mode: StatementMode::Unset,
            bound: BoundData::None,
            options: HashMap::new(),
        }
    }

    /// Transition an unset statement into bulk-ingest mode and return its
    /// options. A statement already holding SQL rejects ingest options.
    fn bulk_ingest(&mut self) -> adbc_core::error::Result<&mut IngestOptions> {
        if matches!(self.mode, StatementMode::SqlQuery(_)) {
            return Err(invalid_state(
                "ingest options cannot be set on a statement holding SQL",
            ));
        }
        if matches!(self.mode, StatementMode::Unset) {
            self.mode = StatementMode::BulkIngest(IngestOptions::default());
        }
        match &mut self.mode {
            StatementMode::BulkIngest(options) => Ok(options),
            _ => unreachable!("statement mode was set to bulk ingest"),
        }
    }

    /// Run one SQL DML/DDL statement to completion and report its row count.
    fn execute_sql_update(&mut self, sql: String) -> adbc_core::error::Result<Option<i64>> {
        if let Some(previous) = self.query_cancel_token.as_ref() {
            previous.cancel();
        }
        let query = Arc::new(CancellationToken::default());
        let mut cursor = self.connection.execute(Arc::clone(&query), sql)?;
        self.query_cancel_token = Some(Arc::clone(&query));

        let outcome = (|| {
            loop {
                if query.is_cancelled() {
                    let _ = cursor.cancel();
                    return Err(Error::with_message_and_status(
                        "QuestDB query cancelled",
                        Status::Cancelled,
                    ));
                }
                if cursor.next_batch()?.is_none() {
                    break;
                }
            }
            Ok(match cursor.terminal() {
                Some(Terminal::ExecDone { rows_affected, .. }) => {
                    Some(i64::try_from(rows_affected).unwrap_or(i64::MAX))
                }
                _ => None,
            })
        })();

        self.query_cancel_token = None;
        outcome
    }
}

impl Statement for QuestDbStatement {
    fn bind(&mut self, batch: RecordBatch) -> adbc_core::error::Result<()> {
        if matches!(self.mode, StatementMode::SqlQuery(_)) {
            return Err(invalid_state("cannot bind data to a statement holding SQL"));
        }

        let schema = batch.schema();
        if let BoundData::Batches {
            schema: existing,
            batches,
        } = &mut self.bound
        {
            if *existing != schema {
                return Err(Error::with_message_and_status(
                    "bound RecordBatch schema differs from the first bound schema",
                    Status::InvalidArguments,
                ));
            }
            batches.push(batch);
            return Ok(());
        }
        if matches!(self.bound, BoundData::Stream { .. }) {
            return Err(invalid_state("cannot mix bind and bind_stream"));
        }

        if matches!(self.mode, StatementMode::Unset) {
            self.mode = StatementMode::BulkIngest(IngestOptions::default());
        }
        self.bound = BoundData::Batches {
            schema,
            batches: vec![batch],
        };
        Ok(())
    }

    fn bind_stream(
        &mut self,
        reader: Box<dyn RecordBatchReader + Send>,
    ) -> adbc_core::error::Result<()> {
        if matches!(self.mode, StatementMode::SqlQuery(_)) {
            return Err(invalid_state("cannot bind data to a statement holding SQL"));
        }
        if !matches!(self.bound, BoundData::None) {
            return Err(invalid_state(
                "cannot mix bind and bind_stream or bind twice",
            ));
        }

        if matches!(self.mode, StatementMode::Unset) {
            self.mode = StatementMode::BulkIngest(IngestOptions::default());
        }
        let schema = reader.schema();
        self.bound = BoundData::Stream { schema, reader };
        Ok(())
    }

    fn execute(&mut self) -> adbc_core::error::Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let sql = match &self.mode {
            StatementMode::SqlQuery(sql) => sql.clone(),
            StatementMode::Unset => {
                return Err(invalid_state("SQL query must be set before execution"));
            }
            StatementMode::BulkIngest(_) => {
                return Err(invalid_state(
                    "bulk ingest statements must be executed with execute_update",
                ));
            }
        };

        // ADBC semantics: a second Execute on the same statement invalidates
        // the first call's result set. The invalidated stream reports its
        // cancellation when it is next advanced.
        if let Some(previous) = self.query_cancel_token.as_ref() {
            previous.cancel();
        }
        let query = Arc::new(CancellationToken::default());
        let cursor = self.connection.execute(Arc::clone(&query), sql)?;
        let reader = QuestDbArrowStreamReader::begin(cursor, Arc::clone(&query))?;
        self.query_cancel_token = Some(query);
        Ok(Box::new(reader))
    }

    fn execute_update(&mut self) -> adbc_core::error::Result<Option<i64>> {
        // Bound data is one-shot: every attempt consumes it, success or not.
        let bound = std::mem::take(&mut self.bound);

        let sql = match &self.mode {
            StatementMode::Unset => {
                return Err(invalid_state(
                    "SQL query or bulk ingest data must be set before execution",
                ));
            }
            StatementMode::SqlQuery(sql) => Some(sql.clone()),
            StatementMode::BulkIngest(_) => None,
        };
        if let Some(sql) = sql {
            return self.execute_sql_update(sql);
        }

        let options = match &self.mode {
            StatementMode::BulkIngest(options) => options.clone(),
            _ => unreachable!("bulk ingest mode checked above"),
        };
        let target = options.target_table.as_deref().ok_or_else(|| {
            Error::with_message_and_status(
                "bulk ingest requires the adbc.ingest.target_table option",
                Status::InvalidArguments,
            )
        })?;
        writer::ingest(&self.connection, &options, target, bound)
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
        self.mode = StatementMode::SqlQuery(query.as_ref().to_owned());
        self.bound = BoundData::None;
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> adbc_core::error::Result<()> {
        Err(Error::with_message_and_status(
            "Substrait plans are not implemented",
            Status::NotImplemented,
        ))
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        // Requests end the in-flight query through the reader's CANCEL frame
        // when the stream is next advanced; the connection itself stays
        // reusable afterwards.
        if let Some(query) = self.query_cancel_token.as_ref() {
            query.cancel();
        }
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
            OptionStatement::TargetTable => {
                let table = crate::options::option_string(value, &option_name)?;
                self.bulk_ingest()?.target_table = Some(table);
                Ok(())
            }
            OptionStatement::TargetDbSchema => {
                let schema = crate::options::option_string(value, &option_name)?;
                self.bulk_ingest()?.target_schema = Some(schema);
                Ok(())
            }
            OptionStatement::IngestMode => {
                let mode = IngestMode::try_from(&value)?;
                self.bulk_ingest()?.mode = mode;
                Ok(())
            }
            OptionStatement::Other(name) if name == TIMESTAMP_COLUMN_OPTION => {
                let column = crate::options::option_string(value, &option_name)?;
                if column.is_empty() {
                    return Err(Error::with_message_and_status(
                        format!("{TIMESTAMP_COLUMN_OPTION} must not be empty"),
                        Status::InvalidArguments,
                    ));
                }
                self.bulk_ingest()?.timestamp_column = Some(column);
                Ok(())
            }
            OptionStatement::Other(name) => {
                self.options
                    .insert(name, crate::options::option_string(value, &option_name)?);
                Ok(())
            }
            other => Err(crate::options::unsupported(other.as_ref())),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
        let option_name = key.as_ref().to_owned();
        match key {
            OptionStatement::TargetTable => self
                .ingest_option()
                .and_then(|options| options.target_table.clone())
                .ok_or_else(|| option_not_found(&option_name)),
            OptionStatement::TargetDbSchema => self
                .ingest_option()
                .and_then(|options| options.target_schema.clone())
                .ok_or_else(|| option_not_found(&option_name)),
            OptionStatement::IngestMode => self
                .ingest_option()
                .map(|options| String::from(options.mode))
                .ok_or_else(|| option_not_found(&option_name)),
            OptionStatement::Other(name) if name == TIMESTAMP_COLUMN_OPTION => self
                .ingest_option()
                .and_then(|options| options.timestamp_column.clone())
                .ok_or_else(|| option_not_found(&option_name)),
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

impl QuestDbStatement {
    fn ingest_option(&self) -> Option<&IngestOptions> {
        match &self.mode {
            StatementMode::BulkIngest(options) => Some(options),
            _ => None,
        }
    }
}

fn invalid_state(message: &str) -> Error {
    Error::with_message_and_status(message, Status::InvalidState)
}

fn option_not_found(name: &str) -> Error {
    Error::with_message_and_status(format!("option {name:?} not set"), Status::NotFound)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use adbc_core::error::Status;
    use adbc_core::options::{IngestMode, OptionStatement, OptionValue};
    use adbc_core::{Optionable, Statement};
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};

    use super::{QuestDbStatement, TIMESTAMP_COLUMN_OPTION};
    use crate::qwp_connection::QwpConnection;

    fn statement() -> QuestDbStatement {
        QuestDbStatement::new(Arc::new(QwpConnection::new(
            "ws::addr=127.0.0.1:1;".to_owned(),
        )))
    }

    fn batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let values: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
        RecordBatch::try_new(schema, vec![values]).expect("record batch")
    }

    fn other_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let values: ArrayRef = Arc::new(arrow_array::StringArray::from(vec!["a"]));
        RecordBatch::try_new(schema, vec![values]).expect("record batch")
    }

    fn stream(batch: RecordBatch) -> Box<dyn arrow_array::RecordBatchReader + Send> {
        let schema = batch.schema();
        Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema))
    }

    #[test]
    fn ingest_options_parse_and_roundtrip() {
        let mut statement = statement();
        statement
            .set_option(OptionStatement::TargetTable, OptionValue::from("trades"))
            .unwrap();
        statement
            .set_option(OptionStatement::TargetDbSchema, OptionValue::from("public"))
            .unwrap();
        statement
            .set_option(
                OptionStatement::IngestMode,
                OptionValue::from(IngestMode::Append),
            )
            .unwrap();
        statement
            .set_option(timestamp_option(), OptionValue::from("event_time"))
            .unwrap();

        assert_eq!(
            statement
                .get_option_string(OptionStatement::TargetTable)
                .unwrap(),
            "trades"
        );
        assert_eq!(
            statement
                .get_option_string(OptionStatement::TargetDbSchema)
                .unwrap(),
            "public"
        );
        assert_eq!(
            statement
                .get_option_string(OptionStatement::IngestMode)
                .unwrap(),
            String::from(IngestMode::Append)
        );
        assert_eq!(
            statement.get_option_string(timestamp_option()).unwrap(),
            "event_time"
        );
    }

    fn timestamp_option() -> OptionStatement {
        OptionStatement::Other(TIMESTAMP_COLUMN_OPTION.to_owned())
    }

    #[test]
    fn timestamp_column_override_rejects_empty_values() {
        let mut statement = statement();
        let error = statement
            .set_option(timestamp_option(), OptionValue::from(""))
            .expect_err("empty timestamp column must fail");
        assert_eq!(error.status, Status::InvalidArguments);
    }

    #[test]
    fn timestamp_column_override_is_rejected_on_a_sql_statement() {
        let mut statement = statement();
        statement.set_sql_query("SELECT 1").unwrap();
        let error = statement
            .set_option(timestamp_option(), OptionValue::from("ts"))
            .expect_err("SQL statement must reject timestamp override");
        assert_eq!(error.status, Status::InvalidState);
    }

    #[test]
    fn ingest_mode_rejects_unknown_values() {
        let mut statement = statement();
        let error = statement
            .set_option(OptionStatement::IngestMode, OptionValue::from("not-a-mode"))
            .expect_err("unknown mode must fail");
        assert_eq!(error.status, Status::InvalidArguments);
    }

    #[test]
    fn temporary_and_incremental_are_not_implemented() {
        let mut statement = statement();
        for option in [OptionStatement::Temporary, OptionStatement::Incremental] {
            let error = statement
                .set_option(option.clone(), OptionValue::from("false"))
                .expect_err("unsupported option must fail");
            assert_eq!(error.status, Status::NotImplemented);
            assert_eq!(
                statement
                    .get_option_string(option)
                    .expect_err("readback must fail")
                    .status,
                Status::NotImplemented
            );
        }
    }

    #[test]
    fn ingest_options_are_rejected_on_a_sql_statement() {
        let mut statement = statement();
        statement.set_sql_query("SELECT 1").unwrap();
        let error = statement
            .set_option(OptionStatement::TargetTable, OptionValue::from("t"))
            .expect_err("SQL statement must reject ingest options");
        assert_eq!(error.status, Status::InvalidState);
    }

    #[test]
    fn bind_creates_bulk_ingest_and_appends_matching_schemas() {
        let mut statement = statement();
        statement.bind(batch(&[1, 2])).unwrap();
        statement.bind(batch(&[3])).unwrap();
        assert!(matches!(
            statement.mode,
            super::StatementMode::BulkIngest(_)
        ));
        match &statement.bound {
            super::BoundData::Batches { batches, .. } => assert_eq!(batches.len(), 2),
            _ => panic!("expected bound batches"),
        }
    }

    #[test]
    fn repeated_bind_requires_identical_schemas() {
        let mut statement = statement();
        statement.bind(batch(&[1])).unwrap();
        let error = statement
            .bind(other_batch())
            .expect_err("schema change must fail");
        assert_eq!(error.status, Status::InvalidArguments);
    }

    #[test]
    fn bind_and_bind_stream_cannot_be_mixed() {
        let mut bound_statement = statement();
        bound_statement.bind(batch(&[1])).unwrap();
        let error = bound_statement
            .bind_stream(stream(batch(&[2])))
            .expect_err("mixing bind and bind_stream must fail");
        assert_eq!(error.status, Status::InvalidState);

        let mut stream_statement = statement();
        stream_statement.bind_stream(stream(batch(&[1]))).unwrap();
        let error = stream_statement
            .bind(batch(&[2]))
            .expect_err("mixing bind_stream and bind must fail");
        assert_eq!(error.status, Status::InvalidState);
    }

    #[test]
    fn set_sql_query_clears_bound_data() {
        let mut statement = statement();
        statement.bind(batch(&[1])).unwrap();
        statement.set_sql_query("CREATE TABLE x").unwrap();
        assert!(matches!(statement.bound, super::BoundData::None));
        let error = statement
            .bind(other_batch())
            .expect_err("SQL statements cannot accept bound data");
        assert_eq!(error.status, Status::InvalidState);
    }

    #[test]
    fn execute_rejects_bulk_ingest_statements() {
        let mut statement = statement();
        statement.bind(batch(&[1])).unwrap();
        let error = match statement.execute() {
            Ok(_) => panic!("bulk ingest requires execute_update"),
            Err(error) => error,
        };
        assert_eq!(error.status, Status::InvalidState);
    }

    #[test]
    fn execute_update_requires_a_target_table() {
        let mut statement = statement();
        statement.bind(batch(&[1])).unwrap();
        let error = statement
            .execute_update()
            .expect_err("missing target table must fail");
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(matches!(statement.bound, super::BoundData::None));
    }

    #[test]
    fn execute_update_on_unset_statement_is_invalid_state() {
        let mut statement = statement();
        let error = statement
            .execute_update()
            .expect_err("unset statement must fail");
        assert_eq!(error.status, Status::InvalidState);
    }
}
