//! Bulk ingestion over the QWP write pool.

use std::sync::Arc;

use adbc_core::error::{Error, Status};
use adbc_core::options::IngestMode;
use arrow_array::types::UInt32Type;
use arrow_array::{Array, DictionaryArray, RecordBatch, RecordBatchReader, StringArray};
use arrow_schema::{DataType, Schema, SchemaRef};
use questdb::ErrorCode;
use questdb::ingress::ColumnName;

use crate::error;
use crate::qwp_connection::QwpConnection;
use crate::statement::{BoundData, IngestOptions, TIMESTAMP_COLUMN_OPTION};

struct TableMetadata {
    designated_timestamp: Option<String>,
}

/// Flush the statement's bound data according to its ingest mode.
pub(crate) fn ingest(
    connection: &Arc<QwpConnection>,
    options: &IngestOptions,
    target: &str,
    bound: BoundData,
) -> adbc_core::error::Result<Option<i64>> {
    if target.is_empty() {
        return Err(Error::with_message_and_status(
            "bulk ingest target table must not be empty",
            Status::InvalidArguments,
        ));
    }
    match options.target_schema.as_deref() {
        None | Some("") | Some("public") => {}
        Some(_) => {
            return Err(Error::with_message_and_status(
                "QuestDB schema-qualified tables are not implemented; only the public schema is supported",
                Status::NotImplemented,
            ));
        }
    }

    let (schema, source) = bind_source(bound)?;

    let metadata = match options.mode {
        IngestMode::Create => {
            if table_metadata(connection, target)?.is_some() {
                return Err(Error::with_message_and_status(
                    format!("table {target:?} already exists"),
                    Status::AlreadyExists,
                ));
            }
            None
        }
        IngestMode::Append => Some(table_metadata(connection, target)?.ok_or_else(|| {
            Error::with_message_and_status(
                format!("table {target:?} does not exist"),
                Status::NotFound,
            )
        })?),
        IngestMode::CreateAppend => None,
        IngestMode::Replace => {
            let sql = format!("DROP TABLE IF EXISTS \"{}\"", escape_identifier(target));
            connection.query_batches(&sql)?;
            None
        }
    };

    let designated = metadata
        .as_ref()
        .and_then(|metadata| metadata.designated_timestamp.as_deref());
    let timestamp = match options.timestamp_column.as_deref() {
        Some(column) => {
            if !is_timestamp(&schema, column) {
                return Err(Error::with_message_and_status(
                    format!(
                        "{TIMESTAMP_COLUMN_OPTION} must name a timestamp column present in the batch; got {column:?}"
                    ),
                    Status::InvalidArguments,
                ));
            }
            Some(column.to_owned())
        }
        None => resolve_timestamp(&schema, designated),
    };
    let timestamp_column = timestamp
        .as_deref()
        .map(ColumnName::new)
        .transpose()
        .map_err(error::from_questdb)?;

    let pool = connection.pool()?;
    let mut total = 0i64;
    let mut flushed = 0usize;
    match source {
        BoundSource::Batches(batches) => {
            for batch in batches {
                flush_batch(&pool, options.mode, target, timestamp_column, &batch)?;
                total = total.saturating_add(row_count(&batch));
                flushed += 1;
            }
        }
        BoundSource::Stream(reader) => {
            for batch in reader {
                let batch = batch.map_err(error::arrow)?;
                flush_batch(&pool, options.mode, target, timestamp_column, &batch)?;
                total = total.saturating_add(row_count(&batch));
                flushed += 1;
            }
        }
    }

    // An empty stream can still create the table. Send an empty batch with the
    // reader's schema when the mode may create one; plain append must not
    // invent a flush.
    if flushed == 0
        && matches!(
            options.mode,
            IngestMode::Create | IngestMode::CreateAppend | IngestMode::Replace
        )
    {
        let empty = RecordBatch::new_empty(Arc::clone(&schema));
        flush_batch(&pool, options.mode, target, timestamp_column, &empty)?;
    }
    Ok(Some(total))
}

enum BoundSource {
    Batches(std::vec::IntoIter<RecordBatch>),
    Stream(Box<dyn RecordBatchReader + Send>),
}

fn bind_source(bound: BoundData) -> adbc_core::error::Result<(SchemaRef, BoundSource)> {
    match bound {
        BoundData::None => Err(Error::with_message_and_status(
            "no data has been bound for ingestion",
            Status::InvalidArguments,
        )),
        BoundData::Batches { schema, batches } => {
            Ok((schema, BoundSource::Batches(batches.into_iter())))
        }
        BoundData::Stream { schema, reader } => Ok((schema, BoundSource::Stream(reader))),
    }
}

fn row_count(batch: &RecordBatch) -> i64 {
    i64::try_from(batch.num_rows()).unwrap_or(i64::MAX)
}

fn flush_batch(
    pool: &questdb::QuestDb,
    mode: IngestMode,
    target: &str,
    timestamp_column: Option<ColumnName<'_>>,
    batch: &RecordBatch,
) -> adbc_core::error::Result<()> {
    pool.flush_arrow_batch(target, batch, timestamp_column, &[], None)
        .map_err(|failure| translate_flush_error(mode, failure))
}

fn table_metadata(
    connection: &Arc<QwpConnection>,
    table: &str,
) -> adbc_core::error::Result<Option<TableMetadata>> {
    let sql = format!(
        "SELECT table_name, designatedTimestamp FROM tables() WHERE table_name = '{}'",
        escape_literal(table)
    );
    let batches = connection.query_batches(&sql)?;
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        return Ok(Some(TableMetadata {
            designated_timestamp: string_cell(&batch, 1, 0),
        }));
    }
    Ok(None)
}

/// Read a VARCHAR/SYMBOL cell as UTF-8. SYMBOL columns arrive as
/// `Dictionary(UInt32, Utf8)`, hence the dictionary path.
fn string_cell(batch: &RecordBatch, column: usize, row: usize) -> Option<String> {
    let array = batch.column(column);
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return (!values.is_null(row)).then(|| values.value(row).to_owned());
    }
    let dictionary = array
        .as_any()
        .downcast_ref::<DictionaryArray<UInt32Type>>()?;
    if dictionary.is_null(row) {
        return None;
    }
    let values = dictionary.values().as_any().downcast_ref::<StringArray>()?;
    Some(
        values
            .value(dictionary.keys().value(row) as usize)
            .to_owned(),
    )
}

fn translate_flush_error(mode: IngestMode, failure: questdb::Error) -> Error {
    if matches!(mode, IngestMode::Append | IngestMode::CreateAppend) && is_schema_mismatch(&failure)
    {
        Error::with_message_and_status(failure.to_string(), Status::AlreadyExists)
    } else {
        error::from_questdb(failure)
    }
}

/// QWP ingress reports a server `SCHEMA_MISMATCH` (`0x03`) as
/// `InvalidApiCall`, unlike the egress path's `ServerSchemaMismatch`. The
/// ingress error keeps the raw status in its message ("... status=0x03 ..."),
/// which is the only discriminator it exposes, since parse errors share the
/// same `InvalidApiCall` code.
fn is_schema_mismatch(failure: &questdb::Error) -> bool {
    failure.code() == ErrorCode::ServerSchemaMismatch
        || (failure.code() == ErrorCode::InvalidApiCall
            && (failure.msg().contains("schema mismatch") || failure.msg().contains("status=0x03")))
}

/// Pick the designated timestamp column for an ingest batch.
///
/// 1. The existing table's designated timestamp, if the batch carries it.
/// 2. A `Timestamp`-typed column named exactly `timestamp`.
/// 3. The batch's only `Timestamp`-typed column.
/// 4. `None`, letting the server stamp arrival time.
pub(crate) fn resolve_timestamp(schema: &Schema, designated: Option<&str>) -> Option<String> {
    if let Some(name) = designated
        && is_timestamp(schema, name)
    {
        return Some(name.to_owned());
    }
    if is_timestamp(schema, "timestamp") {
        return Some("timestamp".to_owned());
    }
    let mut timestamps = schema
        .fields()
        .iter()
        .filter(|field| field.data_type().is_timestamp())
        .map(|field| field.name().clone());
    let only = timestamps.next()?;
    if timestamps.next().is_none() {
        Some(only)
    } else {
        None
    }
}

fn is_timestamp(schema: &Schema, name: &str) -> bool {
    schema
        .field_with_name(name)
        .is_ok_and(|field| field.data_type().is_timestamp())
}

trait IsTimestamp {
    fn is_timestamp(&self) -> bool;
}

impl IsTimestamp for DataType {
    fn is_timestamp(&self) -> bool {
        matches!(self, DataType::Timestamp(_, _))
    }
}

pub(crate) fn escape_literal(value: &str) -> String {
    value.replace('\'', "''")
}

pub(crate) fn escape_identifier(value: &str) -> String {
    value.replace('"', "\"\"")
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::{escape_identifier, escape_literal, resolve_timestamp};

    fn schema(fields: Vec<Field>) -> Schema {
        Schema::new(fields)
    }

    fn timestamp(name: &str) -> Field {
        Field::new(
            name,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )
    }

    #[test]
    fn escapes_sql_literals_and_identifiers() {
        assert_eq!(escape_literal("it's"), "it''s");
        assert_eq!(escape_identifier("a\"b"), "a\"\"b");
    }

    #[test]
    fn prefers_existing_designated_timestamp() {
        let schema = schema(vec![
            timestamp("event_time"),
            timestamp("created_at"),
            Field::new("x", DataType::Int64, false),
        ]);
        assert_eq!(
            resolve_timestamp(&schema, Some("event_time")).as_deref(),
            Some("event_time")
        );
    }

    #[test]
    fn falls_back_from_missing_designated_timestamp() {
        let schema = schema(vec![timestamp("event_time"), timestamp("created_at")]);
        assert_eq!(resolve_timestamp(&schema, Some("ts")), None);
    }

    #[test]
    fn prefers_column_named_timestamp() {
        let schema = schema(vec![timestamp("event_time"), timestamp("timestamp")]);
        assert_eq!(
            resolve_timestamp(&schema, None).as_deref(),
            Some("timestamp")
        );
    }

    #[test]
    fn uses_the_only_timestamp_column() {
        let schema = schema(vec![
            timestamp("event_time"),
            Field::new("x", DataType::Int64, false),
        ]);
        assert_eq!(
            resolve_timestamp(&schema, None).as_deref(),
            Some("event_time")
        );
    }

    #[test]
    fn leaves_ambiguous_or_missing_timestamps_to_the_server() {
        let ambiguous = schema(vec![timestamp("a"), timestamp("b")]);
        assert_eq!(resolve_timestamp(&ambiguous, None), None);
        let none = schema(vec![Field::new("x", DataType::Int64, false)]);
        assert_eq!(resolve_timestamp(&none, None), None);
    }
}
