use std::sync::Arc;

use adbc_core::error::{Error as AdbcError, Status};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Schema, SchemaRef};

use crate::arrow_cursor::ArrowCursor;
use crate::cancellation::CancellationToken;

/// Arrow result for one query. The cursor holds the checkout guard while its
/// query is in flight and pulls batches directly on `next()`; when finished
/// (or dropped), the guard returns the native connection for reuse. The
/// checked-out query is registered with the native connection, so one flag
/// serves both statement-level and connection-level cancellation.
pub(crate) struct QuestDbArrowStreamReader {
    cursor: Option<ArrowCursor>,
    schema: SchemaRef,
    first_batch: Option<RecordBatch>,
    cancellation: Arc<CancellationToken>,
    finished: bool,
}

impl QuestDbArrowStreamReader {
    pub(crate) fn begin(
        mut cursor: ArrowCursor,
        cancellation: Arc<CancellationToken>,
    ) -> adbc_core::error::Result<Self> {
        // Read the first batch so that `RecordBatchReader::schema()` is
        // infallible, including the empty DDL/DML result case.
        let first_batch = cursor.next_batch()?;

        let Some(first_batch) = first_batch else {
            // DML/DDL may complete without producing a result batch.
            // Drop the query and return an empty result.
            return Ok(Self {
                cursor: None,
                schema: Arc::new(Schema::empty()),
                first_batch: None,
                cancellation,
                finished: true,
            });
        };

        let schema = first_batch.schema();

        Ok(Self {
            cursor: Some(cursor),
            schema,
            first_batch: Some(first_batch),
            cancellation,
            finished: false,
        })
    }
}

fn cancellation_error() -> ArrowError {
    ArrowError::ExternalError(Box::new(AdbcError::with_message_and_status(
        "QuestDB query cancelled",
        Status::Cancelled,
    )))
}

impl Iterator for QuestDbArrowStreamReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        // Cancellation plays the role of the PG driver's PGcancel: end the
        // query server-side through the cursor's CANCEL frame and bounded
        // drain, which keeps the underlying connection reusable. A request
        // is only recognised here, between batches — never while another
        // thread is blocked reading the socket.
        if self.cancellation.is_cancelled() {
            let cursor = self.cursor.take();
            if let Some(mut cursor) = cursor {
                let _ = cursor.cancel();
                self.finished = true;
                return Some(Err(cancellation_error()));
            }
        }

        if let Some(batch) = self.first_batch.take() {
            return Some(Ok(batch));
        }

        let outcome = self
            .cursor
            .as_mut()
            .map(ArrowCursor::next_batch)
            .unwrap_or_else(|| Ok(None));
        match outcome {
            Ok(Some(batch)) if batch.schema() == self.schema => Some(Ok(batch)),
            Ok(Some(_)) => {
                self.finished = true;
                self.cursor = None;
                Some(Err(ArrowError::SchemaError(
                    "QuestDB result schema changed during streaming".into(),
                )))
            }
            Ok(None) => {
                self.finished = true;
                self.cursor = None;
                None
            }
            Err(failure) => {
                self.finished = true;
                self.cursor = None;
                Some(Err(ArrowError::ExternalError(Box::new(failure))))
            }
        }
    }
}

impl RecordBatchReader for QuestDbArrowStreamReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Drop for QuestDbArrowStreamReader {
    fn drop(&mut self) {
        // Dropping a partially consumed result is common in ADBC (e.g. a
        // caller stops after LIMIT or an error). Send CANCEL and drain
        // server-side so the native Reader stays reusable; without this the
        // cursor's own Drop would tear down the WebSocket and force the next
        // query to reconnect.
        if let Some(mut cursor) = self.cursor.take() {
            let _ = cursor.cancel();
        }
    }
}

pub(crate) fn build_conf(
    uri: Option<&str>,
    username: Option<&str>,
    password: Option<&str>,
) -> adbc_core::error::Result<String> {
    let uri = uri.unwrap_or("ws://localhost:9000");
    let parsed = url::Url::parse(uri).map_err(|error| {
        AdbcError::with_message_and_status(
            format!("invalid QuestDB URI: {error}"),
            Status::InvalidArguments,
        )
    })?;

    let scheme = match parsed.scheme() {
        "ws" => "ws",
        "wss" => "wss",
        scheme => {
            return Err(AdbcError::with_message_and_status(
                format!("unsupported QuestDB URI scheme {scheme:?}; expected ws or wss"),
                Status::InvalidArguments,
            ));
        }
    };
    let host = parsed.host().ok_or_else(|| {
        AdbcError::with_message_and_status(
            "QuestDB URI must include a host",
            Status::InvalidArguments,
        )
    })?;
    let port = parsed.port().unwrap_or(9000);

    // `url::Host`'s `Display` impl brackets IPv6 addresses; domains/IPv4 are bare.
    let mut conf = format!("{scheme}::addr={host}:{port};");
    if let Some(username) = username {
        conf.push_str(&format!("username={};", conf_value(username)));
    }
    if let Some(password) = password {
        conf.push_str(&format!("password={};", conf_value(password)));
    }
    Ok(conf)
}

fn conf_value(value: &str) -> String {
    value.replace(';', ";;")
}

#[cfg(test)]
mod tests {
    use super::build_conf;

    #[test]
    fn builds_plain_qwp_configuration() {
        assert_eq!(
            build_conf(Some("ws://localhost:9010"), None, None).unwrap(),
            "ws::addr=localhost:9010;"
        );
    }

    #[test]
    fn builds_tls_qwp_configuration_and_escapes_credentials() {
        assert_eq!(
            build_conf(Some("wss://[::1]"), Some("user"), Some("a;b")).unwrap(),
            "wss::addr=[::1]:9000;username=user;password=a;;b;"
        );
    }

    #[test]
    fn builds_ipv4_qwp_configuration() {
        assert_eq!(
            build_conf(Some("ws://127.0.0.1:9010"), None, None).unwrap(),
            "ws::addr=127.0.0.1:9010;"
        );
    }

    #[test]
    fn rejects_non_websocket_uri() {
        assert!(build_conf(Some("http://localhost:9000"), None, None).is_err());
    }
}
