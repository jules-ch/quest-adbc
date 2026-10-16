use std::ops::DerefMut;
use std::sync::Arc;

use adbc_core::error::{Error as AdbcError, Status};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use questdb::egress::Cursor;
use self_cell::{MutBorrow, self_cell};

use crate::cancellation::CancellationToken;
use crate::connection::ReaderGuard;

// A QWP Cursor mutably borrows its Reader for the whole query, like the
// PostgreSQL ADBC driver's TupleReader holds its borrowed PGconn. Keeping
// the cursor and its checkout-guard in one owned, movable cell lets a
// `Send + 'static` result drive both directly, with no worker thread.
self_cell!(
    struct ActiveQuery {
        owner: MutBorrow<ReaderGuard>,

        #[not_covariant]
        dependent: Cursor,
    }
);
/// Arrow result for one query. The reader holds the checkout guard while its
/// query is in flight and pulls batches directly on `next()`; when finished
/// (or dropped), the guard returns the native connection for reuse. The
/// checked-out query is registered with the native connection, so one flag
/// serves both statement-level and connection-level cancellation.
pub(crate) struct QuestDbArrowStreamReader {
    active: Option<ActiveQuery>,
    schema: SchemaRef,
    first_batch: Option<RecordBatch>,
    cancellation: Arc<CancellationToken>,
    finished: bool,
}

impl QuestDbArrowStreamReader {
    pub(crate) fn execute(
        guard: ReaderGuard,
        sql: String,
        cancellation: Arc<CancellationToken>,
    ) -> adbc_core::error::Result<Self> {
        let mut active = ActiveQuery::try_new(MutBorrow::new(guard), |owner| {
            owner.borrow_mut().deref_mut().execute(sql)
        })
        .map_err(io_error)?;
        let first_batch = active
            .with_dependent_mut(|_, cursor| cursor.next_arrow_batch())
            .map_err(io_error)?;

        match first_batch {
            Some(first_batch) => Ok(Self {
                active: Some(active),
                schema: first_batch.schema(),
                first_batch: Some(first_batch),
                cancellation,
                finished: false,
            }),
            None => {
                // A query may end without any result batch — e.g. DML/DDL
                // run through execute(). Such a cursor becomes an empty
                // (zero-column) result instead of an error, like the
                // ClickHouse driver handles schema-less cursor ends
                // (adbc_clickhouse issue 49). The guard drops here and the
                // reader goes straight back to its slot.
                Ok(Self {
                    active: None,
                    schema: Arc::new(Schema::empty()),
                    first_batch: None,
                    cancellation,
                    finished: true,
                })
            }
        }
    }
}

fn io_error(error: impl std::fmt::Display) -> AdbcError {
    AdbcError::with_message_and_status(error.to_string(), Status::IO)
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
            let active = self.active.take();
            if let Some(mut active) = active {
                let _ = active.with_dependent_mut(|_, cursor| cursor.cancel());
                self.finished = true;
                return Some(Err(ArrowError::ExternalError(Box::new(
                    AdbcError::with_message_and_status(
                        "QuestDB query cancelled",
                        Status::Cancelled,
                    ),
                ))));
            }
        }

        if let Some(batch) = self.first_batch.take() {
            return Some(Ok(batch));
        }

        let outcome = self
            .active
            .as_mut()
            .map(|active| active.with_dependent_mut(|_, cursor| cursor.next_arrow_batch()))
            .unwrap_or_else(|| Ok(None));
        match outcome {
            Ok(Some(batch)) if batch.schema() == self.schema => Some(Ok(batch)),
            Ok(Some(_)) => {
                self.finished = true;
                self.active = None;
                Some(Err(ArrowError::SchemaError(
                    "QuestDB result schema changed during streaming".into(),
                )))
            }
            Ok(None) => {
                self.finished = true;
                self.active = None;
                None
            }
            Err(error) => {
                self.finished = true;
                self.active = None;
                Some(Err(ArrowError::ExternalError(Box::new(io_error(error)))))
            }
        }
    }
}

impl RecordBatchReader for QuestDbArrowStreamReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
