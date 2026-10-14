use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread;

use adbc_core::error::{Error as AdbcError, Status};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use questdb::egress::Reader;

/// Owned streaming adapter for the QuestDB cursor API, whose cursor borrows its
/// reader. A dedicated worker owns both QWP objects; a one-batch channel keeps
/// memory bounded and transfers backpressure to the QWP reader.
pub(crate) struct QuestDbBatchReader {
    schema: SchemaRef,
    first_batch: Option<RecordBatch>,
    receiver: Receiver<StreamMessage>,
    finished: bool,
}

enum StreamMessage {
    Batch(RecordBatch),
    Error(AdbcError),
}

impl QuestDbBatchReader {
    pub(crate) fn execute(conf: String, sql: String) -> adbc_core::error::Result<Self> {
        let (sender, receiver) = mpsc::sync_channel(1);
        let (first_sender, first_receiver) = mpsc::sync_channel(1);

        thread::Builder::new()
            .name("questdb-adbc-qwp-reader".into())
            .spawn(move || run_query(conf, sql, sender, first_sender))
            .map_err(|error| {
                AdbcError::with_message_and_status(
                    format!("could not start QuestDB query reader: {error}"),
                    Status::Internal,
                )
            })?;

        // QWP carries the result schema on its initial result batch. Waiting
        // for that batch makes the ADBC schema available before execute returns.
        let first_batch = first_receiver.recv().map_err(|_| {
            AdbcError::with_message_and_status(
                "QuestDB query reader stopped before returning a schema",
                Status::IO,
            )
        })??;
        let schema = first_batch.schema();

        Ok(Self {
            schema,
            first_batch: Some(first_batch),
            receiver,
            finished: false,
        })
    }
}

fn run_query(
    conf: String,
    sql: String,
    sender: SyncSender<StreamMessage>,
    first_sender: SyncSender<adbc_core::error::Result<RecordBatch>>,
) {
    let mut reader = match Reader::from_conf(&conf) {
        Ok(reader) => reader,
        Err(error) => {
            let _ = first_sender.send(Err(map_questdb_error(error.to_string())));
            return;
        }
    };

    let mut cursor = match reader.execute(sql) {
        Ok(cursor) => cursor,
        Err(error) => {
            let _ = first_sender.send(Err(map_questdb_error(error.to_string())));
            return;
        }
    };

    let first_batch = match cursor.next_arrow_batch() {
        Ok(Some(batch)) => batch,
        Ok(None) => {
            let _ = first_sender.send(Err(AdbcError::with_message_and_status(
                "QuestDB ended the query before returning an Arrow schema",
                Status::InvalidData,
            )));
            return;
        }
        Err(error) => {
            let _ = first_sender.send(Err(map_questdb_error(error.to_string())));
            return;
        }
    };

    if first_sender.send(Ok(first_batch)).is_err() {
        return;
    }

    loop {
        match cursor.next_arrow_batch() {
            Ok(Some(batch)) => {
                if sender.send(StreamMessage::Batch(batch)).is_err() {
                    return;
                }
            }
            Ok(None) => return,
            Err(error) => {
                let _ = sender.send(StreamMessage::Error(map_questdb_error(error.to_string())));
                return;
            }
        }
    }
}

fn map_questdb_error(message: String) -> AdbcError {
    AdbcError::with_message_and_status(message, Status::IO)
}

impl Iterator for QuestDbBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        if let Some(batch) = self.first_batch.take() {
            return Some(Ok(batch));
        }

        match self.receiver.recv() {
            Ok(StreamMessage::Batch(batch)) => Some(Ok(batch)),
            Ok(StreamMessage::Error(error)) => {
                self.finished = true;
                Some(Err(ArrowError::ExternalError(Box::new(error))))
            }
            Err(_) => {
                self.finished = true;
                None
            }
        }
    }
}

impl RecordBatchReader for QuestDbBatchReader {
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
