//! Write operations are intentionally deferred from the read-focused MVP.

use adbc_core::error::{Error, Status};

pub(crate) fn not_implemented<T>() -> adbc_core::error::Result<T> {
    Err(Error::with_message_and_status(
        "QuestDB ADBC ingestion and parameter binding are not implemented yet",
        Status::NotImplemented,
    ))
}
