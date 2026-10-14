//! ADBC schema helpers.
//!
//! Query schemas are currently provided directly by QuestDB's QWP Arrow
//! decoder, avoiding a second type-mapping layer in the driver.

use arrow_schema::Schema;

pub(crate) fn unsupported_schema() -> adbc_core::error::Result<Schema> {
    Err(adbc_core::error::Error::with_message_and_status(
        "QuestDB table metadata is not implemented yet",
        adbc_core::error::Status::NotImplemented,
    ))
}
