//! Translation of QuestDB client and Arrow failures into ADBC errors.
//!
//! `questdb-rs` classifies failures with its own [`ErrorCode`] taxonomy. The
//! mapping below preserves the categories ADBC callers act on (cancellation,
//! bad SQL, authentication, transport) instead of flattening everything into
//! `Status::IO`.

use adbc_core::error::{Error, Status};
use arrow_schema::ArrowError;
use questdb::ErrorCode;

pub(crate) fn from_questdb(error: questdb::Error) -> Error {
    Error::with_message_and_status(error.to_string(), status_for(error.code()))
}

pub(crate) fn arrow(error: ArrowError) -> Error {
    Error::with_message_and_status(error.to_string(), Status::Internal)
}

fn status_for(code: ErrorCode) -> Status {
    match code {
        ErrorCode::AuthError => Status::Unauthenticated,
        ErrorCode::ServerSecurityError => Status::Unauthorized,

        ErrorCode::InvalidApiCall | ErrorCode::FailoverWouldDuplicate => Status::InvalidState,
        ErrorCode::InvalidBind
        | ErrorCode::InvalidName
        | ErrorCode::InvalidUtf8
        | ErrorCode::InvalidTimestamp
        | ErrorCode::InvalidDecimal
        | ErrorCode::ConfigError
        | ErrorCode::ServerParseError
        | ErrorCode::ServerSchemaMismatch => Status::InvalidArguments,

        ErrorCode::ServerInternalError => Status::Internal,
        ErrorCode::Cancelled => Status::Cancelled,
        ErrorCode::ConnectTimeout => Status::Timeout,

        ErrorCode::LimitExceeded
        | ErrorCode::ServerLimitExceeded
        | ErrorCode::BatchTooLarge
        | ErrorCode::SchemaDrift
        | ErrorCode::NoSchema
        | ErrorCode::ArrowIngest
        | ErrorCode::ArrowUnsupportedColumnKind
        | ErrorCode::ArrayError => Status::InvalidData,
        ErrorCode::ArrowExport | ErrorCode::SymbolDictFull | ErrorCode::StoreResendRequired => {
            Status::Internal
        }

        ErrorCode::CouldNotResolveAddr
        | ErrorCode::SocketError
        | ErrorCode::HandshakeError
        | ErrorCode::TlsError
        | ErrorCode::UnsupportedServer
        | ErrorCode::ProtocolError
        | ErrorCode::ProtocolVersionError
        | ErrorCode::ServerRejection
        | ErrorCode::FailoverRetry
        | ErrorCode::RoleMismatch => Status::IO,

        _ => Status::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::status_for;
    use adbc_core::error::Status;
    use questdb::ErrorCode;

    #[test]
    fn maps_cancellation_and_bad_sql() {
        assert_eq!(status_for(ErrorCode::Cancelled), Status::Cancelled);
        assert_eq!(
            status_for(ErrorCode::ServerParseError),
            Status::InvalidArguments
        );
        assert_eq!(status_for(ErrorCode::SocketError), Status::IO);
    }
}
