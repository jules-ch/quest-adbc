use adbc_core::error::{Error, Status};
use adbc_core::options::{OptionValue, OptionValue as Value};

pub(crate) fn unsupported(option: &str) -> Error {
    Error::with_message_and_status(
        format!("QuestDB ADBC option {option:?} is not implemented"),
        Status::NotImplemented,
    )
}

pub(crate) fn option_string(value: OptionValue, option: &str) -> adbc_core::error::Result<String> {
    match value {
        Value::String(value) => Ok(value),
        other => Err(Error::with_message_and_status(
            format!("option {option:?} requires a string, got {other:?}"),
            Status::InvalidArguments,
        )),
    }
}

pub(crate) fn read_only_error() -> Error {
    Error::with_message_and_status(
        "QuestDB ADBC runs in autocommit mode; explicit transactions are not implemented",
        Status::NotImplemented,
    )
}
