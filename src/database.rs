use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::error::{Error, Status};
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::{Database, Optionable};

use crate::connection::QuestDbConnection;
use crate::qwp_connection::QwpConnection;
use crate::{options, reader};

#[derive(Default)]
pub struct QuestDbDatabase {
    uri: Option<String>,
    username: Option<String>,
    password: Option<String>,
    options: HashMap<String, String>,
}

impl Database for QuestDbDatabase {
    type ConnectionType = QuestDbConnection;

    fn new_connection(&self) -> adbc_core::error::Result<Self::ConnectionType> {
        self.new_connection_with_opts([])
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> adbc_core::error::Result<Self::ConnectionType> {
        let conf = reader::build_conf(
            self.uri.as_deref(),
            self.username.as_deref(),
            self.password.as_deref(),
        )?;
        let mut connection = QuestDbConnection::new(Arc::new(QwpConnection::new(conf)));
        for (key, value) in opts {
            connection.set_option(key, value)?;
        }
        Ok(connection)
    }
}

impl Optionable for QuestDbDatabase {
    type Option = OptionDatabase;

    fn set_option(
        &mut self,
        key: Self::Option,
        value: OptionValue,
    ) -> adbc_core::error::Result<()> {
        let option_name = key.as_ref().to_owned();
        match key {
            OptionDatabase::Uri => {
                let uri = options::option_string(value, key.as_ref())?;
                // Validate URI eagerly, before the connection is opened.
                reader::build_conf(Some(&uri), None, None)?;
                self.uri = Some(uri);
                Ok(())
            }
            OptionDatabase::Username => {
                self.username = Some(options::option_string(value, key.as_ref())?);
                Ok(())
            }
            OptionDatabase::Password => {
                self.password = Some(options::option_string(value, key.as_ref())?);
                Ok(())
            }
            OptionDatabase::Other(name) => {
                self.options
                    .insert(name, options::option_string(value, &option_name)?);
                Ok(())
            }
            _ => Err(options::unsupported(key.as_ref())),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
        match key {
            OptionDatabase::Uri | OptionDatabase::Username | OptionDatabase::Password => {
                Err(Error::with_message_and_status(
                    "readback of database URI and credentials is omitted",
                    Status::NotFound,
                ))
            }
            OptionDatabase::Other(name) => self.options.get(&name).cloned().ok_or_else(|| {
                Error::with_message_and_status(format!("option {name:?} not set"), Status::NotFound)
            }),
            _ => Err(options::unsupported(key.as_ref())),
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

#[cfg(test)]
mod tests {
    use adbc_core::error::Status;
    use adbc_core::options::{OptionDatabase, OptionValue};
    use adbc_core::{Database, Optionable};

    use super::QuestDbDatabase;

    #[test]
    fn test_set_uri_option() {
        let mut db = QuestDbDatabase::default();
        let result = db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("ws://localhost:9000".to_string()),
        );
        assert!(result.is_ok());
        assert_eq!(db.uri, Some("ws://localhost:9000".to_string()));
    }

    #[test]
    fn test_get_uri_option_not_set() {
        let db = QuestDbDatabase::default();
        let result = db.get_option_string(OptionDatabase::Uri);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::NotFound);
    }

    #[test]
    fn test_new_connection_with_uri() {
        let mut db = QuestDbDatabase::default();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("ws://localhost:9000".to_string()),
        )
        .unwrap();
        assert!(db.new_connection().is_ok());
    }

    #[test]
    fn test_set_username_option() {
        let mut db = QuestDbDatabase::default();
        let result = db.set_option(
            OptionDatabase::Username,
            OptionValue::String("admin".to_string()),
        );
        assert!(result.is_ok());
        assert_eq!(db.username.as_deref(), Some("admin"));
    }

    #[test]
    fn test_set_password_option() {
        let mut db = QuestDbDatabase::default();
        let result = db.set_option(
            OptionDatabase::Password,
            OptionValue::String("secret".to_string()),
        );
        assert!(result.is_ok());
        assert_eq!(db.password.as_deref(), Some("secret"));
    }

    #[test]
    fn test_get_username_not_set() {
        let db = QuestDbDatabase::default();
        let result = db.get_option_string(OptionDatabase::Username);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::NotFound);
    }

    #[test]
    fn test_get_password_not_set() {
        let db = QuestDbDatabase::default();
        let result = db.get_option_string(OptionDatabase::Password);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::NotFound);
    }

    #[test]
    fn test_username_must_be_string() {
        let mut db = QuestDbDatabase::default();
        let result = db.set_option(OptionDatabase::Username, OptionValue::Int(123));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::InvalidArguments);
    }

    #[test]
    fn test_password_must_be_string() {
        let mut db = QuestDbDatabase::default();
        let result = db.set_option(OptionDatabase::Password, OptionValue::Int(123));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::InvalidArguments);
    }

    #[test]
    fn test_new_connection_with_credentials() {
        let mut db = QuestDbDatabase::default();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("ws://localhost:9000".to_string()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Username,
            OptionValue::String("admin".to_string()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Password,
            OptionValue::String("secret".to_string()),
        )
        .unwrap();
        assert!(db.new_connection().is_ok());
    }
}
