use crate::configuration::case_insensitive_enum;
use crate::messenger::configuration::MessengerConfig;

use serde_derive::Deserialize;
use serde_valid::Validate;

use tracing::Level;

fn log_level() -> Level {
    Level::INFO
}

fn graceful_timeout_secs() -> u16 {
    10
}

#[derive(Debug, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct General {
    #[serde(default = "log_level")]
    #[serde(deserialize_with = "case_insensitive_enum")]
    pub log_level: Level,
    pub service_account_credentials_path: String,
    pub(crate) messenger: MessengerConfig,
    #[serde(default = "graceful_timeout_secs")]
    pub graceful_timeout_secs: u16,
}
