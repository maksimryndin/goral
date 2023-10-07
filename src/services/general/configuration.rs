use crate::configuration::{case_insensitive_enum, host_validation, port_validation};
use crate::messenger::configuration::MessengerConfig;

use serde_derive::Deserialize;
use serde_valid::Validate;
use std::fmt::{self, Display};
use std::str::FromStr;
use tracing::Level;
use url::Url;

fn liveness_rule(
    endpoint: &Option<Url>,
    command: &Option<String>,
    typ: &LivenessType,
) -> Result<(), serde_valid::validation::Error> {
    match (endpoint, command, typ) {
        (Some(url), None, LivenessType::Http | LivenessType::Tcp | LivenessType::Grpc) => {
            host_validation(url)?;
            port_validation(url)?;
            Ok(())
        },
        (None, Some(_), LivenessType::Command) => Ok(()),
        _ => Err(serde_valid::validation::Error::Custom(
            "liveness probe type should be either `command` with respective field or `http/tcp/grpc` with `endpoint`".to_owned(),
        )),
    }
}

fn timeout_period_rule(
    period_secs: &u16,
    timeout_ms: &u32,
) -> Result<(), serde_valid::validation::Error> {
    let period_ms = *period_secs as u32 * 1000;
    if timeout_ms > &period_ms {
        return Err(serde_valid::validation::Error::Custom(
            "liveness `timeout_ms` should be less than `period_secs`".to_owned(),
        ));
    }
    Ok(())
}

fn liveness_period_secs() -> u16 {
    3
}

fn liveness_timeout_ms() -> u32 {
    3000
}

fn log_level() -> Level {
    Level::INFO
}

#[derive(Debug, Deserialize, Validate)]
pub(crate) enum LivenessType {
    Http,
    Tcp,
    Command,
    Grpc,
}

impl FromStr for LivenessType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let t = match s {
            _ if s.eq_ignore_ascii_case("http") => Self::Http,
            _ if s.eq_ignore_ascii_case("tcp") => Self::Tcp,
            _ if s.eq_ignore_ascii_case("command") => Self::Command,
            _ if s.eq_ignore_ascii_case("grpc") => Self::Grpc,
            _ => return Err(format!("unsupported liveness type: {}", s)),
        };
        Ok(t)
    }
}

impl Display for LivenessType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let t = match self {
            Self::Http => "http",
            Self::Tcp => "tcp",
            Self::Command => "command",
            Self::Grpc => "grpc",
        };
        write!(f, "{}", t)
    }
}

#[derive(Debug, Deserialize, Validate)]
#[rule(liveness_rule(endpoint, command, typ))]
#[rule(timeout_period_rule(period_secs, timeout_ms))]
#[serde(deny_unknown_fields)]
pub(crate) struct Liveness {
    #[serde(default)]
    pub(crate) initial_delay_secs: u16,
    #[validate(minimum = 1)]
    #[serde(default = "liveness_period_secs")]
    pub(crate) period_secs: u16,
    #[validate(minimum = 1)]
    #[serde(default = "liveness_timeout_ms")]
    pub(crate) timeout_ms: u32,
    pub(crate) endpoint: Option<Url>,
    pub(crate) command: Option<String>,
    #[serde(deserialize_with = "case_insensitive_enum")]
    pub(crate) typ: LivenessType,
}

#[derive(Debug, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub(crate) struct General {
    #[serde(default = "log_level")]
    #[serde(deserialize_with = "case_insensitive_enum")]
    pub(crate) log_level: Level,
    pub(crate) service_account_credentials_path: String,
    pub(crate) messenger: MessengerConfig,
    pub(crate) spreadsheet_id: String,
    pub(crate) liveness: Vec<Liveness>,
}
