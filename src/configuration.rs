use crate::services::general::configuration::General;
use crate::services::healthcheck::configuration::Healthcheck;
use crate::services::logs::configuration::Logs;
use crate::services::metrics::configuration::Metrics;
use crate::services::resources::configuration::Resources;
use config::{Config, ConfigError, Environment, File};
use serde::de::{self, Deserialize, Deserializer};
use serde_derive::Deserialize;
use serde_valid::Validate;

use std::str::FromStr;

use url::Url;

pub const APP_NAME: &str = "GORAL";

pub(crate) fn scrape_interval_secs() -> u16 {
    10
}

pub(crate) fn push_interval_secs() -> u16 {
    30
}

pub(crate) fn host_validation(url: &Url) -> Result<(), serde_valid::validation::Error> {
    url.host()
        .map(|_| ())
        .ok_or(serde_valid::validation::Error::Custom(format!(
            "host for url {} should be specified",
            url
        )))
}

pub(crate) fn port_validation(url: &Url) -> Result<(), serde_valid::validation::Error> {
    url.port()
        .map(|_| ())
        .ok_or(serde_valid::validation::Error::Custom(format!(
            "port for url {} should be specified",
            url
        )))
}

pub(crate) fn host_port_validation(
    endpoints: &Vec<Url>,
) -> Result<(), serde_valid::validation::Error> {
    for e in endpoints {
        host_validation(e)?;
        port_validation(e)?;
    }
    Ok(())
}

pub(crate) fn case_insensitive_enum<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    let variant = String::deserialize(deserializer)?.to_lowercase();
    T::from_str(&variant).map_err(de::Error::custom)
}

#[derive(Debug, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    #[validate]
    pub general: General,
    #[validate]
    pub(crate) healthcheck: Option<Healthcheck>,
    #[validate]
    pub(crate) logs: Option<Logs>,
    #[validate]
    pub(crate) metrics: Option<Metrics>,
    #[validate]
    pub(crate) resources: Option<Resources>,
}

impl Configuration {
    pub fn new(config_path: &str) -> Result<Self, ConfigError> {
        let s = Config::builder()
            .add_source(File::with_name(config_path))
            .add_source(Environment::with_prefix(APP_NAME).separator("__"))
            .build()?;
        let deserialized = s.try_deserialize::<Self>()?;
        deserialized
            .validate()
            .map_err(|e| ConfigError::Message(e.to_string()))?;
        Ok(deserialized)
    }
}
