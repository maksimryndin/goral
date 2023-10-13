pub(crate) mod configuration;
use crate::messenger::configuration::MessengerConfig;

use crate::services::logs::configuration::Logs;
use crate::services::Service;
use crate::Shared;
use async_trait::async_trait;

use std::time::Duration;

pub const LOGS_SERVICE_NAME: &str = "logs";

pub(crate) struct LogsService {
    shared: Shared,
    name: &'static str,
    spreadsheet_id: String,
    push_interval: Duration,
    scrape_interval: Duration,
    messenger_config: Option<MessengerConfig>,
}

impl LogsService {
    pub(crate) fn new(shared: Shared, config: Logs) -> LogsService {
        Self {
            shared,
            name: "logs",
            spreadsheet_id: config.spreadsheet_id,
            push_interval: Duration::from_secs(config.push_interval_secs.into()),
            scrape_interval: Duration::from_secs(config.scrape_interval_secs.into()),
            messenger_config: config.messenger,
        }
    }
}

#[async_trait]
impl Service for LogsService {
    fn name(&self) -> &str {
        self.name
    }

    fn spreadsheet_id(&self) -> &str {
        self.spreadsheet_id.as_str()
    }
}
