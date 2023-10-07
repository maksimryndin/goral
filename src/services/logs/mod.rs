pub(crate) mod configuration;
use crate::messenger::configuration::MessengerConfig;
use crate::messenger::BoxedMessenger;
use crate::services::logs::configuration::Logs;
use crate::services::Service;
use crate::services::Shared;
use crate::spreadsheet::Sheet;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

pub(crate) struct LogsService<'s> {
    shared: Shared<'s>,
    name: &'static str,
    spreadsheet_id: String,
    push_interval: Duration,
    scrape_interval: Duration,
    sheets: Vec<Sheet>,
    messenger_config: Option<MessengerConfig>,
}

impl<'s> LogsService<'s> {
    pub(crate) fn new(shared: Shared<'s>, config: Logs) -> LogsService<'s> {
        Self {
            shared,
            name: "logs",
            spreadsheet_id: config.spreadsheet_id,
            push_interval: Duration::from_secs(config.push_interval_secs.into()),
            scrape_interval: Duration::from_secs(config.scrape_interval_secs.into()),
            sheets: vec![],
            messenger_config: config.messenger,
        }
    }
}

#[async_trait]
impl<'s> Service<'s> for LogsService<'s> {
    fn name(&self) -> &str {
        self.name
    }

    fn spreadsheet_id(&self) -> &str {
        self.spreadsheet_id.as_str()
    }

    fn set_messenger(&mut self, messenger: Arc<BoxedMessenger>) {
        self.shared.messenger = Some(messenger);
    }
}
