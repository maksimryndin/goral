pub(crate) mod configuration;
use crate::messenger::configuration::MessengerConfig;
use crate::messenger::BoxedMessenger;
use crate::services::resources::configuration::Resources;
use crate::services::Service;
use crate::services::Shared;
use crate::spreadsheet::Sheet;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

pub(crate) struct ResourcesService<'s> {
    shared: Shared<'s>,
    name: &'static str,
    spreadsheet_id: String,
    push_interval: Duration,
    scrape_interval: Duration,
    sheets: Vec<Sheet>,
    messenger_config: Option<MessengerConfig>,
    alert_if_cpu_usage_percent_more_than: Option<u8>,
    alert_if_free_disk_space_percent_less_than: Option<u8>,
    alert_if_free_memory_percent_less_than: Option<u8>,
}

impl<'s> ResourcesService<'s> {
    pub(crate) fn new(shared: Shared<'s>, config: Resources) -> ResourcesService<'s> {
        Self {
            shared,
            name: "resources",
            spreadsheet_id: config.spreadsheet_id,
            push_interval: Duration::from_secs(config.push_interval_secs.into()),
            scrape_interval: Duration::from_secs(config.scrape_interval_secs.into()),
            sheets: vec![],
            messenger_config: config.messenger,
            alert_if_cpu_usage_percent_more_than: config.alert_if_cpu_usage_percent_more_than,
            alert_if_free_disk_space_percent_less_than: config
                .alert_if_free_disk_space_percent_less_than,
            alert_if_free_memory_percent_less_than: config.alert_if_free_memory_percent_less_than,
        }
    }
}

#[async_trait]
impl<'s> Service<'s> for ResourcesService<'s> {
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
