pub(crate) mod configuration;
use crate::messenger::configuration::MessengerConfig;
use crate::messenger::BoxedMessenger;
use crate::services::resources::configuration::Resources;
use crate::services::Service;
use crate::Shared;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

pub const RESOURCES_SERVICE_NAME: &str = "resources";

pub(crate) struct ResourcesService {
    shared: Shared,
    name: &'static str,
    spreadsheet_id: String,
    push_interval: Duration,
    scrape_interval: Duration,
    messenger_config: Option<MessengerConfig>,
    alert_if_cpu_usage_percent_more_than: Option<u8>,
    alert_if_free_disk_space_percent_less_than: Option<u8>,
    alert_if_free_memory_percent_less_than: Option<u8>,
}

impl ResourcesService {
    pub(crate) fn new(shared: Shared, config: Resources) -> ResourcesService {
        Self {
            shared,
            name: "resources",
            spreadsheet_id: config.spreadsheet_id,
            push_interval: Duration::from_secs(config.push_interval_secs.into()),
            scrape_interval: Duration::from_secs(config.scrape_interval_secs.into()),
            messenger_config: config.messenger,
            alert_if_cpu_usage_percent_more_than: config.alert_if_cpu_usage_percent_more_than,
            alert_if_free_disk_space_percent_less_than: config
                .alert_if_free_disk_space_percent_less_than,
            alert_if_free_memory_percent_less_than: config.alert_if_free_memory_percent_less_than,
        }
    }
}

#[async_trait]
impl Service for ResourcesService {
    fn name(&self) -> &str {
        self.name
    }

    fn spreadsheet_id(&self) -> &str {
        self.spreadsheet_id.as_str()
    }
}
