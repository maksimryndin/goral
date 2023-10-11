pub(crate) mod configuration;
use crate::messenger::BoxedMessenger;
use crate::services::metrics::configuration::Metrics;
use crate::services::Service;
use crate::Shared;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

pub(crate) struct MetricsService<'s> {
    shared: Shared<'s>,
    name: &'static str,
    spreadsheet_id: String,
    push_interval: Duration,
    scrape_interval: Duration,
    endpoints: Vec<Url>,
}

impl<'s> MetricsService<'s> {
    pub(crate) fn new(shared: Shared<'s>, config: Metrics) -> MetricsService<'s> {
        Self {
            shared,
            name: "metrics",
            spreadsheet_id: config.spreadsheet_id,
            push_interval: Duration::from_secs(config.push_interval_secs.into()),
            scrape_interval: Duration::from_secs(config.scrape_interval_secs.into()),
            endpoints: config.endpoints,
        }
    }
}

#[async_trait]
impl<'s> Service<'s> for MetricsService<'s> {
    fn name(&self) -> &str {
        self.name
    }

    fn spreadsheet_id(&self) -> &str {
        self.spreadsheet_id.as_str()
    }

    fn set_messenger(&mut self, _: Arc<BoxedMessenger>) {}
}
