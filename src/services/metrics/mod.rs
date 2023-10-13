pub(crate) mod configuration;

use crate::services::metrics::configuration::Metrics;
use crate::services::Service;
use crate::Shared;
use async_trait::async_trait;

use std::time::Duration;
use url::Url;

pub const METRICS_SERVICE_NAME: &str = "metrics";

pub(crate) struct MetricsService {
    shared: Shared,
    name: &'static str,
    spreadsheet_id: String,
    push_interval: Duration,
    scrape_interval: Duration,
    endpoints: Vec<Url>,
}

impl MetricsService {
    pub(crate) fn new(shared: Shared, config: Metrics) -> MetricsService {
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
impl Service for MetricsService {
    fn name(&self) -> &str {
        self.name
    }

    fn spreadsheet_id(&self) -> &str {
        self.spreadsheet_id.as_str()
    }
}
