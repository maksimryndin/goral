use crate::configuration::{host_port_validation, push_interval_secs, scrape_interval_secs};

use serde_derive::Deserialize;
use serde_valid::Validate;

use url::Url;

#[derive(Debug, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
#[allow(unused)]
pub(crate) struct Metrics {
    pub(crate) spreadsheet_id: String,
    #[validate(minimum = 10)]
    #[serde(default = "push_interval_secs")]
    pub(crate) push_interval_secs: u16,
    #[validate(minimum = 1)]
    #[serde(default = "scrape_interval_secs")]
    pub(crate) scrape_interval_secs: u16,
    #[validate(custom(host_port_validation))]
    pub(crate) endpoints: Vec<Url>,
}
