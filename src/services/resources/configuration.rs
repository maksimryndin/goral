use crate::configuration::{push_interval_secs, scrape_interval_secs};
use crate::messenger::configuration::MessengerConfig;

use serde_derive::Deserialize;
use serde_valid::Validate;

fn alerts_cpu_rule(
    messenger: &Option<MessengerConfig>,
    alert_if_cpu_usage_percent_more_than: &Option<u8>,
) -> Result<(), serde_valid::validation::Error> {
    if messenger.is_none() && alert_if_cpu_usage_percent_more_than.is_some() {
        return Err(serde_valid::validation::Error::Custom(
            "`[resources].messenger` should be specified if `[resources].alert_if_cpu_usage_percent_more_than` is specified".to_string(),
        ));
    }
    Ok(())
}

fn alerts_disk_rule(
    messenger: &Option<MessengerConfig>,
    alert_if_free_disk_space_percent_less_than: &Option<u8>,
) -> Result<(), serde_valid::validation::Error> {
    if messenger.is_none() && alert_if_free_disk_space_percent_less_than.is_some() {
        return Err(serde_valid::validation::Error::Custom(
            "`[resources].messenger` should be specified if `[resources].alert_if_free_disk_space_percent_less_than` is specified".to_string(),
        ));
    }
    Ok(())
}

fn alerts_memory_rule(
    messenger: &Option<MessengerConfig>,
    alert_if_free_memory_percent_less_than: &Option<u8>,
) -> Result<(), serde_valid::validation::Error> {
    if messenger.is_none() && alert_if_free_memory_percent_less_than.is_some() {
        return Err(serde_valid::validation::Error::Custom(
            "`[resources].messenger` should be specified if `[resources].alert_if_free_memory_percent_less_than` is specified".to_string(),
        ));
    }
    Ok(())
}

#[derive(Debug, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
#[rule(alerts_cpu_rule(messenger, alert_if_cpu_usage_percent_more_than))]
#[rule(alerts_disk_rule(messenger, alert_if_free_disk_space_percent_less_than))]
#[rule(alerts_memory_rule(messenger, alert_if_free_memory_percent_less_than))]
#[allow(unused)]
pub(crate) struct Resources {
    pub(crate) spreadsheet_id: String,
    pub(crate) messenger: Option<MessengerConfig>,
    #[validate(minimum = 10)]
    #[serde(default = "push_interval_secs")]
    pub(crate) push_interval_secs: u16,
    #[validate(minimum = 1)]
    #[serde(default = "scrape_interval_secs")]
    pub(crate) scrape_interval_secs: u16,
    #[validate(minimum = 0)]
    #[validate(maximum = 100)]
    pub(crate) alert_if_cpu_usage_percent_more_than: Option<u8>,
    #[validate(minimum = 0)]
    #[validate(maximum = 100)]
    pub(crate) alert_if_free_disk_space_percent_less_than: Option<u8>,
    #[validate(minimum = 0)]
    #[validate(maximum = 100)]
    pub(crate) alert_if_free_memory_percent_less_than: Option<u8>,
}
