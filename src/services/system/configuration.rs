use crate::configuration::{ceiled_division, push_interval_secs, scrape_interval_secs, APP_NAME};
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

pub(super) fn scrape_push_rule(
    scrape_timeout_ms: &u32,
    scrape_interval_secs: &u16,
    push_interval_secs: &u16,
) -> Result<usize, serde_valid::validation::Error> {
    if scrape_interval_secs > push_interval_secs {
        return Err(serde_valid::validation::Error::Custom(
            format!("push interval ({push_interval_secs}) shouldn't be greater than scrape interval {scrape_interval_secs}")
        ));
    }

    if *scrape_timeout_ms > *scrape_interval_secs as u32 * 1000 {
        return Err(serde_valid::validation::Error::Custom(
            format!("Scrape timeout ({scrape_timeout_ms}ms) shouldn't be greater than scrape interval ({scrape_interval_secs}s)")
        ));
    }

    let number_of_rows_in_batch = ceiled_division(*push_interval_secs, *scrape_interval_secs);
    const LIMIT: u16 = 20;
    if number_of_rows_in_batch > LIMIT {
        return Err(serde_valid::validation::Error::Custom(
            format!("push interval ({push_interval_secs}) is too big or scrape interval ({scrape_interval_secs}) is too small - too much data ({number_of_rows_in_batch} rows vs limit of {LIMIT}) would be accumulated before saving to a spreadsheet")
        ));
    }
    // appending to log is time-consuming
    // during the append we accumulate metrics rows in the channel
    // Estimate of append duration - 1 sec per row
    let append_duration = number_of_rows_in_batch;
    let number_of_queued_rows = ceiled_division(append_duration as u16, *scrape_interval_secs);
    if number_of_queued_rows > LIMIT {
        return Err(serde_valid::validation::Error::Custom(
            format!("push interval ({push_interval_secs}) is too big or scrape interval ({scrape_interval_secs}) is too small - too much data (estimated {number_of_queued_rows} rows vs limit of {LIMIT}) would be accumulated before saving to a spreadsheet")
        ));
    }
    Ok(number_of_queued_rows as usize)
}

fn scrape_timeout_ms() -> u32 {
    3000
}

fn mounts() -> Vec<String> {
    vec!["/".to_string()]
}

fn process_names() -> Vec<String> {
    vec![APP_NAME.to_lowercase()]
}

#[derive(Debug, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
#[rule(scrape_push_rule(scrape_timeout_ms, scrape_interval_secs, push_interval_secs))]
#[rule(alerts_cpu_rule(messenger, alert_if_cpu_usage_percent_more_than))]
#[rule(alerts_disk_rule(messenger, alert_if_free_disk_space_percent_less_than))]
#[rule(alerts_memory_rule(messenger, alert_if_free_memory_percent_less_than))]
#[allow(unused)]
pub(crate) struct System {
    pub(crate) spreadsheet_id: String,
    pub(crate) messenger: Option<MessengerConfig>,
    #[validate(minimum = 10)]
    #[serde(default = "push_interval_secs")]
    pub(crate) push_interval_secs: u16,
    #[validate(minimum = 1)]
    #[serde(default = "scrape_interval_secs")]
    pub(crate) scrape_interval_secs: u16,
    #[serde(default = "scrape_timeout_ms")]
    pub(crate) scrape_timeout_ms: u32,
    #[serde(default = "mounts")]
    pub(crate) mounts: Vec<String>,
    #[serde(default = "process_names")]
    pub(crate) process_names: Vec<String>,
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
