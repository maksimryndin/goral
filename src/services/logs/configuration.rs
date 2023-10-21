
use crate::messenger::configuration::MessengerConfig;

use serde_derive::Deserialize;
use serde_valid::Validate;

pub(super) fn channel_capacity(push_interval_secs: &u16) -> usize {
    const AVERAGE_DATAROWS_PER_SECOND: u16 = 10; // estimate of average log lines per second
    let number_of_rows_in_batch = AVERAGE_DATAROWS_PER_SECOND * push_interval_secs;
    // appending to log is time-consuming
    // during the append we accumulate datarows in the channel
    // Estimate of append duration - 1 sec per row
    let append_duration = number_of_rows_in_batch;
    let number_of_queued_rows = append_duration * AVERAGE_DATAROWS_PER_SECOND;
    number_of_queued_rows as usize
}

fn logs_push_interval_secs() -> u16 {
    5
}

#[derive(Debug, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
#[allow(unused)]
pub(crate) struct Logs {
    pub(crate) spreadsheet_id: String,
    pub(crate) messenger: Option<MessengerConfig>,
    #[validate(maximum = 10)]
    #[validate(minimum = 3)]
    #[serde(default = "logs_push_interval_secs")]
    pub(crate) push_interval_secs: u16,
    pub(crate) filter_if_contains: Option<Vec<String>>,
}
