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
    #[validate(min_items = 1)]
    pub(crate) filter_if_contains: Option<Vec<String>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::tests::build_config;

    #[test]
    fn minimal_confg() {
        let config = r#"
        spreadsheet_id = "123"
        "#;

        let config: Logs =
            build_config(config).expect("should be able to build minimum configuration");
        assert_eq!(config.spreadsheet_id, "123");
        // Defaults
        assert_eq!(config.push_interval_secs, 5);
        assert_eq!(config.filter_if_contains, None);
        assert!(channel_capacity(&config.push_interval_secs) > 0);
    }

    #[test]
    #[should_panic(expected = "filter_if_contains")]
    fn filter_if_contains_cannot_be_empty() {
        let config = r#"
        spreadsheet_id = "123"
        filter_if_contains = []
        "#;

        let _: Logs = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "push_interval_secs")]
    fn push_interval_cannot_be_less_than_minimum() {
        let config = r#"
        spreadsheet_id = "123"
        push_interval_secs = 2
        "#;

        let _: Logs = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "push_interval_secs")]
    fn push_interval_cannot_be_greater_than_maximum() {
        let config = r#"
        spreadsheet_id = "123"
        push_interval_secs = 11
        "#;

        let _: Logs = build_config(config).unwrap();
    }
}
