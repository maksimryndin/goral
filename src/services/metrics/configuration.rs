use crate::configuration::{
    ceiled_division, host_validation, port_validation, push_interval_secs, scrape_interval_secs,
};
use crate::messenger::configuration::MessengerConfig;
use serde_derive::Deserialize;
use serde_valid::Validate;
use url::Url;

pub(super) fn scrape_push_rule(
    endpoints: &Vec<Url>,
    push_interval_secs: &u16,
    scrape_interval_secs: &u16,
    scrape_timeout_ms: &u32,
) -> Result<usize, serde_valid::validation::Error> {
    if scrape_interval_secs > push_interval_secs {
        return Err(serde_valid::validation::Error::Custom(
            format!("push interval ({push_interval_secs}) shouldn't be greater than scrape interval {scrape_interval_secs}")
        ));
    }

    if *scrape_timeout_ms > (*scrape_interval_secs as u32) * 1000 / 2 {
        return Err(serde_valid::validation::Error::Custom(
            format!("Scrape timeout ({scrape_timeout_ms}ms) shouldn't be greater than half of scrape interval ({scrape_interval_secs}s)")
        ));
    }

    let number_of_metrics_endpoints_in_batch =
        ceiled_division(*push_interval_secs, *scrape_interval_secs) * endpoints.len() as u16;
    const ENDPOINTS_LIMIT: u16 = 3;
    if number_of_metrics_endpoints_in_batch > ENDPOINTS_LIMIT {
        return Err(serde_valid::validation::Error::Custom(
            format!("push interval ({push_interval_secs}) is too big or scrape interval ({scrape_interval_secs}) is too small - too much data ({number_of_metrics_endpoints_in_batch} metrics pages vs limit of {ENDPOINTS_LIMIT}) would be accumulated before saving to a spreadsheet")
        ));
    }
    // appending to log is time-consuming
    // during the append we accumulate datarows in the channel
    // Estimate of append duration - 1 sec per row
    const ESTIMATED_ROWS_PER_METRICS_ENDPOINT: usize = 10;
    let append_duration =
        number_of_metrics_endpoints_in_batch as usize * ESTIMATED_ROWS_PER_METRICS_ENDPOINT;
    let number_of_queued_rows = ESTIMATED_ROWS_PER_METRICS_ENDPOINT
        * ceiled_division(append_duration as u16, *scrape_interval_secs) as usize
        * endpoints.len();
    // we truncate output of probe to 1024 bytes - so estimated payload (without other fields) is around 20 KiB
    const ROWS_LIMIT: usize = 30;
    if number_of_queued_rows > ROWS_LIMIT {
        return Err(serde_valid::validation::Error::Custom(
            format!("push interval ({push_interval_secs}) is too big or scrape interval ({scrape_interval_secs}) is too small - too much data (estimated {number_of_queued_rows} rows vs limit of {ROWS_LIMIT}) would be accumulated before saving to a spreadsheet")
        ));
    }
    Ok(number_of_queued_rows)
}

pub(crate) fn host_port_validation_for_http(
    endpoints: &Vec<Url>,
) -> Result<(), serde_valid::validation::Error> {
    for e in endpoints {
        host_validation(e)?;
        port_validation(e)?;
        if e.scheme() != "http" && e.scheme() != "https" {
            return Err(serde_valid::validation::Error::Custom(format!(
                "Scheme for metrics endpoint {e} should be either http or https"
            )));
        }
    }
    Ok(())
}

fn scrape_timeout_ms() -> u32 {
    3000
}

#[derive(Debug, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
#[rule(scrape_push_rule(endpoints, push_interval_secs, scrape_interval_secs, scrape_timeout_ms))]
#[allow(unused)]
pub(crate) struct Metrics {
    pub(crate) messenger: Option<MessengerConfig>,
    pub(crate) spreadsheet_id: String,
    #[validate(minimum = 10)]
    #[serde(default = "push_interval_secs")]
    pub(crate) push_interval_secs: u16,
    #[validate(minimum = 1)]
    #[serde(default = "scrape_interval_secs")]
    pub(crate) scrape_interval_secs: u16,
    #[serde(default = "scrape_timeout_ms")]
    pub(crate) scrape_timeout_ms: u32,
    #[validate(custom(host_port_validation_for_http))]
    pub(crate) endpoints: Vec<Url>,
}
