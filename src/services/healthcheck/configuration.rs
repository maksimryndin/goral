use crate::configuration::{
    case_insensitive_enum, ceiled_division, host_validation, port_validation, push_interval_secs,
};
use crate::messenger::configuration::MessengerConfig;

use serde_derive::Deserialize;
use serde_valid::Validate;
use std::fmt::{self, Display};
use std::str::FromStr;

use url::Url;

fn liveness_rule(
    endpoint: &Option<Url>,
    command: &Option<String>,
    typ: &LivenessType,
) -> Result<(), serde_valid::validation::Error> {
    match (endpoint, command, typ) {
        (Some(url), None, LivenessType::Http | LivenessType::Tcp | LivenessType::Grpc) => {
            host_validation(url)?;
            port_validation(url)?;
            Ok(())
        },
        (None, Some(_), LivenessType::Command) => Ok(()),
        _ => Err(serde_valid::validation::Error::Custom(
            "liveness probe type should be either `command` with respective field or `http/tcp/grpc` with `endpoint`".to_string(),
        )),
    }
}

fn timeout_period_rule(
    period_secs: &u16,
    timeout_ms: &u32,
) -> Result<(), serde_valid::validation::Error> {
    let period_ms = *period_secs as u32 * 1000;
    if timeout_ms > &period_ms {
        return Err(serde_valid::validation::Error::Custom(
            "liveness `timeout_ms` should be less than `period_secs`".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn scrape_push_rule(
    liveness: &Vec<Liveness>,
    push_interval_secs: &u16,
) -> Result<usize, serde_valid::validation::Error> {
    for l in liveness {
        if l.period_secs > *push_interval_secs {
            return Err(serde_valid::validation::Error::Custom(
                format!("push interval ({push_interval_secs}) should be greater or equal than liveness period {}", l.period_secs)
            ));
        }

        if l.period_secs == 0 {
            return Err(serde_valid::validation::Error::Custom(format!(
                "liveness period cannot be zero"
            )));
        }
    }

    let number_of_rows_in_batch = liveness.iter().fold(0, |acc, l| {
        acc + ceiled_division(*push_interval_secs, l.period_secs)
    });
    // we truncate output of probe to 1024 bytes - so estimated payload (without other fields) is around 20 KiB
    const LIMIT: u16 = 20;
    if number_of_rows_in_batch > LIMIT {
        return Err(serde_valid::validation::Error::Custom(
            format!("push interval ({push_interval_secs}) is too big for current choices of liveness periods or liveness periods are too small - too much data ({number_of_rows_in_batch} rows vs limit of {LIMIT}) would be accumulated before saving to a spreadsheet")
        ));
    }
    // appending to log is time-consuming
    // during the append we accumulate liveness outputs in the channel
    // Estimate of append duration - 1 sec per row
    // it intuitively clear for the user in a typical case of one healthcheck with period 1 sec and push interval 20 secs
    let append_duration = number_of_rows_in_batch;
    let number_of_queued_rows = liveness.iter().fold(0, |acc, l| {
        acc + ceiled_division(append_duration, l.period_secs)
    }) as usize;
    if number_of_queued_rows > LIMIT as usize {
        return Err(serde_valid::validation::Error::Custom(
            format!("push interval ({push_interval_secs}) is too big for current choices of liveness periods or liveness periods are too small - too much data ({number_of_queued_rows} rows vs limit of {LIMIT}) would be accumulated before saving to a spreadsheet")
        ));
    }
    Ok(number_of_queued_rows)
}

fn liveness_period_secs() -> u16 {
    3
}

fn liveness_timeout_ms() -> u32 {
    1000
}

#[derive(Debug, Deserialize, Clone, PartialEq, Validate)]
pub(crate) enum LivenessType {
    Http,
    Tcp,
    Command,
    Grpc,
}

impl FromStr for LivenessType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let t = match s {
            _ if s.eq_ignore_ascii_case("http") => Self::Http,
            _ if s.eq_ignore_ascii_case("tcp") => Self::Tcp,
            _ if s.eq_ignore_ascii_case("command") => Self::Command,
            _ if s.eq_ignore_ascii_case("grpc") => Self::Grpc,
            _ => return Err(format!("unsupported liveness type: {}", s)),
        };
        Ok(t)
    }
}

impl Display for LivenessType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let t = match self {
            Self::Http => "http",
            Self::Tcp => "tcp",
            Self::Command => "command",
            Self::Grpc => "grpc",
        };
        write!(f, "{}", t)
    }
}

#[derive(Debug, Deserialize, Validate)]
#[rule(liveness_rule(endpoint, command, typ))]
#[rule(timeout_period_rule(period_secs, timeout_ms))]
#[serde(deny_unknown_fields)]
pub(crate) struct Liveness {
    #[serde(default)]
    pub(crate) initial_delay_secs: u16,
    #[validate(minimum = 1)]
    #[serde(default = "liveness_period_secs")]
    pub(crate) period_secs: u16,
    #[validate(minimum = 1)]
    #[serde(default = "liveness_timeout_ms")]
    pub(crate) timeout_ms: u32,
    pub(crate) endpoint: Option<Url>,
    pub(crate) command: Option<String>,
    #[serde(deserialize_with = "case_insensitive_enum")]
    pub(crate) typ: LivenessType,
}

#[derive(Debug, Deserialize, Validate)]
#[rule(scrape_push_rule(liveness, push_interval_secs))]
#[serde(deny_unknown_fields)]
pub(crate) struct Healthcheck {
    pub(crate) messenger: Option<MessengerConfig>,
    pub(crate) spreadsheet_id: String,
    #[validate]
    #[validate(min_items = 1)]
    pub(crate) liveness: Vec<Liveness>,
    #[validate(minimum = 10)]
    #[serde(default = "push_interval_secs")]
    pub(crate) push_interval_secs: u16,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::tests::build_config;
    use std::str::FromStr;
    use url::Url;

    #[test]
    fn minimal_confg() {
        let config = r#"
        spreadsheet_id = "123"
        [[liveness]]
        typ = "Http"
        endpoint = "http://127.0.0.1:9898"
        [[liveness]]
        typ = "Command"
        command = "ls -lha /"
        "#;

        let config: Healthcheck =
            build_config(config).expect("should be able to build minimum configuration");
        assert_eq!(config.spreadsheet_id, "123");
        assert_eq!(config.liveness[0].typ, LivenessType::Http);
        assert_eq!(
            config.liveness[0].endpoint,
            Some(Url::from_str("http://127.0.0.1:9898").unwrap())
        );
        assert_eq!(config.liveness[0].command, None);
        assert_eq!(config.liveness[1].typ, LivenessType::Command);
        assert_eq!(config.liveness[1].command, Some("ls -lha /".to_string()));
        assert_eq!(config.liveness[1].endpoint, None);
        // Defaults
        assert_eq!(config.push_interval_secs, 30);
        assert_eq!(config.liveness[0].timeout_ms, 1000);
        assert_eq!(config.liveness[0].initial_delay_secs, 0);
        assert_eq!(config.liveness[0].period_secs, 3);
        assert_eq!(config.liveness[1].timeout_ms, 1000);
        assert_eq!(config.liveness[1].initial_delay_secs, 0);
        assert_eq!(config.liveness[1].period_secs, 3);

        assert!(
            scrape_push_rule(&config.liveness, &config.push_interval_secs)
                .expect("channel capacity should be calculated for defaults")
                > 0
        );
    }

    #[test]
    fn case_insentive_liveness_typ() {
        let config = r#"
        spreadsheet_id = "123"
        [[liveness]]
        typ = "hTtp"
        endpoint = "http://127.0.0.1:9898"
        "#;

        let config: Healthcheck =
            build_config(config).expect("should be able to build minimum configuration");
        assert_eq!(config.liveness[0].typ, LivenessType::Http);
    }

    #[test]
    #[should_panic(expected = "liveness")]
    fn liveness_cannot_be_empty() {
        let config = r#"
        spreadsheet_id = "123"
        liveness = []
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "timeout_ms")]
    fn probe_timeout_cannot_be_zero() {
        let config = r#"
        spreadsheet_id = "123"
        [[liveness]]
        typ = "Http"
        endpoint = "http://127.0.0.1:9898"
        timeout_ms = 0
        [[liveness]]
        typ = "Command"
        command = "ls -lha /"
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "period_secs")]
    fn period_cannot_be_less_than_timeout() {
        let config = r#"
        spreadsheet_id = "123"
        [[liveness]]
        typ = "Http"
        endpoint = "http://127.0.0.1:9898"
        period_secs = 2
        timeout_ms = 3000
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "should be greater or equal than liveness period")]
    fn period_cannot_be_greater_than_push_interval() {
        let config = r#"
        spreadsheet_id = "123"
        push_interval_secs = 5
        [[liveness]]
        typ = "Http"
        endpoint = "http://127.0.0.1:9898"
        period_secs = 10
        timeout_ms = 3000
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "would be accumulated before saving to a spreadsheet")]
    fn scrape_push_violation() {
        let config = r#"
        spreadsheet_id = "123"
        push_interval_secs = 201
        [[liveness]]
        typ = "Http"
        endpoint = "http://127.0.0.1:9898"
        period_secs = 10
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "cannot be zero")]
    fn period_cannot_be_zero() {
        let config = r#"
        spreadsheet_id = "123"
        [[liveness]]
        typ = "Http"
        endpoint = "http://127.0.0.1:9898"
        period_secs = 0
        [[liveness]]
        typ = "Command"
        command = "ls -lha /"
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(
        expected = "liveness probe type should be either `command` with respective field or `http/tcp/grpc` with `endpoint`"
    )]
    fn http_should_have_endpoint() {
        let config = r#"
        spreadsheet_id = "123"
        [[liveness]]
        typ = "Http"
        command = "ls -lha /"
        [[liveness]]
        typ = "Command"
        command = "ls -lha /"
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "port")]
    fn port_is_required() {
        let config = r#"
        spreadsheet_id = "123"
        [[liveness]]
        typ = "Http"
        endpoint = "http://localhost"
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "push_interval_secs")]
    fn push_interval_cannot_be_less_than_minimum() {
        let config = r#"
        spreadsheet_id = "123"
        push_interval_secs = 9
        [[liveness]]
        typ = "Http"
        endpoint = "http://127.0.0.1:9898"
        "#;

        let _: Healthcheck = build_config(config).unwrap();
    }
}
