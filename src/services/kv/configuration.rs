use crate::messenger::configuration::MessengerConfig;
use serde_derive::Deserialize;
use serde_valid::Validate;

const PORT_RANGE_MESSAGE: &str = "consider using a port in the range 49152-65535 to avoid potential conflicts (see https://en.wikipedia.org/wiki/List_of_TCP_and_UDP_port_numbers#Dynamic,_private_or_ephemeral_ports)";

#[inline]
fn port_min_error_message(_params: &serde_valid::MinimumError) -> String {
    PORT_RANGE_MESSAGE.to_string()
}

#[inline]
fn port_max_error_message(_params: &serde_valid::MaximumError) -> String {
    PORT_RANGE_MESSAGE.to_string()
}

#[derive(Debug, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
#[allow(unused)]
pub(crate) struct Kv {
    pub(crate) spreadsheet_id: String,
    #[validate]
    pub(crate) messenger: Option<MessengerConfig>,
    #[validate(minimum = 49152, message_fn(port_min_error_message))]
    #[validate(maximum = 65535, message_fn(port_max_error_message))]
    pub(crate) port: u16,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::tests::build_config;

    #[test]
    fn minimal_confg() {
        let config = r#"
        spreadsheet_id = "123"
        port = 49152
        "#;

        let config: Kv =
            build_config(config).expect("should be able to build minimum configuration");
        assert_eq!(config.spreadsheet_id, "123");
        assert_eq!(config.port, 49152);
    }

    #[test]
    #[should_panic(expected = "port")]
    fn minimal_port() {
        let config = r#"
        spreadsheet_id = "123"
        port = 49151
        "#;

        let _: Kv = build_config(config).expect("should be able to build minimum configuration");
    }

    #[test]
    #[should_panic(expected = "port")]
    fn maximal_port() {
        let config = r#"
        spreadsheet_id = "123"
        port = 65536
        "#;

        let _: Kv = build_config(config).expect("should be able to build minimum configuration");
    }

    #[test]
    #[should_panic(expected = "doesn't match messenger implementation")]
    fn wrong_messenger_confg_wrong_host() {
        let config = r#"
        messenger.url = "https://api.telegram.org/bot123/sendMessage"
        spreadsheet_id = "123"
        port = 65500
        "#;

        let _: Kv = build_config(config).unwrap();
    }
}
