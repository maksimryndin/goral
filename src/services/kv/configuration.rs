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
    pub(crate) messenger: Option<MessengerConfig>,
    #[validate(minimum = 49152, message_fn(port_min_error_message))]
    #[validate(maximum = 65535, message_fn(port_max_error_message))]
    pub(crate) port: u16,
}
