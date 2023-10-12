use crate::configuration::host_validation;

use serde_derive::Deserialize;
use serde_valid::Validate;
use std::fmt::{self, Debug};
use url::{Host, Url};

#[derive(Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct MessengerConfig {
    pub(crate) bot_token: String,
    pub(crate) chat_id: String,
    #[validate(custom(host_validation))]
    pub(crate) url: Url,
}

impl MessengerConfig {
    pub fn host(&self) -> Host<&str> {
        // safe to unwrap as we validate that host is not empty
        self.url.host().unwrap()
    }
}

// We should output token and full url in logs
impl Debug for MessengerConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "MessengerConfig({}:{})",
            self.url.host().unwrap(),
            self.chat_id
        )
    }
}
