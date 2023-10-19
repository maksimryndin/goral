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
        self.url
            .host()
            .expect("assert: host is validated to be nonempty in config")
    }
}

// We shouldn't output confidential token and full url in logs
impl Debug for MessengerConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "MessengerConfig({}:{})",
            self.url
                .host()
                .expect("assert: host for messenger is validated at configuration"),
            self.chat_id
        )
    }
}
