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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::tests::build_config;
    use std::str::FromStr;
    use url::Url;

    #[test]
    fn normal_confg() {
        let config = r#"
        bot_token = "123"
        chat_id = "test_chat_id"
        url = "https://api.telegram.org/bot123/sendMessage"
        "#;

        let config: MessengerConfig =
            build_config(config).expect("should be able to build minimum configuration");
        assert_eq!(config.bot_token, "123");
        assert_eq!(config.chat_id, "test_chat_id");
        assert_eq!(
            config.url,
            Url::from_str("https://api.telegram.org/bot123/sendMessage").unwrap()
        );
        assert!(config.url.as_str().contains("api.telegram.org"));
        assert_eq!(
            config.url.host().map(|h| h.to_string()),
            Some("api.telegram.org".to_string()),
            "host is required"
        );
    }

    #[test]
    fn token_and_url_are_not_printed() {
        let config = r#"
        bot_token = "123"
        chat_id = "test_chat_id"
        url = "https://api.telegram.org/bot123/sendMessage"
        "#;

        let config: MessengerConfig =
            build_config(config).expect("should be able to build minimum configuration");
        // We shouldn't output confidential token and full url in logs
        let printed = format!("{:?}", config);
        assert!(
            !printed.contains("123"),
            "debug print doesn't contain secret token"
        );
        assert!(
            !printed.contains("/bot123/sendMessage"),
            "debug print doesn't contain url path as it may contain the token"
        );
    }

    #[test]
    #[should_panic(expected = "host")]
    fn host_is_required() {
        let config = r#"
        bot_token = "123"
        chat_id = "test_chat_id"
        url = "https://"
        "#;
        let _: MessengerConfig = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "missing field `url`")]
    fn url_is_required() {
        let config = r#"
        bot_token = "123"
        chat_id = "test_chat_id"
        "#;

        let _: MessengerConfig = build_config(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "missing field `chat_id`")]
    fn chat_id_is_required() {
        let config = r#"
        bot_token = "123"
        url = "https://api.telegram.org/bot123/sendMessage"
        "#;

        let _: MessengerConfig = build_config(config).unwrap();
    }
}
