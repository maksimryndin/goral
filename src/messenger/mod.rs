pub(crate) mod configuration;
mod telegram;
use crate::messenger::configuration::MessengerConfig;
use crate::messenger::telegram::Telegram;
use anyhow::anyhow;
use anyhow::Result;
use async_trait::async_trait;

pub(crate) type BoxedMessenger = Box<dyn Messenger + Sync + Send>;

#[async_trait]
pub(crate) trait Messenger {
    // when implementing info, consider supressing mesenger's notifications for it
    // if the messenger allows
    async fn send_info(&self, config: &MessengerConfig, markdown: &str) -> Result<()>;
    async fn send_warning(&self, config: &MessengerConfig, markdown: &str) -> Result<()>;
    async fn send_error(&self, config: &MessengerConfig, markdown: &str) -> Result<()>;
}

pub(crate) fn get_messenger(host: &str) -> Result<BoxedMessenger> {
    if host.contains("telegram") {
        Ok(Box::new(Telegram::new()))
    } else {
        Err(anyhow!(
            "Unsupported messenger. Currently available: Telegram"
        ))
    }
}
