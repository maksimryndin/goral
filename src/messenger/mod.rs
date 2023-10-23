pub(crate) mod configuration;
mod slack;
mod telegram;
use crate::messenger::configuration::MessengerConfig;
use crate::messenger::slack::Slack;
use crate::messenger::telegram::Telegram;
use anyhow::anyhow;
use anyhow::Result;
use async_trait::async_trait;
use tracing::Level;

pub type BoxedMessenger = Box<dyn Messenger + Sync + Send>;

#[async_trait]
pub trait Messenger {
    // when implementing info, consider supressing mesenger's notifications for it
    // if the messenger allows
    async fn send_info(&self, config: &MessengerConfig, markdown: &str) -> Result<()>;
    async fn send_warning(&self, config: &MessengerConfig, markdown: &str) -> Result<()>;
    async fn send_error(&self, config: &MessengerConfig, markdown: &str) -> Result<()>;
    async fn send_by_level(
        &self,
        config: &MessengerConfig,
        markdown: &str,
        level: Level,
    ) -> Result<()> {
        match level {
            Level::INFO | Level::DEBUG | Level::TRACE => self.send_info(config, markdown).await,
            Level::WARN => self.send_warning(config, markdown).await,
            Level::ERROR => self.send_error(config, markdown).await,
        }
    }
}

pub fn get_messenger(host: &str) -> Result<BoxedMessenger> {
    if host.contains("telegram") {
        Ok(Box::new(Telegram::new()))
    } else if host.contains("slack") {
        Ok(Box::new(Slack::new()))
    } else {
        Err(anyhow!(
            "Unsupported messenger. Currently available: Telegram"
        ))
    }
}
