pub(crate) mod configuration;

use crate::messenger::configuration::MessengerConfig;
use crate::messenger::BoxedMessenger;
use crate::services::general::configuration::General;
use crate::services::Service;
use crate::storage::AppendableLog;
use crate::{Notification, Shared};
use anyhow::Result;
use async_trait::async_trait;

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::mpsc::Receiver;
use tracing::{instrument, Level};

pub const GENERAL_SERVICE_NAME: &str = "general";

#[derive(Debug)]
pub(crate) struct GeneralService {
    shared: Shared,
    messenger_config: MessengerConfig,
    log_level: Level,
    channel: Receiver<Notification>,
}

impl GeneralService {
    pub(crate) fn new(
        shared: Shared,
        config: General,
        channel: Receiver<Notification>,
    ) -> GeneralService {
        Self {
            shared,
            messenger_config: config.messenger,
            log_level: config.log_level,
            channel,
        }
    }

    async fn send_notification(&self, notification: Notification, messenger: &Arc<BoxedMessenger>) {
        tracing::debug!(
            "{} got notification to send: {:?}",
            self.name(),
            notification
        );

        let result = match notification.level {
            Level::INFO | Level::DEBUG | Level::TRACE => {
                messenger
                    .send_info(&self.messenger_config, &notification.message)
                    .await
            }
            Level::WARN => {
                messenger
                    .send_warning(&self.messenger_config, &notification.message)
                    .await
            }
            Level::ERROR => {
                messenger
                    .send_error(&self.messenger_config, &notification.message)
                    .await
            }
        };
        if let Err(_) = result {
            tracing::error!(
                "{} service failed to send message: {:?}",
                self.name(),
                notification
            );
        }
    }
}

#[async_trait]
impl Service for GeneralService {
    fn name(&self) -> &str {
        GENERAL_SERVICE_NAME
    }

    fn spreadsheet_id(&self) -> &str {
        ""
    }

    #[instrument(skip_all)]
    async fn run(&mut self, _: AppendableLog, mut shutdown: broadcast::Receiver<u16>) {
        let messenger = self
            .shared
            .messenger
            .clone()
            .expect("assert: messenger is always configured for general service");
        tracing::info!(
            "{} initialized with log level {}",
            self.name(),
            self.log_level
        );
        loop {
            tokio::select! {
                result = shutdown.recv() => {
                    let graceful_shutdown_timeout = match result {
                        Err(_) => panic!("assert: shutdown signal sender should be dropped after all service listeneres"),
                        Ok(graceful_shutdown_timeout) => graceful_shutdown_timeout,
                    };
                    tracing::info!("{} service has got shutdown signal", self.name());
                    if graceful_shutdown_timeout > 0 {
                        let graceful_shutdown_timeout: u16 = graceful_shutdown_timeout - 1;
                        loop {
                            tokio::select! {
                                _ = tokio::time::sleep(Duration::from_secs(graceful_shutdown_timeout.into())) => return,
                                Some(notification) = self.channel.recv() => {
                                    self.send_notification(notification, &messenger).await;
                                }
                            }
                        }
                    }
                    tracing::info!("{} service has successfully shutdowned", self.name());
                    return;
                },
                Some(notification) = self.channel.recv() => {
                    self.send_notification(notification, &messenger).await;
                }
            }
        }
    }
}
