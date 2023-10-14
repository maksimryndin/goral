pub(crate) mod configuration;

use crate::messenger::configuration::MessengerConfig;
use crate::messenger::BoxedMessenger;
use crate::services::general::configuration::General;
use crate::services::Service;
use crate::storage::AppendableLog;
use crate::{Notification, Shared};

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

        let result = messenger
            .send_by_level(
                &self.messenger_config,
                &notification.message,
                notification.level,
            )
            .await;
        if let Err(_) = result {
            tracing::error!(
                "{} service failed to send message: {:?}",
                self.name(),
                notification
            );
        }
    }

    async fn collect_notifications(&mut self) {
        let messenger = self
            .shared
            .messenger
            .clone()
            .expect("assert: messenger is always configured for general service");
        while let Some(notification) = self.channel.recv().await {
            self.send_notification(notification, &messenger).await;
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

    async fn run(&mut self, _: AppendableLog, mut shutdown: broadcast::Receiver<u16>) {
        tracing::info!("running with log level {}", self.log_level);
        loop {
            tokio::select! {
                result = shutdown.recv() => {
                    let graceful_shutdown_timeout = match result {
                        Err(_) => panic!("assert: shutdown signal sender should be dropped after all service listeneres"),
                        Ok(graceful_shutdown_timeout) => graceful_shutdown_timeout,
                    };
                    tracing::info!("{} service has got shutdown signal", self.name());
                    // we read out messages from other services and componens as much as we can (so we don't close the channel)
                    assert!(graceful_shutdown_timeout > 0, "graceful_shutdown_timeout is validated in configuration to be positive");
                    let graceful_shutdown_timeout = graceful_shutdown_timeout - 1;
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(graceful_shutdown_timeout.into())) => {},
                        _ = self.collect_notifications() => {}
                    }
                    tracing::info!("{} service has successfully shutdowned", self.name());
                    return;
                },
                _ = self.collect_notifications() => {}
            }
        }
    }
}
