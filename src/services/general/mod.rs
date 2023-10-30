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
use tracing::Level;

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
            tracing::debug!("{:?}", notification);
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
        let collect = self.collect_notifications();
        tokio::pin!(collect); // pin and pass by mutable ref to prevent cancelling this future by select!
        tokio::select! {
            result = shutdown.recv() => {
                let graceful_shutdown_timeout = match result {
                    Err(_) => panic!("assert: shutdown signal sender should be dropped after all service listeneres"),
                    Ok(graceful_shutdown_timeout) => graceful_shutdown_timeout,
                };
                tracing::info!("{} service has got shutdown signal", GENERAL_SERVICE_NAME);
                // we read out messages from other services and componens as much as we can (so we don't close the channel)
                assert!(graceful_shutdown_timeout > 0, "graceful_shutdown_timeout is validated in configuration to be positive");
                let graceful_shutdown_timeout = graceful_shutdown_timeout - 1;
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(graceful_shutdown_timeout.into())) => {},
                    _ = &mut collect => {}
                }
                tracing::info!("{} service has successfully shutdowned", GENERAL_SERVICE_NAME);
                return;
            },
            _ = &mut collect => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::tests::build_config;
    use crate::messenger::tests::TestMessenger;
    use crate::spreadsheet::spreadsheet::SpreadsheetAPI;
    use crate::spreadsheet::tests::TestState;
    use crate::storage::Storage;
    use crate::tests::{TEST_HOST_ID, TEST_PROJECT_ID};
    use crate::{create_log, Sender, Shared};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tokio::sync::{broadcast, mpsc};

    #[tokio::test]
    async fn general_service_flow() {
        const NUMBER_OF_MESSAGES: usize = 10;

        let (send_notification, notifications_receiver) = mpsc::channel(NUMBER_OF_MESSAGES);
        let send_notification = Sender::new(send_notification);
        let sheets_api = SpreadsheetAPI::new(
            send_notification.clone(),
            TestState::new(vec![], None, None),
        );
        let storage = Arc::new(Storage::new(
            TEST_HOST_ID.to_string(),
            TEST_PROJECT_ID.to_string(),
            sheets_api,
            send_notification.clone(),
        ));
        let log = create_log(
            storage.clone(),
            "spreadsheet1".to_string(),
            GENERAL_SERVICE_NAME.to_string(),
        );

        let (shutdown, rx) = broadcast::channel(1);

        let config = r#"
        service_account_credentials_path = "/path/to/service_account.json"
        messenger.url = "https://discord.com/api/webhooks/123/456"
        "#;

        let config: General =
            build_config(config).expect("should be able to build minimum configuration");

        let shared = Shared::new(send_notification);
        let notifications_counter = Arc::new(AtomicUsize::new(0));
        let messenger =
            Arc::new(Box::new(TestMessenger::new(notifications_counter.clone())) as BoxedMessenger);
        let cloned_messenger = messenger.clone();
        let shared_clone = shared.clone();
        let service = tokio::spawn(async move {
            let mut service = GeneralService::new(
                shared_clone.clone_with_messenger(Some(cloned_messenger)),
                config,
                notifications_receiver,
            );
            service.run(log, rx).await;
        });

        for _ in 0..NUMBER_OF_MESSAGES {
            shared
                .send_notification
                .try_info(format!("some notification"));
        }

        shutdown
            .send(1)
            .expect("test assert: test service should run when shutdown signal is sent");

        service.await.unwrap();
        assert_eq!(
            notifications_counter.load(Ordering::SeqCst),
            NUMBER_OF_MESSAGES
        );
    }
}
