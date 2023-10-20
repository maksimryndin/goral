pub(crate) mod configuration;
use crate::messenger::configuration::MessengerConfig;

use crate::services::logs::configuration::Logs;
use crate::services::{Data, Service, TaskResult};
use crate::storage::{AppendableLog, Datarow, Datavalue};
use crate::{Sender, Shared};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::time::Duration;
use tokio::io::{self, AsyncBufReadExt, BufReader};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{self};
use tokio::task::{self, JoinHandle};

pub const LOGS_SERVICE_NAME: &str = "logs";

pub(crate) struct LogsService {
    shared: Shared,
    spreadsheet_id: String,
    push_interval: Duration,
    channel_capacity: usize,
    drop_if_contains: Vec<String>,
    messenger_config: Option<MessengerConfig>,
}

impl LogsService {
    pub(crate) fn new(shared: Shared, config: Logs) -> LogsService {
        Self {
            shared,
            spreadsheet_id: config.spreadsheet_id,
            push_interval: Duration::from_secs(config.push_interval_secs.into()),
            drop_if_contains: config.drop_if_contains.unwrap_or(vec![]),
            channel_capacity: 10,
            messenger_config: config.messenger,
        }
    }

    fn process_line(line: String, drop_if_contains: &Vec<String>) -> Data {
        let text = line.trim();
        if text.is_empty() {
            return Data::Empty;
        }
        for substring in drop_if_contains {
            if text.contains(substring) {
                tracing::debug!(
                    "log line {} is dropped because it contains {}",
                    text,
                    substring
                );
                return Data::Empty;
            }
        }
        Data::Single({
            Datarow::new(
                LOGS_SERVICE_NAME.to_string(),
                Utc::now().naive_utc(),
                vec![(format!("log_line"), Datavalue::Text(text.to_string()))],
                None,
            )
        })
    }

    async fn logs_collector(
        sender: mpsc::Sender<TaskResult>,
        send_notification: Sender,
        drop_if_contains: Vec<String>,
    ) {
        tracing::info!("starting stdin logs scraping");
        let cloned_sender = sender.clone();
        let mut stdin = BufReader::new(io::stdin()).lines();
        tokio::pin!(stdin);

        loop {
            tokio::select! {
                _ = sender.closed() => {
                    tracing::info!("finished stdin logs scraping");
                    return;
                },
                res = stdin.next_line() => {
                    let result = match res {
                        Err(e) => {
                            Err(Data::Message(format!("error reading next log line from stdin `{e}`")))
                        },
                        Ok(Some(line)) => {
                            Ok(Self::process_line(line, &drop_if_contains))
                        },
                        Ok(None) => {
                            tracing::info!("finished collecting logs from stdin - no more lines");
                            break;
                        }
                    };
                    match sender.try_send(TaskResult{id: 0, result}) {
                        Err(TrySendError::Full(res)) => {
                            let msg = "log messages queue is full so decrease push interval".to_string();
                            tracing::error!("{}. Cannot send log result `{:?}`", msg, res);
                            send_notification.try_error(msg);
                        },
                        Err(TrySendError::Closed(res)) => {
                            let msg = "log messages queue has been unexpectedly closed".to_string();
                            tracing::error!("{}: cannot send log result {:?}", msg, res);
                            send_notification.fatal(msg).await;
                            panic!("assert: log messages queue shouldn't be closed before shutdown signal");
                        },
                        _ => {},
                    }
                }
            }
        }
        sender.closed().await;
    }

    async fn send_error(&self, message: &str) {
        let message = format!("`{}` while collecting logs from stdin", message);
        if let Some(messenger) = self.shared.messenger.as_ref() {
            let messenger_config = self
                .messenger_config
                .as_ref()
                .expect("assert: if messenger is set, then config is also nonempty");
            if let Err(_) = messenger.send_error(messenger_config, &message).await {
                tracing::error!("failed to send liveness probe output via configured messenger: {:?} for service {}",  messenger_config, self.name());
                self.shared.send_notification.try_error(format!(
                    "{}. Sending via configured messenger failed.",
                    message
                ));
            }
        } else {
            tracing::error!(
                "Messenger is not configured for {}. Error: {}",
                self.name(),
                message,
            );
            self.shared.send_notification.try_error(format!(
                "{}\nMessenger is not configured for {}",
                message,
                self.name(),
            ));
        }
    }
}

#[async_trait]
impl Service for LogsService {
    fn name(&self) -> &str {
        LOGS_SERVICE_NAME
    }

    fn spreadsheet_id(&self) -> &str {
        self.spreadsheet_id.as_str()
    }

    fn channel_capacity(&self) -> usize {
        self.channel_capacity
    }

    fn push_interval(&self) -> Duration {
        self.push_interval
    }

    async fn process_task_result_on_shutdown(
        &mut self,
        result: TaskResult,
        log: &AppendableLog,
    ) -> Data {
        self.process_task_result(result, log).await
    }

    async fn process_task_result(&mut self, result: TaskResult, _log: &AppendableLog) -> Data {
        let TaskResult { result, .. } = result;
        match result {
            Ok(data) => data,
            Err(Data::Message(msg)) => {
                tracing::error!("{}", msg);
                self.send_error(&msg).await;
                Data::Empty
            }
            _ => panic!("assert: system result contains either single datarow or error text"),
        }
    }

    async fn spawn_tasks(&mut self, sender: mpsc::Sender<TaskResult>) -> Vec<JoinHandle<()>> {
        let sender = sender.clone();
        let send_notification = self.shared.send_notification.clone();
        let drop_if_contains = self.drop_if_contains.clone();
        vec![tokio::spawn(async move {
            Self::logs_collector(sender, send_notification, drop_if_contains).await;
        })]
    }
}
