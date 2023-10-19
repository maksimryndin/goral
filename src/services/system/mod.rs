pub(crate) mod collector;
pub(crate) mod configuration;
use crate::messenger::configuration::MessengerConfig;
use crate::services::system::configuration::{scrape_push_rule, System};
use crate::services::{Data, Service, TaskResult};
use crate::storage::AppendableLog;
use crate::{Sender, Shared};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::time::Duration;
use tokio::sync::mpsc::{self};
use tokio::task::{self, JoinHandle};

pub const SYSTEM_SERVICE_NAME: &str = "system";

pub(crate) struct SystemService {
    shared: Shared,
    spreadsheet_id: String,
    push_interval: Duration,
    scrape_interval: Duration,
    scrape_timeout: Duration,
    mounts: Vec<String>,
    process_names: Vec<String>,
    channel_capacity: usize,
    error_previous_state: Option<String>,
    messenger_config: Option<MessengerConfig>,
    alert_if_cpu_usage_percent_more_than: Option<u8>,
    alert_if_free_disk_space_percent_less_than: Option<u8>,
    alert_if_free_memory_percent_less_than: Option<u8>,
}

impl SystemService {
    pub(crate) fn new(shared: Shared, config: System) -> SystemService {
        let channel_capacity = scrape_push_rule(
            &config.scrape_timeout_ms,
            &config.scrape_interval_secs,
            &config.push_interval_secs,
        )
        .expect("assert: push/scrate ratio is validated at configuration");
        Self {
            shared,
            spreadsheet_id: config.spreadsheet_id,
            push_interval: Duration::from_secs(config.push_interval_secs.into()),
            scrape_interval: Duration::from_secs(config.scrape_interval_secs.into()),
            scrape_timeout: Duration::from_millis(config.scrape_timeout_ms.into()),
            mounts: config.mounts,
            process_names: config.process_names,
            channel_capacity,
            error_previous_state: None,
            messenger_config: config.messenger,
            alert_if_cpu_usage_percent_more_than: config.alert_if_cpu_usage_percent_more_than,
            alert_if_free_disk_space_percent_less_than: config
                .alert_if_free_disk_space_percent_less_than,
            alert_if_free_memory_percent_less_than: config.alert_if_free_memory_percent_less_than,
        }
    }

    fn collect_sysinfo(
        sender: mpsc::Sender<TaskResult>,
        mut request_rx: mpsc::Receiver<DateTime<Utc>>,
        mounts: Vec<String>,
        names: Vec<String>,
    ) {
        let mut sys = collector::initialize();
        tracing::info!("started system info scraping thread");

        while let Some(scrape_time) = request_rx.blocking_recv() {
            let result = collector::collect(&mut sys, &mounts, &names, scrape_time.naive_utc())
                .map(|datarows| Data::Many(datarows))
                .map_err(|t| Data::Message(t));
            if let Err(_) = sender.blocking_send(TaskResult { id: 0, result }) {
                break;
            }
        }
        tracing::info!("exiting system info scraping thread");
    }

    async fn make_timed_scrape(
        request_tx: &mut mpsc::Sender<DateTime<Utc>>,
        scrape_timeout: Duration,
        scrape_time: DateTime<Utc>,
    ) -> Result<(), String> {
        tokio::select! {
            _ = tokio::time::sleep(scrape_timeout) => Err(format!("sysinfo scrape timeout {:?}", scrape_timeout)),
            res = request_tx.send(scrape_time) => res.map_err(|e| e.to_string())
        }
    }

    async fn sys_observer(
        scrape_interval: Duration,
        scrape_timeout: Duration,
        sender: mpsc::Sender<TaskResult>,
        send_notification: Sender,
        mounts: Vec<String>,
        names: Vec<String>,
    ) {
        let mut interval = tokio::time::interval(scrape_interval);
        let (mut tx, rx) = mpsc::channel::<DateTime<Utc>>(1);
        tracing::info!("starting system info scraping");
        let cloned_sender = sender.clone();
        let join_result =
            task::spawn_blocking(move || Self::collect_sysinfo(cloned_sender, rx, mounts, names));
        tokio::pin!(join_result);

        loop {
            tokio::select! {
                _ = sender.closed() => {
                    tracing::info!("finished system scraping");
                    return;
                },
                res = &mut join_result => {
                    match res {
                        Err(e) => {
                            let msg = "failed to spawn sysinfo collection in a separate thread".to_string();
                            tracing::error!("{}: `{}`", msg, e);
                            send_notification.fatal(msg).await;
                            panic!("assert: should be able to spawn blocking tasks");
                        },
                        Ok(_) => {
                            tracing::info!("finished collecting sysinfo");
                        },
                    };
                }
                _ = interval.tick() => {
                    let scrape_time = Utc::now();
                    if let Err(e) = Self::make_timed_scrape(&mut tx, scrape_timeout, scrape_time).await {
                        let msg = format!("error sending request for sysinfo `{}`", e);
                        tracing::error!("{}", msg);
                        send_notification.try_error(msg);
                    }
                }
            }
        }
    }

    async fn send_error(&self, message: &str) {
        let message = format!("`{}` while scraping sysinfo", message);
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
impl Service for SystemService {
    fn name(&self) -> &str {
        SYSTEM_SERVICE_NAME
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
                if self.error_previous_state.is_none()
                    || self.error_previous_state.as_ref() != Some(&msg)
                {
                    self.send_error(&msg).await;
                    self.error_previous_state = Some(msg);
                }
                Data::Empty
            }
            _ => panic!("assert: system result contains either many datarows or error text"),
        }
    }

    async fn spawn_tasks(&mut self, sender: mpsc::Sender<TaskResult>) -> Vec<JoinHandle<()>> {
        let sender = sender.clone();
        let send_notification = self.shared.send_notification.clone();
        let mounts = self.mounts.clone();
        let names = self.process_names.clone();
        let scrape_interval = self.scrape_interval.clone();
        let scrape_timeout = self.scrape_timeout.clone();
        vec![tokio::spawn(async move {
            Self::sys_observer(
                scrape_interval,
                scrape_timeout,
                sender,
                send_notification,
                mounts,
                names,
            )
            .await;
        })]
    }
}
