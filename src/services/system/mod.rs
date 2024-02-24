pub(crate) mod collector;
pub(crate) mod configuration;
use crate::google::datavalue::{Datarow, Datavalue};
use crate::messenger::configuration::MessengerConfig;
use crate::notifications::{MessengerApi, Notification, Sender};
use crate::rules::{Action, Rule, RuleCondition};
use crate::services::system::configuration::{scrape_push_rule, System};
use crate::services::{Data, Service, TaskResult};
use crate::storage::AppendableLog;
use crate::Shared;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::mpsc::{self};
use tokio::task::JoinHandle;

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
    messenger: Option<MessengerApi>,
    truncate_at: f32,
}

impl SystemService {
    pub(crate) fn new(shared: Shared, mut config: System) -> SystemService {
        let channel_capacity = scrape_push_rule(
            &config.scrape_timeout_ms,
            &config.scrape_interval_secs,
            &config.push_interval_secs,
        )
        .expect("assert: push/scrate ratio is validated at configuration");
        let messenger = config
            .messenger
            .take()
            .map(|messenger_config| MessengerApi::new(messenger_config, SYSTEM_SERVICE_NAME));
        Self {
            shared,
            spreadsheet_id: config.spreadsheet_id,
            push_interval: Duration::from_secs(config.push_interval_secs.into()),
            scrape_interval: Duration::from_secs(config.scrape_interval_secs.into()),
            scrape_timeout: Duration::from_millis(config.scrape_timeout_ms.into()),
            mounts: config.mounts,
            process_names: config.process_names,
            channel_capacity,
            messenger,
            truncate_at: config.autotruncate_at_usage_percent,
        }
    }

    fn collect_sysinfo(
        is_shutdown: Arc<AtomicBool>,
        sender: mpsc::Sender<TaskResult>,
        mut request_rx: mpsc::Receiver<DateTime<Utc>>,
        mounts: Vec<String>,
        names: Vec<String>,
        messenger: Sender,
    ) {
        let mut sys = collector::initialize();
        tracing::info!("started system info scraping thread");

        while let Some(scrape_time) = request_rx.blocking_recv() {
            let result = collector::collect(
                &mut sys,
                &mounts,
                &names,
                scrape_time.naive_utc(),
                &messenger,
            )
            .map(Data::Many)
            .map_err(|e| Data::Message(format!("sysinfo scraping error {e}")));
            if sender.blocking_send(TaskResult { id: 0, result }).is_err() {
                if is_shutdown.load(Ordering::Relaxed) {
                    return;
                }
                panic!("assert: sysinfo messages queue shouldn't be closed before shutdown signal");
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

    #[allow(clippy::too_many_arguments)]
    async fn sys_observer(
        is_shutdown: Arc<AtomicBool>,
        scrape_interval: Duration,
        scrape_timeout: Duration,
        sender: mpsc::Sender<TaskResult>,
        messenger: Sender,
        send_notification: Sender,
        mounts: Vec<String>,
        names: Vec<String>,
    ) {
        let mut interval = tokio::time::interval(scrape_interval);
        let (mut tx, rx) = mpsc::channel::<DateTime<Utc>>(1);
        tracing::info!("starting system info scraping");
        let cloned_sender = sender.clone();
        let cloned_is_shutdown = is_shutdown.clone();
        std::thread::Builder::new()
            .name("sysinfo-collector".into())
            .spawn(move || {
                Self::collect_sysinfo(
                    cloned_is_shutdown,
                    cloned_sender,
                    rx,
                    mounts,
                    names,
                    messenger,
                )
            })
            .expect("assert: can spawn sysinfo collecting thread");

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let scrape_time = Utc::now();
                    if let Err(e) = Self::make_timed_scrape(&mut tx, scrape_timeout, scrape_time).await {
                        if is_shutdown.load(Ordering::Relaxed) {
                            tracing::info!("finished sysinfo collection");
                            return;
                        }
                        let msg = format!("error sending request for sysinfo `{}`", e);
                        tracing::error!("{}", msg);
                        send_notification.fatal(msg).await;
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Service for SystemService {
    fn name(&self) -> &'static str {
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

    fn get_example_rules(&self) -> Vec<Datarow> {
        let mut rows = Vec::with_capacity(2 + self.mounts.len() + 2 * self.process_names.len());
        rows.push(
            Rule {
                log_name: collector::BASIC_LOG.to_string(),
                key: collector::MEMORY_USE.to_string(),
                condition: RuleCondition::Greater,
                value: Datavalue::Percent(90.),
                action: Action::Warn,
            }
            .into(),
        );
        rows.push(
            Rule {
                log_name: collector::BASIC_LOG.to_string(),
                key: collector::SWAP_USE.to_string(),
                condition: RuleCondition::Greater,
                value: Datavalue::Percent(90.),
                action: Action::Warn,
            }
            .into(),
        );
        for mount in &self.mounts {
            rows.push(
                Rule {
                    log_name: mount.to_string(),
                    key: collector::DISK_USE.to_string(),
                    condition: RuleCondition::Greater,
                    value: Datavalue::Percent(90.),
                    action: Action::Warn,
                }
                .into(),
            );
        }
        for process_name in &self.process_names {
            rows.push(
                Rule {
                    log_name: process_name.to_string(),
                    key: collector::MEMORY_USE.to_string(),
                    condition: RuleCondition::Greater,
                    value: Datavalue::Percent(90.),
                    action: Action::Warn,
                }
                .into(),
            );
            rows.push(
                Rule {
                    log_name: process_name.to_string(),
                    key: collector::CPU.to_string(),
                    condition: RuleCondition::Greater,
                    value: Datavalue::Percent(90.),
                    action: Action::Warn,
                }
                .into(),
            );
        }
        rows
    }

    fn shared(&self) -> &Shared {
        &self.shared
    }

    fn messenger(&self) -> Option<Sender> {
        self.messenger.as_ref().map(|m| m.message_tx.clone())
    }

    fn messenger_config(&self) -> Option<&MessengerConfig> {
        self.messenger.as_ref().map(|m| &m.config)
    }

    fn take_messenger_rx(&mut self) -> Option<mpsc::Receiver<Notification>> {
        self.messenger.as_mut().and_then(|m| m.message_rx.take())
    }

    fn truncate_at(&self) -> f32 {
        self.truncate_at
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
                self.send_error(format!("`{}` while scraping sysinfo", msg))
                    .await;
                Data::Empty
            }
            _ => panic!("assert: system result contains either many datarows or error text"),
        }
    }

    async fn spawn_tasks(
        &mut self,
        is_shutdown: Arc<AtomicBool>,
        sender: mpsc::Sender<TaskResult>,
    ) -> Vec<JoinHandle<()>> {
        let is_shutdown = is_shutdown.clone();
        let sender = sender.clone();
        let send_notification = self.shared.send_notification.clone();
        let messenger = self
            .messenger()
            .unwrap_or(self.shared.send_notification.clone());
        let mounts = self.mounts.clone();
        let names = self.process_names.clone();
        let scrape_interval = self.scrape_interval;
        let scrape_timeout = self.scrape_timeout;
        vec![tokio::spawn(async move {
            Self::sys_observer(
                is_shutdown,
                scrape_interval,
                scrape_timeout,
                sender,
                messenger,
                send_notification,
                mounts,
                names,
            )
            .await;
        })]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::google::datavalue::Datavalue;
    use crate::notifications::Notification;
    use tracing::Level;

    #[tokio::test]
    async fn single_sys_scrape() {
        const NUM_OF_SCRAPES: usize = 1;
        let (send_notification, mut notifications_receiver) = mpsc::channel(1);
        let send_notification = Sender::new(send_notification, "test");
        let (data_sender, mut data_receiver) = mpsc::channel(NUM_OF_SCRAPES);
        let is_shutdown = Arc::new(AtomicBool::new(false));

        let notifications = tokio::spawn(async move {
            while let Some(notification) = notifications_receiver.recv().await {
                println!("Notification received: {notification:?}");
            }
        });

        let is_shutdown_clone = is_shutdown.clone();
        let scrape_handle = tokio::spawn(async move {
            SystemService::sys_observer(
                is_shutdown_clone,
                Duration::from_secs(1),
                Duration::from_secs(2),
                data_sender,
                send_notification.clone(),
                send_notification,
                vec![],
                vec![],
            )
            .await;
        });

        tokio::time::sleep(Duration::from_secs(2 * NUM_OF_SCRAPES as u64)).await;
        is_shutdown.store(true, Ordering::Release);
        data_receiver.close();

        if let Some(TaskResult {
            result: Ok(Data::Many(datarows)),
            ..
        }) = data_receiver.recv().await
        {
            if let Some(Datavalue::HeatmapPercent(mem_use)) =
                datarows[0].keys_values().get("memory_use")
            {
                assert!(*mem_use > 0.0, "memory usage should be positive");
            } else {
                panic!("test assert: memory use should be scraped");
            }
        } else {
            panic!("test assert: at least one successfull scrape should be collected");
        }

        scrape_handle.await.unwrap(); // scrape should finish as the data channel is closed
        notifications.await.unwrap();
    }

    #[tokio::test]
    async fn sys_scrape_timeout() {
        let (send_notification, mut notifications_receiver) = mpsc::channel(1);
        let send_notification = Sender::new(send_notification, SYSTEM_SERVICE_NAME);
        let (data_sender, mut data_receiver) = mpsc::channel(1);
        let is_shutdown = Arc::new(AtomicBool::new(false));

        let notification = tokio::spawn(async move { notifications_receiver.recv().await });

        let is_shutdown_clone = is_shutdown.clone();
        let scrape_handle = tokio::spawn(async move {
            SystemService::sys_observer(
                is_shutdown_clone,
                Duration::from_secs(1),
                Duration::from_millis(1),
                data_sender,
                send_notification.clone(),
                send_notification,
                vec![],
                vec![],
            )
            .await;
        });
        if let Some(Notification { message, level }) = notification.await.unwrap() {
            assert!(message.contains("sysinfo scrape timeout"));
            assert_eq!(level, Level::ERROR);
        } else {
            panic!("test assert: at least one timeout should be happen");
        }

        is_shutdown.store(true, Ordering::Release);
        data_receiver.close();
        scrape_handle.await.unwrap(); // scrape should finish as the data channel is closed
    }
}
