pub(crate) mod general;
pub(crate) mod healthcheck;
pub(crate) mod http_client;
pub(crate) mod kv;
pub(crate) mod logs;
pub(crate) mod metrics;
pub(crate) mod system;

use crate::messenger::configuration::MessengerConfig;
use crate::rules::{Action, Rule, RuleCondition, RuleOutput, Triggered};
use crate::spreadsheet::datavalue::{Datarow, Datavalue};
use crate::storage::AppendableLog;
use crate::{jitter_duration, BoxedMessenger, Notification, Sender, Shared};
use async_trait::async_trait;
use futures::future::try_join_all;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::task::JoinHandle;
use tracing::Level;

#[derive(Debug, Clone)]
pub enum Data {
    Empty,
    Single(Datarow),
    Many(Vec<Datarow>),
    Message(String),
}

impl Data {
    pub(crate) fn into_iter(self) -> DataIntoIter {
        match self {
            Data::Empty | Data::Message(_) => DataIntoIter {
                next: None,
                many: vec![],
                is_single: true,
            },
            Data::Single(row) => DataIntoIter {
                next: Some(row),
                many: vec![],
                is_single: true,
            },
            Data::Many(rows) => DataIntoIter {
                next: None,
                many: rows,
                is_single: false,
            },
        }
    }
}

pub struct DataIntoIter {
    next: Option<Datarow>,
    many: Vec<Datarow>,
    is_single: bool,
}

impl Iterator for DataIntoIter {
    type Item = Datarow;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_single {
            self.next.take()
        } else {
            self.many.pop()
        }
    }
}

#[derive(Debug)]
pub struct TaskResult {
    id: usize,
    result: Result<Data, Data>,
}

// TODO perhaps rayon with par_iter
fn process_rules(
    is_shutdown: Arc<AtomicBool>,
    send_notification: Sender,
    mut input_rx: mpsc::Receiver<RulePayload>,
    output_tx: mpsc::Sender<Triggered>,
    service_name: String,
) {
    tracing::info!(
        "started rules processing thread for service {}",
        service_name
    );

    let RulePayload::Rules(mut rules) = input_rx
        .try_recv()
        .expect("assert: rules are provided at service initialization")
    else {
        panic!("assert: rules are provided at service initialization as a first message");
    };
    while let Some(payload) = input_rx.blocking_recv() {
        match payload {
            RulePayload::Rules(updated_rules) => {
                rules = updated_rules;
                continue;
            }
            RulePayload::Data(data) => {
                for row in data.into_iter() {
                    let applicant = row.into();
                    for rule in &rules {
                        match rule.apply(&applicant) {
                            RuleOutput::SkipFurtherRules => {
                                break;
                            }
                            RuleOutput::Process(Some(triggered)) => {
                                match output_tx.try_send(triggered) {
                                    Err(TrySendError::Full(triggered)) => {
                                        let msg = format!(
                                            "rules output messages queue is full for service {}",
                                            service_name
                                        );
                                        tracing::error!(
                                            "{}. Cannot send rules application output: {:#?}",
                                            msg,
                                            triggered
                                        );
                                        send_notification.try_error(msg);
                                    }
                                    Err(TrySendError::Closed(triggered)) => {
                                        if is_shutdown.load(Ordering::Relaxed) {
                                            return;
                                        }
                                        let msg = format!(
                                            "rules output messages queue has been unexpectedly closed for service {}",
                                            service_name
                                        );
                                        tracing::error!(
                                            "{}: cannot send rules application output: {:#?}",
                                            msg,
                                            triggered
                                        );
                                        panic!("assert: rules output messages queue shouldn't be closed before shutdown signal");
                                    }
                                    _ => {}
                                }
                            }
                            _ => {
                                continue;
                            }
                        }
                    }
                }
            }
        }
    }
    tracing::info!(
        "exiting rules processing thread for service {}",
        service_name
    );
}

async fn rules_notifications(
    messenger: Sender,
    mut output_receiver: mpsc::Receiver<Triggered>,
    base_url: String,
) {
    while let Some(triggered) = output_receiver.recv().await {
        use Action::*;
        let level = match triggered.action {
            Info => Level::INFO,
            Warn => Level::WARN,
            Error => Level::ERROR,
            SkipFurtherRules => panic!("assert: `skip further rules` action shouldn't be sent"),
        };
        let message = format!(
            "```{}``` [spreadsheet]({}{}) may be created a bit later",
            triggered.message, base_url, triggered.sheet_id
        );
        messenger.send(Notification::new(message, level)).await;
    }
}

async fn messenger_queue(
    messenger: Arc<BoxedMessenger>,
    messenger_config: MessengerConfig,
    send_notification: Sender,
    mut rx: mpsc::Receiver<Notification>,
    host_id: String,
    service: &'static str,
) {
    while let Some(notification) = rx.recv().await {
        let Notification { message, level } = notification;
        if messenger
            .send_by_level(&messenger_config, &format!("*{host_id}*: {message}"), level)
            .await
            .is_err()
        {
            tracing::error!(
                "failed to send notification via configured messenger: `{:?}` for service {}",
                messenger_config,
                service
            );
            send_notification.try_error(format!(
                "{}. Sending via configured messenger failed.",
                message
            ));
        }
    }
}

pub enum RulePayload {
    Rules(Vec<Rule>),
    Data(Data),
}

#[async_trait]
pub trait Service: Send + Sync {
    fn name(&self) -> &'static str;

    fn spreadsheet_id(&self) -> &str;

    fn channel_capacity(&self) -> usize {
        // Default for General service where the channel is not actually used
        1
    }

    fn push_interval(&self) -> Duration {
        // Default for General and KV services
        Duration::from_secs(u64::MAX)
    }

    fn rules_update_interval(&self) -> Duration {
        Duration::from_secs(15) + jitter_duration()
    }

    fn get_example_rules(&self) -> Vec<Datarow> {
        let row: Datarow = Rule {
            log_name: "log_name (everything up to the first @ in a sheet title)".to_string(),
            key: "key (column header)".to_string(),
            condition: RuleCondition::Contains,
            value: Datavalue::Text("substring".to_string()),
            action: Action::SkipFurtherRules,
        }
        .into();
        vec![row]
    }

    fn shared(&self) -> &Shared;

    fn messenger(&self) -> Option<Sender> {
        None
    }

    fn messenger_config(&self) -> Option<&MessengerConfig> {
        None
    }

    fn take_messenger_rx(&mut self) -> Option<mpsc::Receiver<Notification>> {
        None
    }

    fn truncate_at(&self) -> f32;

    async fn prerun_hook(&self, log: &AppendableLog) {
        if let Err(e) = log.healthcheck().await {
            let msg = format!(
                "service `{}` failed to connect to Google API: `{:?}`",
                self.name(),
                e
            );
            tracing::error!("{}", msg);
            self.shared().send_notification.fatal(msg.clone()).await;
            panic!("{}", msg);
        }
        let msg = format!(
            "service `{0}` is running with spreadsheet [{1}]({1})",
            self.name(),
            log.spreadsheet_baseurl(),
        );
        tracing::info!("{}", msg);
        self.shared().send_notification.try_info(msg);
        tracing::debug!(
            "channel capacity `{}` for service `{}`",
            self.channel_capacity(),
            self.name()
        );
    }

    async fn send_error(&self, message: String) {
        if let Some(messenger) = self.messenger() {
            messenger
                .send(Notification::new(message, Level::ERROR))
                .await;
        } else {
            tracing::error!(
                "Messenger is not configured for {}. Error: {}",
                self.name(),
                message,
            );
            self.shared().send_notification.try_error(format!(
                "{}\nMessenger is not configured for {}",
                message,
                self.name(),
            ));
        }
    }

    async fn process_task_result_on_shutdown(
        &mut self,
        result: TaskResult,
        log: &AppendableLog,
    ) -> Data {
        self.process_task_result(result, log).await
    }

    async fn process_task_result(&mut self, _result: TaskResult, _log: &AppendableLog) -> Data {
        Data::Empty
    }

    async fn spawn_tasks(
        &mut self,
        _shutdown: Arc<AtomicBool>,
        _sender: mpsc::Sender<TaskResult>,
    ) -> Vec<JoinHandle<()>> {
        // Default for General service
        std::future::pending::<()>().await;
        vec![]
    }

    async fn send_for_rule_processing(
        &self,
        log: &AppendableLog,
        data: &mut Data,
        input_tx: &mut mpsc::Sender<RulePayload>,
    ) {
        if self.shared().messenger.is_none() {
            return;
        }
        let service_name = self.name();
        let host_id = log.host_id();
        match data {
            Data::Empty | Data::Message(_) => {}
            Data::Single(ref mut datarow) => {
                datarow.sheet_id(host_id, service_name);
            }
            Data::Many(ref mut datarows) => {
                datarows.iter_mut().for_each(|d| {
                    d.sheet_id(host_id, service_name);
                });
            }
        }
        match input_tx.try_send(RulePayload::Data(data.clone())) {
            Err(TrySendError::Full(_)) => {
                let msg = format!(
                    "rules input messages queue is full for service {}",
                    self.name()
                );
                tracing::error!("{}. Cannot send data rows for rules application", msg);
                self.shared().send_notification.try_error(msg);
            }
            Err(TrySendError::Closed(_)) => {
                let msg = format!(
                    "rules input messages queue has been unexpectedly closed for service {}",
                    self.name()
                );
                tracing::error!("{}: cannot send data rows for rules application", msg);
                self.shared().send_notification.fatal(msg).await;
                panic!(
                    "assert: rules input messages queue shouldn't be closed before shutdown signal"
                );
            }
            _ => {}
        }
    }

    async fn send_updated_rules(
        &self,
        log: &AppendableLog,
        input_tx: &mut mpsc::Sender<RulePayload>,
    ) {
        if self.shared().messenger.is_none() {
            return;
        }
        let rules = match log.get_rules().await {
            Ok(rules) => rules,
            Err(e) => {
                let msg = format!(
                    "failed to fetch rules for service `{}`: `{}`",
                    self.name(),
                    e
                );
                tracing::error!("{}", msg);
                self.shared().send_notification.try_error(msg);
                return;
            }
        };

        match input_tx.try_send(RulePayload::Rules(rules)) {
            Err(TrySendError::Full(_)) => {
                let msg = format!(
                    "rules input messages queue is full for service {}",
                    self.name()
                );
                tracing::error!("{}. Cannot send rules update", msg);
                self.shared().send_notification.try_error(msg);
            }
            Err(TrySendError::Closed(_)) => {
                let msg = format!(
                    "rules input messages queue has been unexpectedly closed for service {}",
                    self.name()
                );
                tracing::error!("{}: cannot send rules update", msg);
                self.shared().send_notification.fatal(msg).await;
                panic!(
                    "assert: rules input messages queue shouldn't be closed before shutdown signal"
                );
            }
            _ => {}
        }
    }

    async fn start_rules_thread(
        &self,
        is_shutdown: Arc<AtomicBool>,
        log: &mut AppendableLog,
    ) -> (mpsc::Sender<RulePayload>, mpsc::Receiver<Triggered>) {
        if self.shared().messenger.is_none() {
            let (input_tx, _) = mpsc::channel(1);
            let (_, output_rx) = mpsc::channel(1);
            return (input_tx, output_rx);
        }
        //  channel to collect rule applicants and rules updates for rules processing
        let (input_tx, input_rx) = mpsc::channel(2 * self.channel_capacity());
        //  channel to collect rule output triggers
        let (output_tx, output_rx) = mpsc::channel(2 * self.channel_capacity());
        let example_rules = self.get_example_rules();
        let _ = log.append(example_rules).await;
        let rules = log
            .get_rules()
            .await
            .expect("assert: can fetch rules at the start");
        input_tx
            .send(RulePayload::Rules(rules))
            .await
            .expect("assert: can send rules at initialization");
        let service_name = self.name().to_string();
        let send_notification = self.shared().send_notification.clone();
        std::thread::Builder::new()
            .name(format!("rules-processor-{}", self.name()))
            .spawn(move || {
                process_rules(
                    is_shutdown,
                    send_notification,
                    input_rx,
                    output_tx,
                    service_name,
                );
            })
            .expect("assert: can spawn rule processing thread");
        (input_tx, output_rx)
    }

    async fn run(&mut self, mut log: AppendableLog, mut shutdown: broadcast::Receiver<u16>) {
        self.prerun_hook(&log).await;
        //  channel to collect results
        let (tx, mut data_receiver) = mpsc::channel(2 * self.channel_capacity());
        let is_shutdown = Arc::new(AtomicBool::new(false));
        let (mut rules_input, rules_output) =
            self.start_rules_thread(is_shutdown.clone(), &mut log).await;
        let mut tasks = self.spawn_tasks(is_shutdown.clone(), tx).await;

        if let Some(message_rx) = self.take_messenger_rx() {
            let messenger =
                self.shared().messenger.clone().expect(
                    "assert: if messenger receiver channel is set then the messenger is set",
                );
            let messenger_config = self
                .messenger_config()
                .expect(
                    "assert: if messenger receiver channel is set then the messenger config is set",
                )
                .clone();
            let send_notification = self.shared().send_notification.clone();
            let service = self.name();
            let host_id = log.host_id().to_string();
            tasks.push(tokio::spawn(async move {
                messenger_queue(
                    messenger,
                    messenger_config,
                    send_notification,
                    message_rx,
                    host_id,
                    service,
                )
                .await
            }));
        }

        if let Some(message_tx) = self.messenger() {
            let base_url = log.spreadsheet_baseurl();
            tasks.push(tokio::spawn(async move {
                rules_notifications(message_tx, rules_output, base_url).await
            }));
        }
        let tasks = try_join_all(tasks);
        tokio::pin!(tasks);
        let mut push_interval = tokio::time::interval(self.push_interval());
        let mut rules_update_interval = tokio::time::interval(self.rules_update_interval());
        let mut accumulated_data = vec![];
        loop {
            tokio::select! {
                result = shutdown.recv() => {
                    is_shutdown.store(true, Ordering::Release);
                    data_receiver.close(); // stop tasks
                    let graceful_shutdown_timeout = match result {
                        Err(_) => panic!("assert: shutdown signal sender should be dropped after all service listeneres"),
                        Ok(graceful_shutdown_timeout) => graceful_shutdown_timeout,
                    };
                    tracing::info!("{} service has got shutdown signal", self.name());
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(graceful_shutdown_timeout.into())) => {
                            tracing::error!("{} service has badly shutdowned. Some data is lost.", self.name());
                        },
                        _ = async {
                            // drain remaining messages
                            while let Some(task_result) = data_receiver.recv().await {
                                let data = self.process_task_result_on_shutdown(task_result, &log).await;
                                match data {
                                    Data::Empty | Data::Message(_) => {},
                                    Data::Single(datarow) => {accumulated_data.push(datarow);}
                                    Data::Many(mut datarows) => {accumulated_data.append(&mut datarows);}
                                }
                            }
                            let _ = log.append(accumulated_data).await;
                        } => {
                            tracing::info!("{} service has successfully shutdowned", self.name());
                        }
                    }
                    return;
                },
                _ = push_interval.tick() => {
                    let example_rules = self.get_example_rules();
                    accumulated_data.extend(example_rules);
                    let _ = log.append(accumulated_data).await;
                    accumulated_data = vec![];
                },
                _ = rules_update_interval.tick() => {
                    self.send_updated_rules(&log, &mut rules_input).await;
                }
                Some(task_result) = data_receiver.recv() => {
                    let mut data = self.process_task_result(task_result, &log).await;
                    self.send_for_rule_processing(&log, &mut data, &mut rules_input).await;
                    match data {
                        Data::Empty | Data::Message(_) => {},
                        Data::Single(datarow) => {accumulated_data.push(datarow);}
                        Data::Many(mut datarows) => {accumulated_data.append(&mut datarows);}
                    }
                }
                res = &mut tasks => {
                    res.unwrap(); // propagate panics from spawned tasks
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::ceiled_division;
    use crate::services::general::GENERAL_SERVICE_NAME;
    use crate::spreadsheet::datavalue::Datavalue;
    use crate::spreadsheet::spreadsheet::SpreadsheetAPI;
    use crate::spreadsheet::tests::TestState;
    use crate::spreadsheet::Metadata;
    use crate::storage::Storage;
    use crate::tests::TEST_HOST_ID;
    use crate::Sender;

    use chrono::Utc;

    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tokio::sync::{
        broadcast,
        mpsc::{self, error::TrySendError},
    };

    struct TestService {
        counter: Arc<AtomicUsize>,
        shared: Shared,
    }

    const APPEND_DURATION_MS: u64 = 100;
    const SCRAPE_INTERVAL_MS: u64 = 20;

    #[async_trait]
    impl Service for TestService {
        fn name(&self) -> &'static str {
            GENERAL_SERVICE_NAME
        }

        fn spreadsheet_id(&self) -> &str {
            "spreadsheet1"
        }

        fn channel_capacity(&self) -> usize {
            // during appending we accumulate
            100 * ceiled_division(APPEND_DURATION_MS as u16, SCRAPE_INTERVAL_MS as u16) as usize
        }

        fn push_interval(&self) -> Duration {
            Duration::from_millis(50)
        }

        fn shared(&self) -> &Shared {
            &self.shared
        }

        fn truncate_at(&self) -> f32 {
            100.0
        }

        async fn process_task_result(&mut self, result: TaskResult, _log: &AppendableLog) -> Data {
            let TaskResult { result, .. } = result;
            result.expect("test assert: ok result is sent")
        }

        async fn spawn_tasks(
            &mut self,
            is_shutdown: Arc<AtomicBool>,
            sender: mpsc::Sender<TaskResult>,
        ) -> Vec<JoinHandle<()>> {
            let counter = self.counter.clone();
            vec![tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(SCRAPE_INTERVAL_MS));
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            let result = Ok(Data::Single(
                                Datarow::new(
                                    "log_name1".to_string(),
                                    Utc::now().naive_utc(),
                                    vec![
                                        (format!("key11"), Datavalue::HeatmapPercent(3_f64)),
                                        (format!("key12"), Datavalue::Size(400_u64)),
                                    ],
                                ),
                            ));

                            match sender.try_send(TaskResult{id: 0, result}) {
                                Err(TrySendError::Full(_)) => {
                                    panic!("test assert: messages queue shouldn't be full");
                                },
                                Err(TrySendError::Closed(_)) => {
                                    if is_shutdown.load(Ordering::Relaxed) {
                                        return;
                                    }
                                    panic!("test assert: messages queue shouldn't be closed before shutdown signal");
                                },
                                _ => {counter.fetch_add(1, Ordering::SeqCst);},
                            };
                        }
                    }
                }
            })]
        }
    }

    #[tokio::test]
    async fn basic_service_flow() {
        let (tx, mut rx) = mpsc::channel(1);
        tokio::task::spawn(async move {
            while let Some(msg) = rx.recv().await {
                println!("message {msg:?}");
            }
        });
        let tx = Sender::new(tx, GENERAL_SERVICE_NAME);
        let data_counter = Arc::new(AtomicUsize::new(0));
        let sheets_api = SpreadsheetAPI::new(
            tx.clone(),
            TestState::new(
                vec![],
                None,
                Some(APPEND_DURATION_MS / 2), // because append is 2 Google api calls
            ),
        );
        let storage = Arc::new(Storage::new(
            TEST_HOST_ID.to_string(),
            sheets_api,
            tx.clone(),
        ));

        let (shutdown, rx) = broadcast::channel(1);

        let counter = data_counter.clone();
        let shared = Shared {
            messenger: None,
            send_notification: tx.clone(),
        };
        let mut service = TestService { counter, shared };
        let log = AppendableLog::new(
            storage.clone(),
            "spreadsheet1".to_string(),
            GENERAL_SERVICE_NAME.to_string(),
            Some(tx.clone()),
            service.truncate_at(),
        );
        let service = tokio::spawn(async move {
            service.run(log, rx).await;
        });
        tokio::spawn(async move {
            let work_duration = jitter_duration();
            tracing::info!("will work for {:?}", work_duration);
            tokio::time::sleep(work_duration).await;
            shutdown
                .send(1)
                .expect("test assert: test service should run when shutdown signal is sent");
            tracing::info!("shutdown signal is sent");
        });

        service.await.unwrap();

        let all_sheets = storage
            .google()
            .sheets_filtered_by_metadata("spreadsheet1", &Metadata::new(vec![]))
            .await
            .unwrap();
        assert_eq!(
            all_sheets.len(),
            2,
            "only 2 sheets (for `log_name1` and rules) should be created"
        );
        assert!(
            all_sheets[0].title().contains("log_name1")
                || all_sheets[1].title().contains("log_name1")
        );
        assert!(all_sheets[0].title().contains("rules") || all_sheets[1].title().contains("rules"));
        let row_count = if all_sheets[0].title().contains("log_name1") {
            all_sheets[0].row_count()
        } else {
            all_sheets[1].row_count()
        };
        assert_eq!(
            row_count,
            Some(data_counter.load(Ordering::SeqCst) as i32 + 1), // one row for header row
            "`log_name1` contains all data generated by the service"
        );
    }
}
