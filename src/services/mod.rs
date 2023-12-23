pub(crate) mod general;
pub(crate) mod healthcheck;
pub(crate) mod kv;
pub(crate) mod logs;
pub(crate) mod metrics;
pub(crate) mod system;

use crate::configuration::APP_NAME;
use crate::messenger::configuration::MessengerConfig;
use crate::rules::{Action, Rule, RuleCondition, RuleOutput, Triggered};
use crate::spreadsheet::datavalue::{Datarow, Datavalue};
use crate::storage::AppendableLog;
use crate::{BoxedMessenger, HttpsClient, Notification, Sender, Shared};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::future::try_join_all;
use hyper::{
    body::{Body, HttpBody as _},
    header, Client, Request, StatusCode, Uri,
};
use lazy_static::lazy_static;
use regex::Regex;
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
        if let Err(_) = messenger
            .send_by_level(&messenger_config, &format!("*{host_id}*: {message}"), level)
            .await
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
        Duration::from_secs(15)
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
        let rules = log.get_rules().await;
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
        let rules = log.get_rules().await;
        input_tx
            .send(RulePayload::Rules(rules))
            .await
            .expect("assert: can send rules at initialization");
        let service_name = self.name().to_string();
        let send_notification = self.shared().send_notification.clone();
        std::thread::Builder::new()
            .name(format!("rules-processor-{}", self.name()).into())
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
        log.healthcheck()
            .await
            .expect("failed to connect to Google API");
        tracing::info!(
            "service {} is running with spreadsheet {}",
            self.name(),
            self.spreadsheet_id(),
        );
        tracing::debug!(
            "channel capacity {} for service {}",
            self.channel_capacity(),
            self.name()
        );
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

/// Http(s) client for purposes of Healthchecks and Metrics
pub(crate) struct HttpClient {
    client: HttpsClient,
    body_max_size: usize,
    error_on_oversize: bool,
    url: Uri,
}

impl HttpClient {
    pub(crate) fn new(
        body_max_size: usize,
        error_on_oversize: bool,
        pool_idle_timeout: Duration,
        url: Uri,
    ) -> Self {
        let client: HttpsClient = Client::builder()
            .http09_responses(true)
            .retry_canceled_requests(false)
            .pool_max_idle_per_host(1)
            .pool_idle_timeout(Some(pool_idle_timeout))
            .build(
                hyper_rustls::HttpsConnectorBuilder::new()
                    .with_native_roots()
                    .https_or_http()
                    .enable_http1()
                    .build(),
            );
        Self {
            client,
            body_max_size,
            error_on_oversize,
            url,
        }
    }

    pub(crate) async fn get(&self) -> Result<String, String> {
        let mut res = self
            .client
            .request(
                Request::get(self.url.clone())
                    .header(header::USER_AGENT, APP_NAME)
                    .body(Body::empty())
                    .expect("assert: should be able to construct an http get request"),
            )
            .await
            .map_err(|e| e.to_string())?;
        let mut size = 0;
        let mut body = vec![];
        while size < self.body_max_size {
            if let Some(next) = res.data().await {
                let chunk = next.map_err(|e| e.to_string())?;
                size += chunk.len();
                body.extend_from_slice(&chunk);
            } else {
                break;
            }
        }
        if size >= self.body_max_size {
            if self.error_on_oversize {
                let msg = format!(
                    "response body for endpoint {} is greater than limit of {} bytes",
                    self.url, self.body_max_size
                );
                tracing::error!("{}", msg);
                return Err(msg);
            } else {
                tracing::warn!(
                    "output body for endpoint {:?} was truncated to {} bytes.",
                    self.url,
                    self.body_max_size
                );
            }
        }
        let size = size.min(self.body_max_size);
        let text = String::from_utf8_lossy(&body[..size]).to_string();
        if res.status() >= StatusCode::OK && res.status() < StatusCode::BAD_REQUEST {
            Ok(text)
        } else {
            Err(text)
        }
    }
}

fn capture_datetime(line: &str) -> Option<NaiveDateTime> {
    lazy_static! {
        static ref RE: Regex = Regex::new(
            r"(?x)
            (?P<datetime>
                \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\s\+\d{2}:\d{2}|
                \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2}:\d{2}|
                \d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\+\d{2}:\d{2}|
                \d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\s\+\d{2}:\d{2}|
                \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z|
                \d{4}[-/]\d{2}[-/]\d{2}\s\d{2}:\d{2}:\d{2}\.\d+|
                \d{4}[-/]\d{2}[-/]\d{2}\s\d{2}:\d{2}:\d{2}
            )"
        )
        .expect("assert: datetime regex is properly constructed");
    }
    RE.captures(line).and_then(|cap| {
        cap.name("datetime").and_then(|datetime| {
            let captured = datetime.as_str();
            captured
                .parse::<DateTime<Utc>>()
                .ok()
                .map(|d| d.naive_utc())
                .or_else(|| NaiveDateTime::parse_from_str(captured, "%Y-%m-%dT%H:%M:%S%.f").ok())
                .or_else(|| NaiveDateTime::parse_from_str(captured, "%Y/%m/%d %H:%M:%S%.f").ok())
                .or_else(|| NaiveDateTime::parse_from_str(captured, "%Y-%m-%d %H:%M:%S%.f").ok())
                .or_else(|| NaiveDateTime::parse_from_str(captured, "%Y/%m/%d %H:%M:%S").ok())
                .or_else(|| NaiveDateTime::parse_from_str(captured, "%Y-%m-%d %H:%M:%S").ok())
        })
    })
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
    use crate::storage::{jitter_duration, Storage};
    use crate::tests::TEST_HOST_ID;
    use crate::Sender;
    use chrono::NaiveDate;
    use chrono::Utc;
    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Body, Method, Request, Response, Server, StatusCode};
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
        let (tx, _) = mpsc::channel(1);
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

    pub(crate) const HEALTHY_REPLY: &str = "Test service is healthy";
    pub(crate) const UNHEALTHY_REPLY: &str = "Test service is unhealthy";
    const METRICS_REPLY: &str = r#"""
    # HELP example_http_request_duration_seconds The HTTP request latencies in seconds.
    # TYPE example_http_request_duration_seconds histogram
    example_http_request_duration_seconds_bucket{handler="all",parity="0",le="0.005"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="0",le="0.01"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="0",le="0.025"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="0",le="0.05"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="0",le="0.1"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="0",le="0.25"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="0",le="0.5"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="0",le="1"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="0",le="2.5"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="0",le="5"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="0",le="10"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="0",le="+Inf"} 18
    example_http_request_duration_seconds_sum{handler="all",parity="0"} 0.005173251
    example_http_request_duration_seconds_count{handler="all",parity="0"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="1",le="0.005"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="1",le="0.01"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="1",le="0.025"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="1",le="0.05"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="1",le="0.1"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="1",le="0.25"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="1",le="0.5"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="1",le="1"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="1",le="2.5"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="1",le="5"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="1",le="10"} 18
    example_http_request_duration_seconds_bucket{handler="all",parity="1",le="+Inf"} 18
    example_http_request_duration_seconds_sum{handler="all",parity="1"} 0.004740836999999999
    example_http_request_duration_seconds_count{handler="all",parity="1"} 18
    # HELP example_http_requests_total Number of HTTP requests made.
    # TYPE example_http_requests_total counter
    example_http_requests_total{handler="all"} 37
    # HELP example_http_response_size_bytes The HTTP response sizes in bytes.
    # TYPE example_http_response_size_bytes gauge
    example_http_response_size_bytes{handler="all"} 2779
    """#;

    pub(crate) async fn router(req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
        assert_eq!(
            req.headers()
                .get(header::USER_AGENT)
                .map(|h| h.to_str().expect("test assert: header value is ascii")),
            Some(APP_NAME),
            "request from Goral should contain User-Agent header with the Goral name"
        );
        match (req.method(), req.uri().path()) {
            (&Method::GET, "/health") => Ok(Response::new(Body::from(HEALTHY_REPLY))),
            (&Method::GET, "/unhealthy") => Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(UNHEALTHY_REPLY.into())
                .expect("test assert: should be able to construct response for static body")),
            (&Method::GET, "/metrics") => Ok(Response::new(Body::from(METRICS_REPLY))),
            (&Method::GET, "/timeout") => {
                let timeout = Duration::from_secs(1);
                tokio::time::sleep(timeout).await;
                Ok(Response::new(Body::from(format!("timeout {timeout:?}"))))
            }
            _ => {
                let mut not_found = Response::default();
                *not_found.status_mut() = StatusCode::NOT_FOUND;
                Ok(not_found)
            }
        }
    }

    pub(crate) async fn run_server(port: u16) {
        let addr = ([127, 0, 0, 1], port).into();
        let service = make_service_fn(|_| async { Ok::<_, hyper::Error>(service_fn(router)) });
        let server = Server::bind(&addr).serve(service);
        server
            .await
            .expect("test assert: should be able to run http server");
    }

    #[tokio::test]
    async fn http_client_ok() {
        tokio::spawn(async move {
            run_server(53260).await;
        });
        tokio::time::sleep(Duration::from_millis(10)).await; // some time for server to start

        let client = HttpClient::new(
            HEALTHY_REPLY.len(),
            false,
            Duration::from_millis(10),
            Uri::from_static("http://127.0.0.1:53260/health"),
        );
        assert_eq!(client.get().await, Ok(HEALTHY_REPLY.to_string()));
    }

    #[tokio::test]
    async fn http_client_err() {
        tokio::spawn(async move {
            run_server(53261).await;
        });
        tokio::time::sleep(Duration::from_millis(10)).await; // some time for server to start

        let client = HttpClient::new(
            UNHEALTHY_REPLY.len(),
            false,
            Duration::from_millis(10),
            Uri::from_static("http://127.0.0.1:53261/unhealthy"),
        );
        assert_eq!(client.get().await, Err(UNHEALTHY_REPLY.to_string()));
    }

    #[tokio::test]
    async fn http_client_body_truncation() {
        tokio::spawn(async move {
            run_server(53262).await;
        });
        tokio::time::sleep(Duration::from_millis(10)).await; // some time for server to start

        let client = HttpClient::new(
            HEALTHY_REPLY.len() - 1,
            false,
            Duration::from_millis(10),
            Uri::from_static("http://127.0.0.1:53262/health"),
        );
        assert_eq!(
            client.get().await,
            Ok(HEALTHY_REPLY
                .chars()
                .take(HEALTHY_REPLY.len() - 1)
                .collect())
        );
    }

    #[tokio::test]
    async fn http_client_body_size_err() {
        tokio::spawn(async move {
            run_server(53263).await;
        });
        tokio::time::sleep(Duration::from_millis(10)).await; // some time for server to start

        let client = HttpClient::new(
            HEALTHY_REPLY.len() - 1,
            true,
            Duration::from_millis(10),
            Uri::from_static("http://127.0.0.1:53263/health"),
        );
        assert_eq!(client.get().await, Err(format!("response body for endpoint http://127.0.0.1:53263/health is greater than limit of {} bytes", HEALTHY_REPLY.len()-1)));
    }

    #[test]
    fn log_date_time() {
        assert_eq!(
            capture_datetime("INFO:2023/02/17 14:30:15 This is an info message."),
            Some(
                NaiveDate::from_ymd_opt(2023, 02, 17)
                    .expect("test assert: static datetime")
                    .and_hms_opt(14, 30, 15)
                    .expect("test assert: static datetime")
            )
        );
        assert_eq!(
            capture_datetime("INFO:2023-02-17 14:30:15 This is an info message."),
            Some(
                NaiveDate::from_ymd_opt(2023, 02, 17)
                    .expect("test assert: static datetime")
                    .and_hms_opt(14, 30, 15)
                    .expect("test assert: static datetime")
            )
        );
        assert_eq!(
            capture_datetime(
                r#"[2m2023-11-02 12:29:51.552906[0m [32m INFO[0m [2mgoral::services::healthcheck[0m[2m:[0m starting check for Http(http://127.0.0.1:9898/)"#
            ),
            Some(
                NaiveDate::from_ymd_opt(2023, 11, 02)
                    .expect("test assert: static datetime")
                    .and_hms_micro_opt(12, 29, 51, 552906)
                    .expect("test assert: static datetime")
            )
        );
        assert_eq!(
            capture_datetime(
                r#"[2m2023/11/02 12:29:51.552906[0m [32m INFO[0m [2mgoral::services::healthcheck[0m[2m:[0m starting check for Http(http://127.0.0.1:9898/)"#
            ),
            Some(
                NaiveDate::from_ymd_opt(2023, 11, 02)
                    .expect("test assert: static datetime")
                    .and_hms_micro_opt(12, 29, 51, 552906)
                    .expect("test assert: static datetime")
            )
        );
        assert_eq!(
            capture_datetime(
                r#"[2m2023-11-02T12:29:51.552906Z[0m [32m INFO[0m [2mgoral::services::healthcheck[0m[2m:[0m starting check for Http(http://127.0.0.1:9898/)"#
            ),
            Some(
                NaiveDate::from_ymd_opt(2023, 11, 02)
                    .expect("test assert: static datetime")
                    .and_hms_micro_opt(12, 29, 51, 552906)
                    .expect("test assert: static datetime")
            )
        );
        assert_eq!(
            capture_datetime("INFO:2014-11-28 21:00:09 +09:00 This is an info message."),
            Some(
                NaiveDate::from_ymd_opt(2014, 11, 28)
                    .expect("test assert: static datetime")
                    .and_hms_opt(12, 00, 09)
                    .expect("test assert: static datetime")
            )
        );
        assert_eq!(
            capture_datetime("INFO:2014-11-28T21:00:09+09:00 This is an info message."),
            Some(
                NaiveDate::from_ymd_opt(2014, 11, 28)
                    .expect("test assert: static datetime")
                    .and_hms_opt(12, 00, 09)
                    .expect("test assert: static datetime")
            )
        );
    }
}
