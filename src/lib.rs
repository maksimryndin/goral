pub mod configuration;
pub mod messenger;
pub mod rules;
pub mod services;
pub mod spreadsheet;
pub mod storage;
use crate::messenger::configuration::MessengerConfig;
use chrono::{DateTime, NaiveDateTime, Utc};
pub use configuration::*;
use google_sheets4::hyper_rustls::HttpsConnector;
use hyper::{client::connect::HttpConnector, Client};
use lazy_static::lazy_static;
pub use messenger::*;
use regex::Regex;
use services::general::{GeneralService, GENERAL_SERVICE_NAME};
use services::healthcheck::{HealthcheckService, HEALTHCHECK_SERVICE_NAME};
use services::kv::{KvService, KV_SERVICE_NAME};
use services::logs::{LogsService, LOGS_SERVICE_NAME};
use services::metrics::{MetricsService, METRICS_SERVICE_NAME};
use services::system::{SystemService, SYSTEM_SERVICE_NAME};
pub use services::*;
use spreadsheet::sheet::TabColorRGB;
pub use spreadsheet::*;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::Arc;
use std::time::Duration;
pub use storage::*;
use sysinfo::{System, SystemExt};
use tokio::sync::mpsc::{self, error::TrySendError, Receiver, Sender as TokioSender};
use tracing::Level;

pub(crate) type HyperConnector = HttpsConnector<HttpConnector>;
pub(crate) type HttpsClient = Client<HyperConnector>;

pub(crate) const HOST_ID_CHARS_LIMIT: usize = 8;

fn get_service_tab_color(service_name: &str) -> TabColorRGB {
    let rgb = match service_name {
        GENERAL_SERVICE_NAME => (0, 0, 0), // general service hasn't sheets
        HEALTHCHECK_SERVICE_NAME => (255, 0, 0), // red
        KV_SERVICE_NAME => (255, 153, 0),  // orange
        LOGS_SERVICE_NAME => (0, 0, 255),  // blue
        METRICS_SERVICE_NAME => (153, 0, 255), // purple
        SYSTEM_SERVICE_NAME => (0, 255, 0), // green,
        _ => panic!("assert: every service has its tab color defined"),
    };
    (
        rgb.0 as f32 / 255.0,
        rgb.1 as f32 / 255.0,
        rgb.2 as f32 / 255.0,
    )
}

// collect messengers hosts (for proper connection pooling) because some service may share a messenger
pub fn collect_messengers(
    config: &Configuration,
) -> HashMap<&'static str, Option<Arc<BoxedMessenger>>> {
    let pairs = [
        (
            GENERAL_SERVICE_NAME,
            Some(config.general.messenger.host().to_string()),
        ),
        (
            HEALTHCHECK_SERVICE_NAME,
            config.healthcheck.as_ref().and_then(|healthcheck| {
                healthcheck.messenger.as_ref().map(|m| m.host().to_string())
            }),
        ),
        (
            KV_SERVICE_NAME,
            config
                .kv
                .as_ref()
                .and_then(|kv| kv.messenger.as_ref().map(|m| m.host().to_string())),
        ),
        (
            LOGS_SERVICE_NAME,
            config
                .logs
                .as_ref()
                .and_then(|logs| logs.messenger.as_ref().map(|m| m.host().to_string())),
        ),
        (
            METRICS_SERVICE_NAME,
            config
                .metrics
                .as_ref()
                .and_then(|metrics| metrics.messenger.as_ref().map(|m| m.host().to_string())),
        ),
        (
            SYSTEM_SERVICE_NAME,
            config
                .system
                .as_ref()
                .and_then(|system| system.messenger.as_ref().map(|m| m.host().to_string())),
        ),
    ];

    let hosts: HashMap<Option<String>, Vec<&'static str>> =
        pairs
            .into_iter()
            .fold(HashMap::new(), |mut map, (service, host)| {
                map.entry(host).or_default().push(service);
                map
            });

    let mut map = HashMap::new();
    for (messenger_host, services) in hosts {
        let messenger = messenger_host.map(|host| {
            let messenger = get_messenger(&host)
                .unwrap_or_else(|_| panic!("failed to create messenger for host `{}`", host));
            Arc::new(messenger)
        });
        for service in services {
            map.insert(service, messenger.clone());
        }
    }
    map
}

pub fn collect_services(
    config: Configuration,
    shared: Shared,
    mut messengers: HashMap<&'static str, Option<Arc<BoxedMessenger>>>,
    channel: Receiver<Notification>,
) -> Vec<Box<dyn Service + Sync + Send + 'static>> {
    let mut services = Vec::with_capacity(5);
    let assertion =
        "assert: safe to unwrap on messengers map as we collect all services-messenger pairs";
    if let Some(healthcheck) = config.healthcheck {
        let healthcheck_service = HealthcheckService::new(
            shared.clone_with_messenger(
                messengers
                    .remove(HEALTHCHECK_SERVICE_NAME)
                    .expect(assertion),
            ),
            healthcheck,
        );
        let healthcheck_service = Box::new(healthcheck_service) as Box<dyn Service + Sync + Send>;
        services.push(healthcheck_service);
    }

    if let Some(metrics) = config.metrics {
        let metrics_service = MetricsService::new(
            shared.clone_with_messenger(messengers.remove(METRICS_SERVICE_NAME).expect(assertion)),
            metrics,
        );
        let metrics_service = Box::new(metrics_service) as Box<dyn Service + Sync + Send>;
        services.push(metrics_service);
    }

    if let Some(kv) = config.kv {
        let kv_service = KvService::new(
            shared.clone_with_messenger(messengers.remove(KV_SERVICE_NAME).expect(assertion)),
            kv,
        );
        let kv_service = Box::new(kv_service) as Box<dyn Service + Sync + Send>;
        services.push(kv_service);
    }

    if let Some(logs) = config.logs {
        let logs_service = LogsService::new(
            shared.clone_with_messenger(messengers.remove(LOGS_SERVICE_NAME).expect(assertion)),
            logs,
        );
        let logs_service = Box::new(logs_service) as Box<dyn Service + Sync + Send>;
        services.push(logs_service);
    }

    if let Some(system) = config.system {
        let system_service = SystemService::new(
            shared.clone_with_messenger(messengers.remove(SYSTEM_SERVICE_NAME).expect(assertion)),
            system,
        );
        let system_service = Box::new(system_service) as Box<dyn Service + Sync + Send>;
        services.push(system_service);
    }

    let general_service = GeneralService::new(
        shared.clone_with_messenger(messengers.remove(GENERAL_SERVICE_NAME).expect(assertion)),
        config.general,
        channel,
    );
    let general_service = Box::new(general_service) as Box<dyn Service + Sync + Send>;
    services.push(general_service);
    assert_eq!(
        services
            .last()
            .expect("assert: services should contain at least general service")
            .name(),
        GENERAL_SERVICE_NAME
    );
    services
}

#[derive(Debug)]
pub struct Notification {
    pub(crate) message: String,
    pub(crate) level: Level,
}

impl Notification {
    pub(crate) fn new(message: String, level: Level) -> Self {
        Self { message, level }
    }
}

#[derive(Debug, Clone)]
pub struct Sender {
    tx: TokioSender<Notification>,
    service: &'static str,
}

impl Sender {
    pub fn new(tx: TokioSender<Notification>, service: &'static str) -> Self {
        Self { tx, service }
    }

    pub async fn send(&self, notification: Notification) {
        if self.tx.send(notification).await.is_err() {
            panic!(
                "failed to send notification - `{}` service doesn't accept notifications",
                self.service
            );
        }
    }

    pub fn send_nonblock(&self, notification: Notification) {
        match self.tx.try_send(notification) {
            Err(TrySendError::Closed(_)) => {
                panic!(
                    "failed to send notification - `{}` service doesn't accept notifications",
                    self.service
                )
            }
            Err(TrySendError::Full(n)) => {
                tracing::error!(
                    "failed to send notification {:?} - `{}` service notifications queue is full",
                    n,
                    self.service
                );
            }
            Ok(_) => (),
        }
    }

    pub async fn info(&self, message: String) {
        let notification = Notification::new(message, Level::INFO);
        self.send(notification).await
    }

    pub fn try_info(&self, message: String) {
        let notification = Notification::new(message, Level::INFO);
        self.send_nonblock(notification)
    }

    pub async fn warn(&self, message: String) {
        let notification = Notification::new(message, Level::WARN);
        self.send(notification).await
    }

    pub fn try_warn(&self, message: String) {
        let notification = Notification::new(message, Level::WARN);
        self.send_nonblock(notification)
    }

    pub async fn error(&self, message: String) {
        let notification = Notification::new(message, Level::ERROR);
        self.send(notification).await
    }

    pub fn try_error(&self, message: String) {
        let notification = Notification::new(message, Level::ERROR);
        self.send_nonblock(notification)
    }

    pub async fn fatal(&self, message: String) {
        let notification = Notification::new(message, Level::ERROR);
        self.send(notification).await;
        // for fatal errors we need some time to send error
        // It is more important to notify user via messenger than to
        // restart quickly because restart doesn't help for recovery
        // user is required to fix a problem
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

pub(crate) fn setup_messenger_channel(service: &'static str) -> (Sender, Receiver<Notification>) {
    let (tx, rx) = mpsc::channel(60); // 60 simultaneous messages is enough for any rate limiting messenger
    let tx = Sender::new(tx, service);
    (tx, rx)
}

pub fn setup_general_messenger_channel() -> (Sender, Receiver<Notification>) {
    setup_messenger_channel(GENERAL_SERVICE_NAME)
}

#[derive(Debug)]
pub(crate) struct MessengerApi {
    pub(crate) config: MessengerConfig,
    pub(crate) message_tx: Sender,
    pub(crate) message_rx: Option<Receiver<Notification>>,
}

impl MessengerApi {
    fn new(config: MessengerConfig, service: &'static str) -> Self {
        let (message_tx, message_rx) = setup_messenger_channel(service);
        Self {
            config,
            message_tx,
            message_rx: Some(message_rx),
        }
    }
}

#[derive(Clone)]
pub struct Shared {
    pub(crate) messenger: Option<Arc<BoxedMessenger>>,
    pub(crate) send_notification: Sender,
}

impl Shared {
    pub fn new(send_notification: Sender) -> Shared {
        Self {
            messenger: None,
            send_notification,
        }
    }

    pub fn clone_with_messenger(&self, messenger: Option<Arc<BoxedMessenger>>) -> Self {
        let mut clone = self.clone();
        clone.messenger = messenger;
        clone
    }
}

impl Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Shared")
    }
}

pub async fn welcome(send_notification: Sender, project_id: String, host_id: String) {
    let sys = tokio::task::spawn_blocking(|| {
        sysinfo::set_open_files_limit(0);
        let sys = System::new_all();
        let mem = sys.total_memory() / 1000 / 1000 / 1000;
        match (
            sys.name(),
            sys.long_os_version(),
            sys.kernel_version(),
            sys.host_name(),
        ) {
            (Some(name), Some(os_version), Some(kernel_version), Some(host_name)) => format!(
                "{name} {os_version}(kernel {kernel_version}); hostname: {host_name}, RAM {mem}G"
            ),
            (Some(name), Some(os_version), None, Some(host_name)) => {
                format!("{name} {os_version}; hostname: {host_name}, RAM {mem}G")
            }
            (Some(name), Some(os_version), None, None) => {
                format!("{name} {os_version}; RAM {mem}G")
            }
            (Some(name), None, None, None) => format!("{name}; RAM {mem}G"),
            _ => format!("RAM {mem}G"),
        }
    })
    .await
    .expect("assert: should be able to collect basic system info");
    let msg = format!(
        "{APP_NAME} has started with [api usage page](https://console.cloud.google.com/apis/dashboard?project={project_id}&show=all) and [api quota page](https://console.cloud.google.com/iam-admin/quotas?project={project_id}) at `{sys}`, host id `{host_id}`", 
    );
    send_notification.info(msg).await;
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

pub(crate) fn jitter_duration() -> Duration {
    let mut buf = [0u8; 2];
    getrandom::getrandom(&mut buf).expect("assert: can get random from the OS");
    let jitter = u16::from_be_bytes(buf);
    let jitter = jitter >> 2; // to limit values to 2^14 = 16384 or ~16 secs
    Duration::from_millis(jitter as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    pub(crate) const TEST_HOST_ID: &str = "testhost";
    use chrono::NaiveDate;

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

    #[test]
    fn jittered_duration() {
        let upper_bound = Duration::from_millis(2u64.pow(14) + 1);
        for _ in 0..100 {
            assert!(jitter_duration() < upper_bound);
        }
    }
}
