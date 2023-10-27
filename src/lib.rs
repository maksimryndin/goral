pub mod configuration;
pub mod messenger;
pub mod services;
pub mod spreadsheet;
pub mod storage;
pub use configuration::*;
use google_sheets4::hyper_rustls::HttpsConnector;
use hyper::{client::connect::HttpConnector, Client};
pub use messenger::*;
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
use tokio::sync::mpsc::{error::TrySendError, Receiver, Sender as TokioSender};
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

    let hosts = pairs
        .into_iter()
        .fold(HashMap::new(), |mut map, (service, host)| {
            map.entry(host).or_insert(vec![]).push(service);
            map
        });

    let mut map = HashMap::new();
    for (messenger_host, services) in hosts {
        let messenger = messenger_host.map(|host| {
            let messenger = get_messenger(&host)
                .expect(format!("failed to create messenger for host `{}`", host).as_str());
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

#[derive(Clone)]
pub struct Sender(TokioSender<Notification>);

impl Sender {
    pub fn new(sender: TokioSender<Notification>) -> Self {
        Self(sender)
    }

    pub async fn send(&self, notification: Notification) {
        if let Err(_) = self.0.send(notification).await {
            panic!("failed to send notification - general service doesn't accept notifications");
        }
    }

    pub fn send_nonblock(&self, notification: Notification) {
        match self.0.try_send(notification) {
            Err(TrySendError::Closed(_)) => {
                panic!("failed to send notification - general service doesn't accept notifications")
            }
            Err(TrySendError::Full(n)) => {
                tracing::error!("failed to send notification {:?} - general service notifications queue is full", n);
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

#[cfg(test)]
mod tests {
    pub(crate) const TEST_HOST_ID: &str = "testhost";
}
