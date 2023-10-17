pub(crate) mod configuration;

use crate::messenger::configuration::MessengerConfig;

use crate::services::healthcheck::configuration::{
    scrape_push_rule, Healthcheck, Liveness as LivenessConfig, LivenessType,
};
use crate::services::{Data, HttpClient, Service, TaskResult};
use crate::storage::{AppendableLog, Datarow, Datavalue};
use crate::{Sender, Shared};
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use hyper::Uri;
use std::fmt::{self, Debug, Display};
use std::result::Result as StdResult;
use std::time::Duration;

use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::task::JoinHandle;
use tracing::Level;

pub const HEALTHCHECK_SERVICE_NAME: &str = "health";
const MAX_BYTES_LIVENESS_OUTPUT: usize = 1024;

#[derive(Clone)]
pub(crate) struct Liveness {
    pub(crate) initial_delay: Duration,
    pub(crate) period: Duration,
    pub(crate) timeout: Duration,
    pub(crate) endpoint: Option<Uri>,
    pub(crate) command: Option<String>,
    pub(crate) typ: LivenessType,
}

impl Debug for Liveness {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(endpoint) = &self.endpoint {
            write!(f, "{} ({})", endpoint, self.typ)
        } else {
            write!(f, "{}", self.command.as_ref().unwrap())
        }
    }
}

impl Display for Liveness {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(endpoint) = &self.endpoint {
            write!(f, "{} ({})", endpoint, self.typ)
        } else {
            write!(f, "{}", self.command.as_ref().unwrap())
        }
    }
}

impl From<LivenessConfig> for Liveness {
    fn from(c: LivenessConfig) -> Self {
        Self {
            initial_delay: Duration::from_secs(c.initial_delay_secs.into()),
            period: Duration::from_secs(c.period_secs.into()),
            timeout: Duration::from_millis(c.timeout_ms.into()),
            endpoint: c.endpoint.map(|u| {
                let s: String = u.into();
                Uri::from_maybe_shared(s.into_bytes())
                    .expect("assert: endpoint url is validated in configuration")
            }),
            command: c.command,
            typ: c.typ,
        }
    }
}

#[derive(Debug)]
pub(crate) struct HealthcheckService {
    shared: Shared,
    spreadsheet_id: String,
    liveness: Vec<Liveness>,
    messenger_config: Option<MessengerConfig>,
    push_interval: Duration,
    channel_capacity: usize,
    liveness_previous_state: Vec<Option<bool>>,
}

impl HealthcheckService {
    pub(crate) fn new(shared: Shared, config: Healthcheck) -> HealthcheckService {
        let channel_capacity = scrape_push_rule(&config.liveness, &config.push_interval_secs)
            .expect("assert: push/scrate ratio is validated at configuration");
        let liveness_previous_state = vec![None; config.liveness.len()];
        Self {
            shared,
            spreadsheet_id: config.spreadsheet_id,
            liveness: config.liveness.into_iter().map(|l| l.into()).collect(),
            messenger_config: config.messenger,
            push_interval: Duration::from_secs(config.push_interval_secs.into()),
            channel_capacity,
            liveness_previous_state,
        }
    }

    async fn make_probe(liveness: &Liveness) -> StdResult<String, String> {
        tracing::trace!("making probe for {:?}", liveness);
        match liveness.typ {
            LivenessType::Http => {
                let url = liveness.endpoint.as_ref().expect(
                    "assert: http endpoint is set for http liveness type, checked at configuration",
                );
                match url.scheme_str() {
                    Some("http") | Some("https") => {
                        // we setup a new connection for the checked app each time as a new client would do
                        // https://docs.rs/hyper/latest/hyper/client/struct.Builder.html
                        let client = HttpClient::new(
                            MAX_BYTES_LIVENESS_OUTPUT,
                            false,
                            liveness.timeout,
                            url.clone(),
                        );
                        client.get().await
                    }
                    _ => return Err(format!("unknown url scheme for probe {:?}", liveness)),
                }
            }
            LivenessType::Tcp | LivenessType::Grpc | LivenessType::Command => {
                Err(format!("{:?} probe is unsupported atm", liveness.typ))
            }
        }
    }

    async fn is_alive(liveness: &Liveness) -> StdResult<String, String> {
        tokio::select! {
            _ = tokio::time::sleep(liveness.timeout) => Err(format!("probe timeout {:?}", liveness.timeout)),
            res = Self::make_probe(liveness) => res
        }
    }

    fn create_datarow(
        is_alive: bool,
        text: String,
        liveness: &Liveness,
        probe_time: DateTime<Utc>,
    ) -> Datarow {
        Datarow::new(
            liveness.to_string(),
            probe_time.naive_utc(),
            vec![
                ("is_alive".to_string(), Datavalue::Bool(is_alive)),
                ("output".to_string(), Datavalue::Text(text)),
            ],
            None,
        )
    }

    async fn run_check(
        index: usize,
        liveness: Liveness,
        sender: mpsc::Sender<TaskResult>,
        send_notification: Sender,
    ) {
        tokio::time::sleep(liveness.initial_delay).await;
        let mut interval = tokio::time::interval(liveness.period);
        tracing::info!("starting check for {:?}", liveness);
        loop {
            tokio::select! {
                _ = sender.closed() => {
                    tracing::info!("finished check for {:?}", liveness);
                    return;
                },
                _ = interval.tick() => {
                    let probe_time = Utc::now();
                    let result = Self::is_alive(&liveness).await
                        .map(|t| Data::Single(Self::create_datarow(true, t, &liveness, probe_time)))
                        .map_err(|t| {
                            tracing::debug!("liveness check for {:?} failed with output `{}`", liveness, t);
                            Data::Single(Self::create_datarow(false, t, &liveness, probe_time))
                        });
                    match sender.try_send(TaskResult{id: index, result}) {
                        Err(TrySendError::Full(res)) => {
                            let msg = "health messages queue is full so increase scrape interval and decrease push interval".to_string();
                            tracing::error!("{}. Cannot send liveness `{:?}` result `{:?}`", msg, liveness, res);
                            send_notification.try_error(msg);
                        },
                        Err(TrySendError::Closed(res)) => {
                            let msg = "health messages queue has been unexpectedly closed".to_string();
                            tracing::error!("{}: cannot send liveness {:?} result {:?}", msg, liveness, res);
                            send_notification.fatal(msg).await;
                            panic!("assert: health messages queue shouldn't be closed before shutdown signal");
                        },
                        _ => {},
                    }
                }
            }
        }
    }

    async fn send_message(
        &self,
        liveness: &Liveness,
        is_alive: bool,
        datarow: &mut Datarow,
        log: &AppendableLog,
    ) {
        let (level, message) = if is_alive {
            (
                Level::INFO,
                format!("Liveness probe for `{:?}` succeeded", liveness),
            )
        } else {
            (Level::ERROR,
            format!(
                "Liveness probe for `{:?}` failed with output at [spreadsheet]({}), sheet may be created a bit later",
                liveness, log.sheet_url(datarow.sheet_id(log.host_id(), self.name()))))
        };
        if let Some(messenger) = self.shared.messenger.as_ref() {
            let messenger_config = self
                .messenger_config
                .as_ref()
                .expect("assert: if messenger is set, then config is also nonempty");
            if let Err(_) = messenger
                .send_by_level(messenger_config, &message, level)
                .await
            {
                tracing::error!("failed to send liveness probe output via configured messenger: {:?} for service {}",  messenger_config, self.name());
                self.shared.send_notification.try_error(format!(
                    "{}. Sending via configured messenger failed.",
                    message
                ));
            }
        } else {
            if is_alive {
                tracing::warn!(
                    "{}. Messenger is not configured for {}.",
                    message,
                    self.name()
                );
            } else {
                tracing::error!(
                    "{}. Messenger is not configured for {}.",
                    message,
                    self.name()
                );
                self.shared.send_notification.try_error(format!(
                    "{}\nMessenger is not configured for {}",
                    message,
                    self.name(),
                ));
            }
        }
    }
}

#[async_trait]
impl Service for HealthcheckService {
    fn name(&self) -> &str {
        HEALTHCHECK_SERVICE_NAME
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
        _: &AppendableLog,
    ) -> Data {
        let TaskResult { id: _, result } = result;
        match result {
            Ok(data) => data,
            Err(data) => data,
        }
    }

    async fn process_task_result(&mut self, result: TaskResult, log: &AppendableLog) -> Data {
        let TaskResult { id, result } = result;
        let (is_alive, mut datarow) = match result {
            Ok(Data::Single(datarow)) => (true, datarow),
            Err(Data::Single(datarow)) => (false, datarow),
            _ => panic!(
                "assert: healthcheck result contains single datarow both for error and for ok"
            ),
        };
        if self.liveness_previous_state[id].is_none()
            || self.liveness_previous_state[id] != Some(is_alive)
        {
            self.send_message(&self.liveness[id], is_alive, &mut datarow, log)
                .await;
            self.liveness_previous_state[id] = Some(is_alive);
        }
        Data::Single(datarow)
    }

    async fn spawn_tasks(&mut self, sender: mpsc::Sender<TaskResult>) -> Vec<JoinHandle<()>> {
        self.liveness
            .iter()
            .enumerate()
            .map(|(i, l)| {
                let liveness = l.clone();
                let sender = sender.clone();
                let send_notification = self.shared.send_notification.clone();
                tokio::spawn(async move {
                    Self::run_check(i, liveness, sender, send_notification).await;
                })
            })
            .collect()
    }
}
