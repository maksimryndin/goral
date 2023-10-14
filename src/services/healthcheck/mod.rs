pub(crate) mod configuration;

use crate::messenger::configuration::MessengerConfig;

use crate::services::healthcheck::configuration::{
    scrape_push_rule, Healthcheck, Liveness as LivenessConfig, LivenessType,
};
use crate::services::Service;
use crate::storage::{AppendableLog, Datarow, Datavalue};
use crate::Shared;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hyper::client::connect::HttpConnector;
use hyper::{body::HttpBody as _, Client, StatusCode, Uri};
use std::fmt::{self, Debug, Display};
use std::result::Result as StdResult;

use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::mpsc::{self, error::TrySendError};
use tracing::{instrument, Level};

pub const HEALTHCHECK_SERVICE_NAME: &str = "healthcheck";
const MAX_BYTES_LIVENESS_OUTPUT: usize = 1024;
type LivenessOutput = (usize, DateTime<Utc>, StdResult<String, String>);

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
}

impl HealthcheckService {
    pub(crate) fn new(shared: Shared, config: Healthcheck) -> HealthcheckService {
        let channel_capacity = scrape_push_rule(&config.liveness, &config.push_interval_secs)
            .expect("assert: push/scrate ratio is validated at configuration");
        Self {
            shared,
            spreadsheet_id: config.spreadsheet_id,
            liveness: config.liveness.into_iter().map(|l| l.into()).collect(),
            messenger_config: config.messenger,
            push_interval: Duration::from_secs(config.push_interval_secs.into()),
            channel_capacity,
        }
    }

    async fn make_probe(liveness: &Liveness) -> StdResult<String, String> {
        tracing::trace!("making probe for {:?}", liveness);
        match liveness.typ {
            LivenessType::Http => {
                let url = liveness
                    .endpoint
                    .as_ref()
                    .expect("assert: http endpoint is set for http liveness type");
                match url.scheme_str() {
                    Some("http") => {
                        // we setup a new connection for the checked app each time as a new client would do
                        // https://docs.rs/hyper/latest/hyper/client/struct.Builder.html
                        let client: Client<HttpConnector> = Client::builder()
                            .http09_responses(true)
                            .http1_max_buf_size(8192)
                            .retry_canceled_requests(false)
                            .pool_max_idle_per_host(1)
                            .pool_idle_timeout(Some(liveness.timeout))
                            .build_http();
                        let mut res = client.get(url.clone()).await.map_err(|e| e.to_string())?;
                        let mut size = 0;
                        let mut body = vec![];
                        while size < MAX_BYTES_LIVENESS_OUTPUT {
                            if let Some(next) = res.data().await {
                                let chunk = next.map_err(|e| e.to_string())?;
                                size += chunk.len();
                                body.extend_from_slice(&chunk);
                            }
                        }
                        if size >= MAX_BYTES_LIVENESS_OUTPUT {
                            tracing::warn!(
                                "output body for probe {:?} was truncated to {} bytes.",
                                liveness,
                                MAX_BYTES_LIVENESS_OUTPUT
                            );
                        }
                        let text =
                            String::from_utf8_lossy(&body[..MAX_BYTES_LIVENESS_OUTPUT]).to_string();
                        if res.status() >= StatusCode::OK || res.status() < StatusCode::BAD_REQUEST
                        {
                            Ok(text)
                        } else {
                            Err(text)
                        }
                    }
                    Some("https") => {
                        todo!()
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

    async fn run_check(index: usize, liveness: Liveness, sender: mpsc::Sender<LivenessOutput>) {
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
                    let res = Self::is_alive(&liveness).await;
                    match sender.try_send((index, probe_time, res)) {
                        Err(TrySendError::Full(res)) => tracing::error!("channel is full: cannot send liveness {:?} result {:?}", liveness, res),
                        Err(TrySendError::Closed(res)) => tracing::error!("channel is closed: cannot send liveness {:?} result {:?}", liveness, res),
                        _ => {},
                    }
                }
            }
        }
    }

    async fn run_checks(&self, sender: mpsc::Sender<LivenessOutput>) {
        self.liveness.iter().enumerate().for_each(|(i, l)| {
            let liveness = l.clone();
            let sender = sender.clone();
            tokio::spawn(async move {
                Self::run_check(i, liveness, sender).await;
            });
        });
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
            tracing::warn!(
                "{}. Messenger is not configured for {}.",
                message,
                self.name()
            );
        }
    }

    fn liveness_output_to_datarow(&self, output: LivenessOutput) -> (usize, bool, Datarow) {
        let (index, probe_time, result) = output;
        let liveness = &self.liveness[index]; // safe as the index is collected from the same vector, but the vector shouldn't be changed
        let (is_alive, text) = match result {
            Ok(output) => (true, output),
            Err(output) => (false, output),
        };
        (
            index,
            is_alive,
            Datarow::new(
                liveness.to_string(),
                probe_time.naive_utc(),
                vec![
                    ("is_alive".to_string(), Datavalue::Bool(is_alive)),
                    ("output".to_string(), Datavalue::Text(text)),
                ],
            ),
        )
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

    async fn run(&mut self, mut log: AppendableLog, mut shutdown: broadcast::Receiver<u16>) {
        log.healthcheck()
            .await
            .expect("failed to connect to Google API");
        tracing::info!(
            "running with spreadsheet {}, channel_capacity {}",
            self.spreadsheet_id(),
            self.channel_capacity
        );
        //  channel to collect liveness checks results
        let (tx, mut data_receiver) = mpsc::channel(self.channel_capacity);
        self.run_checks(tx).await;
        let mut liveness_previous_state = vec![false; self.liveness.len()];
        let mut push_interval = tokio::time::interval(self.push_interval);
        let mut accumulated_data = vec![];
        let mut is_first_iteration = true;
        loop {
            tokio::select! {
                result = shutdown.recv() => {
                    data_receiver.close(); // stop running probes
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
                            while let Some(output) = data_receiver.recv().await {
                                let (_, _, datarow) = self.liveness_output_to_datarow(output);
                                accumulated_data.push(datarow);
                            }
                            let _ = log.append(accumulated_data).await;
                        } => {
                            tracing::info!("{} service has successfully shutdowned", self.name());
                        }
                    }

                    return;
                },
                _ = push_interval.tick() => {
                    tracing::info!("appending to log {} rows", accumulated_data.len());
                    let _ = log.append(accumulated_data).await;
                    accumulated_data = vec![];
                }
                Some(output) = data_receiver.recv() => {
                    let (liveness_index, is_alive, mut datarow) = self.liveness_output_to_datarow(output);
                    if liveness_previous_state[liveness_index] != is_alive || is_first_iteration {
                        self.send_message(&self.liveness[liveness_index], is_alive, &mut datarow, &log).await;
                        liveness_previous_state[liveness_index] = is_alive;
                        is_first_iteration = false;
                    }
                    accumulated_data.push(datarow);
                }
            }
        }
    }
}
