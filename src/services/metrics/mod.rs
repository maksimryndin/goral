pub(crate) mod configuration;

use crate::messenger::configuration::MessengerConfig;
use crate::services::metrics::configuration::{scrape_push_rule, Metrics};
use crate::services::{Data, HttpClient, Service, TaskResult};
use crate::storage::{AppendableLog, Datarow, Datavalue};
use crate::{Sender, Shared};
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use hyper::Uri;
use prometheus_parse::{Sample, Scrape, Value};
use std::cmp::Ordering as Cmp;
use std::collections::HashMap;
use std::result::Result as StdResult;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::task::{self, JoinHandle};

pub const METRICS_SERVICE_NAME: &str = "metrics";
const MAX_BYTES_METRICS_OUTPUT: usize = 2_usize.pow(14); // ~16 KiB

#[derive(Debug)]
struct ScrapeTarget {
    url: Uri,
    timeout: Duration,
    interval: Duration,
}

pub(crate) struct MetricsService {
    shared: Shared,
    messenger_config: Option<MessengerConfig>,
    spreadsheet_id: String,
    push_interval: Duration,
    scrape_interval: Duration,
    scrape_timeout: Duration,
    endpoints: Vec<Uri>,
    channel_capacity: usize,
}

impl MetricsService {
    pub(crate) fn new(shared: Shared, config: Metrics) -> MetricsService {
        let channel_capacity = scrape_push_rule(
            &config.endpoints,
            &config.push_interval_secs,
            &config.scrape_interval_secs,
            &config.scrape_timeout_ms,
        )
        .expect("assert: push/scrate ratio is validated at configuration");
        Self {
            shared,
            messenger_config: config.messenger,
            spreadsheet_id: config.spreadsheet_id,
            push_interval: Duration::from_secs(config.push_interval_secs.into()),
            scrape_interval: Duration::from_secs(config.scrape_interval_secs.into()),
            scrape_timeout: Duration::from_millis(config.scrape_timeout_ms.into()),
            endpoints: config
                .endpoints
                .into_iter()
                .map(|u| {
                    let s: String = u.into();
                    Uri::from_maybe_shared(s.into_bytes())
                        .expect("assert: endpoint url is validated in configuration")
                })
                .collect(),
            channel_capacity,
        }
    }

    // sort samples for a neat order of keys
    // histogram/summary, then count, then sum
    fn sort_samples(samples: &mut Vec<Sample>) {
        samples.sort_unstable_by(|a, b| {
            if a.metric.ends_with("_count") && b.metric.ends_with("_sum") {
                return Cmp::Less;
            }
            if matches!(a.value, Value::Histogram(_) | Value::Summary(_))
                && (b.metric.ends_with("_sum") || b.metric.ends_with("_count"))
            {
                return Cmp::Less;
            }
            Cmp::Equal
        });
    }

    fn samples_to_datavalues(mut samples: Vec<Sample>) -> Vec<(String, Datavalue)> {
        use Value::*;
        Self::sort_samples(&mut samples);
        let first = samples
            .first()
            .expect("assert: each metric parsed should have at least one sample");
        let mut labels: Vec<(String, Datavalue)> = first
            .labels
            .iter()
            .map(|(k, v)| (k.to_string(), Datavalue::Text(v.to_string())))
            .collect();
        labels.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let mut values: Vec<(String, Datavalue)> = samples
            .into_iter()
            .map(|s| {
                let values: Vec<(String, Datavalue)> = match s.value {
                    Counter(v) | Gauge(v) | Untyped(v) => vec![(s.metric, Datavalue::Number(v))],
                    Histogram(buckets) => buckets
                        .into_iter()
                        .map(|b| (format!("le={}", b.less_than), Datavalue::Number(b.count)))
                        .collect(),
                    Summary(quantiles) => quantiles
                        .into_iter()
                        .map(|q| {
                            (
                                format!("quantile={}", q.quantile),
                                Datavalue::Number(q.count),
                            )
                        })
                        .collect(),
                };
                values
            })
            .flatten()
            .collect();
        values.append(&mut labels);
        values
    }

    fn parse_scrape_output(
        output: String,
        scrape_time: DateTime<Utc>,
        identifier: Option<String>,
    ) -> StdResult<Vec<Datarow>, String> {
        let lines: Vec<_> = output.lines().map(|s| Ok(s.to_string())).collect();
        let Scrape { docs, samples } =
            prometheus_parse::Scrape::parse_at(lines.into_iter(), scrape_time)
                .map_err(|e| e.to_string())?;
        let mut map = HashMap::with_capacity(samples.len());

        // see https://prometheus.io/docs/practices/histograms/#count-and-sum-of-observations
        // and https://prometheus.io/docs/concepts/metric_types/#summary
        // group all samples by their base metrics,
        // labels and timestamps to produce separate rows
        samples.into_iter().for_each(|s| {
            let name = if docs.contains_key(&s.metric) {
                &s.metric
            } else if let Some((metric_basename, _)) = s.metric.rsplit_once("_count") {
                if docs.contains_key(metric_basename) {
                    metric_basename
                } else {
                    &s.metric
                }
            } else if let Some((metric_basename, _)) = s.metric.rsplit_once("_sum") {
                if docs.contains_key(metric_basename) {
                    metric_basename
                } else {
                    &s.metric
                }
            } else {
                panic!(
                    "assert: metric should either have some base metric or be a metric on its own"
                );
            };
            let mut kv: Vec<&str> = s
                .labels
                .iter()
                .map(|(k, v)| [k.as_str(), v.as_str()])
                .flatten()
                .collect();
            kv.sort_unstable();
            let key = kv.join("");
            map.entry((name.to_string(), key, s.timestamp))
                .or_insert(vec![])
                .push(s);
        });

        // Label keys are fixed in each scrape output
        // see https://prometheus.io/docs/instrumenting/writing_clientlibs/#labels
        // and https://github.com/prometheus/client_java/issues/696
        Ok(map
            .into_iter()
            .map(|((metric_name, _, timestamp), samples)| {
                let values = Self::samples_to_datavalues(samples);
                let note = docs
                    .get(&metric_name)
                    .expect("assert: all metrics are in docs by their basenames");
                let log_name = if let Some(prefix) = identifier.as_ref() {
                    // Prmotheus metric name cannot contain `:` so we use it as a delimiter
                    // see https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
                    format!("{prefix}:{metric_name}")
                } else {
                    metric_name
                };
                Datarow::new(
                    log_name,
                    timestamp.naive_utc(),
                    values,
                    Some(note.to_string()),
                )
            })
            .collect())
    }

    async fn make_timed_scrape(
        client: &HttpClient,
        target: &ScrapeTarget,
    ) -> StdResult<String, String> {
        tokio::select! {
            _ = tokio::time::sleep(target.timeout) => Err(format!("scrape timeout {:?} for {}", target.timeout, target.url)),
            res = client.get() => res
        }
    }

    async fn run_scrape(
        client: HttpClient,
        index: usize,
        scrape_target: ScrapeTarget,
        sender: mpsc::Sender<TaskResult>,
        send_notification: Sender,
        identifier: Option<String>,
    ) {
        let mut interval = tokio::time::interval(scrape_target.interval);
        tracing::info!("starting metrics scraping for {:?}", scrape_target.url);
        loop {
            tokio::select! {
                _ = sender.closed() => {
                    tracing::info!("finished scraping for {:?}", scrape_target.url);
                    return;
                },
                _ = interval.tick() => {
                    tracing::trace!("metrics scrape for {:?}", scrape_target.url);
                    let scrape_time = Utc::now();
                    let scrape_result = Self::make_timed_scrape(&client, &scrape_target).await;
                    let result = match scrape_result {
                        Ok(output) => {
                            tracing::info!("starting metrics parsing for {:?}", scrape_target.url);
                            let identifier = identifier.clone();
                            let join_result = task::spawn_blocking(move || {
                                Self::parse_scrape_output(output, scrape_time, identifier)
                            }).await;
                            tracing::info!("finished metrics parsing for {:?}", scrape_target.url);
                            match join_result {
                                Err(e) => {
                                    let msg = "failed to spawn parsing of metrics output in a separate thread".to_string();
                                    tracing::error!("{}: `{}`", msg, e);
                                    send_notification.fatal(msg).await;
                                    panic!("assert: should be able to spawn blocking tasks");
                                },
                                Ok(res) => res.map(|d| Data::Many(d)).map_err(|e| Data::Message(format!("error scraping metrics `{e}`"))),
                            }
                        },
                        err => {
                            tracing::debug!("metrics scrape result for {:?} is an error: {:?}", scrape_target.url, err);
                            err.map(|_| Data::Many(vec![])).map_err(|t| Data::Message(t))
                        },
                    };

                    match sender.try_send(TaskResult{id: index, result}) {
                        Err(TrySendError::Full(res)) => {
                            let msg = "scrape messages queue is full so increase scrape interval and decrease push interval".to_string();
                            tracing::error!("{}. Cannot send scrape target `{:?}` result `{:?}`", msg, scrape_target, res);
                            send_notification.try_error(msg);
                        },
                        Err(TrySendError::Closed(res)) => {
                            let msg = "scrape messages queue has been unexpectedly closed".to_string();
                            tracing::error!("{}: cannot send scrape target {:?} result {:?}", msg, scrape_target, res);
                            send_notification.fatal(msg).await;
                            panic!("assert: scrape messages queue shouldn't be closed before shutdown signal");
                        },
                        _ => {},
                    }
                }
            }
        }
    }

    async fn send_error(&self, endpoint: &Uri, message: &str) {
        let message = format!(
            "`{}` while scraping metrics for endpoint `{}`",
            message, endpoint
        );
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
impl Service for MetricsService {
    fn name(&self) -> &str {
        METRICS_SERVICE_NAME
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

    async fn process_task_result(&mut self, result: TaskResult, _: &AppendableLog) -> Data {
        let TaskResult { id, result } = result;
        match result {
            Ok(data) => data,
            Err(Data::Message(msg)) => {
                tracing::error!("{}", msg);
                self.send_error(&self.endpoints[id], &msg).await;
                Data::Empty
            }
            _ => panic!("assert: metrics result contains either multiple datarows or error text"),
        }
    }

    async fn spawn_tasks(
        &mut self,
        shutdown: Arc<AtomicBool>,
        sender: mpsc::Sender<TaskResult>,
    ) -> Vec<JoinHandle<()>> {
        self.endpoints
            .iter()
            .enumerate()
            .map(|(index, u)| {
                let url = u.clone();
                let sender = sender.clone();
                let interval = self.scrape_interval.clone();
                let identifier = if self.endpoints.len() > 1 {
                    Some(
                        url.port()
                            .expect("assert: metric endpoint port is validated at configuration")
                            .to_string(),
                    )
                } else {
                    None
                };
                let scrape_target = ScrapeTarget {
                    url: url.clone(),
                    interval,
                    timeout: self.scrape_timeout,
                };
                let client =
                    HttpClient::new(MAX_BYTES_METRICS_OUTPUT, true, self.scrape_interval, url);
                let send_notification = self.shared.send_notification.clone();
                tokio::spawn(async move {
                    Self::run_scrape(
                        client,
                        index,
                        scrape_target,
                        sender,
                        send_notification,
                        identifier,
                    )
                    .await;
                })
            })
            .collect()
    }
}
