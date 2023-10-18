pub(crate) mod general;
pub(crate) mod healthcheck;
pub(crate) mod logs;
pub(crate) mod metrics;
pub(crate) mod system;

use crate::storage::{AppendableLog, Datarow};
use crate::HttpsClient;
use async_trait::async_trait;
use futures::future::try_join_all;
use hyper::{body::HttpBody as _, Client, StatusCode, Uri};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

#[derive(Debug)]
pub enum Data {
    Empty,
    Single(Datarow),
    Many(Vec<Datarow>),
    Message(String),
}

#[derive(Debug)]
pub struct TaskResult {
    id: usize,
    result: Result<Data, Data>,
}

#[async_trait]
pub trait Service {
    fn name(&self) -> &str;

    fn spreadsheet_id(&self) -> &str;

    fn channel_capacity(&self) -> usize {
        // TODO remove after all implementations
        1
    }

    fn push_interval(&self) -> Duration {
        // TODO remove after all implementations ??
        Duration::from_secs(u64::MAX)
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

    async fn spawn_tasks(&mut self, _sender: mpsc::Sender<TaskResult>) -> Vec<JoinHandle<()>> {
        // TODO remove after all implementations
        std::future::pending::<()>().await;
        vec![]
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
        let tasks = try_join_all(self.spawn_tasks(tx).await);
        tokio::pin!(tasks);
        let mut push_interval = tokio::time::interval(self.push_interval());
        let mut accumulated_data = vec![];
        loop {
            tokio::select! {
                result = shutdown.recv() => {
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
                    let _ = log.append(accumulated_data).await;
                    accumulated_data = vec![];
                }
                Some(task_result) = data_receiver.recv() => {
                    let data = self.process_task_result(task_result, &log).await;
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
            .get(self.url.clone())
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
        if res.status() >= StatusCode::OK || res.status() < StatusCode::BAD_REQUEST {
            Ok(text)
        } else {
            Err(text)
        }
    }
}
