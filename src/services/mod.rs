pub(crate) mod general;
pub(crate) mod healthcheck;
pub(crate) mod kv;
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
        // Default for General and KV services
        1
    }

    fn push_interval(&self) -> Duration {
        // Default for General and KV services
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
        // Default for General service
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::ceiled_division;
    use crate::services::general::GENERAL_SERVICE_NAME;
    use crate::spreadsheet::spreadsheet::SpreadsheetAPI;
    use crate::spreadsheet::tests::TestState;
    use crate::spreadsheet::Metadata;
    use crate::storage::{jitter_duration, Datavalue, Storage};
    use crate::tests::{TEST_HOST_ID, TEST_PROJECT_ID};
    use crate::{create_log, Sender};
    use chrono::Utc;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tokio::sync::{broadcast, mpsc};

    struct TestService {
        counter: Arc<AtomicUsize>,
    }

    const APPEND_DURATION_MS: u64 = 100;
    const SCRAPE_INTERVAL_MS: u64 = 20;

    #[async_trait]
    impl Service for TestService {
        fn name(&self) -> &str {
            GENERAL_SERVICE_NAME
        }

        fn spreadsheet_id(&self) -> &str {
            "spreadsheet1"
        }

        fn channel_capacity(&self) -> usize {
            // during appending we accumulate
            5 * ceiled_division(APPEND_DURATION_MS as u16, SCRAPE_INTERVAL_MS as u16) as usize
        }

        fn push_interval(&self) -> Duration {
            Duration::from_millis(50)
        }

        async fn process_task_result(&mut self, result: TaskResult, _log: &AppendableLog) -> Data {
            let TaskResult { result, .. } = result;
            result.expect("test assert: ok result is sent")
        }

        async fn spawn_tasks(&mut self, sender: mpsc::Sender<TaskResult>) -> Vec<JoinHandle<()>> {
            let counter = self.counter.clone();
            vec![tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(SCRAPE_INTERVAL_MS));
                loop {
                    tokio::select! {
                        _ = sender.closed() => {
                            return;
                        },
                        _ = interval.tick() => {
                            let result = Ok(Data::Single(
                                Datarow::new(
                                    "log_name1".to_string(),
                                    Utc::now().naive_utc(),
                                    vec![
                                        (format!("key11"), Datavalue::HeatmapPercent(3_f64)),
                                        (format!("key12"), Datavalue::Size(400_u64)),
                                    ],
                                    None,
                                ),
                            ));
                            counter.fetch_add(1, Ordering::SeqCst);
                            sender.try_send(TaskResult{id: 0, result}).expect("test assert: should be able to send task result");
                        }
                    }
                }
            })]
        }
    }

    #[tokio::test]
    async fn basic_service_flow() {
        let (tx, _) = mpsc::channel(1);
        let tx = Sender::new(tx);
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
            TEST_PROJECT_ID.to_string(),
            sheets_api,
            tx.clone(),
        ));
        let log = create_log(
            storage.clone(),
            "spreadsheet1".to_string(),
            GENERAL_SERVICE_NAME.to_string(),
        );

        let (shutdown, rx) = broadcast::channel(1);

        let counter = data_counter.clone();
        let service = tokio::spawn(async move {
            let mut service = TestService { counter };
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

        let (all_sheets, _, _) = storage
            .google()
            .sheets_filtered_by_metadata("spreadsheet1", &Metadata::new(vec![]))
            .await
            .unwrap();
        assert_eq!(
            all_sheets.len(),
            1,
            "only a sheet for `log_name1` should be created"
        );
        assert!(all_sheets[0].title().contains("log_name1"));
        assert_eq!(
            all_sheets[0].row_count(),
            Some(data_counter.load(Ordering::SeqCst) as i32 + 1), // one row for header row
            "`log_name1` contains all data generated by the service"
        );
    }
}
