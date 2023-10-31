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
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::mpsc::{self, error::TrySendError};
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

    async fn spawn_tasks(
        &mut self,
        _shutdown: Arc<AtomicBool>,
        _sender: mpsc::Sender<TaskResult>,
    ) -> Vec<JoinHandle<()>> {
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
        let is_shutdown = Arc::new(AtomicBool::new(false));
        let tasks = try_join_all(self.spawn_tasks(is_shutdown.clone(), tx).await);
        tokio::pin!(tasks);
        let mut push_interval = tokio::time::interval(self.push_interval());
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
        if res.status() >= StatusCode::OK && res.status() < StatusCode::BAD_REQUEST {
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
    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Body, Method, Request, Response, Server, StatusCode};
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
                                    None,
                                ),
                            ));

                            match sender.try_send(TaskResult{id: 0, result}) {
                                Err(TrySendError::Full(_)) => {
                                    panic!("test assert: messages queue is full");
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

    pub(crate) const HEALTHY_REPLY: &str = "Test service is healthy";
    pub(crate) const UNHEALTHY_REPLY: &str = "Test service is unhealthy";

    pub(crate) async fn router(req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
        match (req.method(), req.uri().path()) {
            (&Method::GET, "/health") => Ok(Response::new(Body::from(HEALTHY_REPLY))),
            (&Method::GET, "/unhealthy") => Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(UNHEALTHY_REPLY.into())
                .expect("test assert: should be able to construct response for static body")),
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
}
