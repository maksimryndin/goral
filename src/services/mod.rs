pub(crate) mod general;
pub(crate) mod healthcheck;
pub(crate) mod logs;
pub(crate) mod metrics;
pub(crate) mod resources;

use crate::storage::AppendableLog;
use crate::HttpsClient;
use async_trait::async_trait;
use hyper::{body::HttpBody as _, Client, StatusCode, Uri};
use std::time::Duration;
use tokio::sync::broadcast::Receiver;

#[async_trait]
pub trait Service {
    fn name(&self) -> &str;

    fn spreadsheet_id(&self) -> &str;

    async fn run(&mut self, _appendable_log: AppendableLog, mut shutdown: Receiver<u16>) {
        loop {
            tokio::select! {
                result = shutdown.recv() => {
                    let graceful_shutdown_timeout = match result {
                        Err(_) => panic!("assert: shutdown signal sender should be dropped after all service listeneres"),
                        Ok(graceful_shutdown_timeout) => graceful_shutdown_timeout,
                    };
                    tracing::info!("{} service has got shutdown signal", self.name());
                    return;
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
