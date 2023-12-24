use crate::configuration::APP_NAME;
use crate::HttpsClient;
use hyper::{
    body::{Body, HttpBody as _},
    header, Client, Request, StatusCode, Uri,
};
use std::time::Duration;

/// Http(s) client for purposes of services
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

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Body, Method, Request, Response, Server, StatusCode};

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
}
