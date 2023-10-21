pub(crate) mod configuration;
use crate::messenger::configuration::MessengerConfig;

use crate::services::kv::configuration::Kv;
use crate::services::Service;
use crate::spreadsheet::HttpResponse;
use crate::storage::{AppendableLog, Datarow, Datavalue};
use crate::{Sender, Shared};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hyper::service::{make_service_fn, service_fn};
use hyper::{
    body::Buf, header, Body, Method, Request as HyperRequest, Response as HyperResponse, Server,
    StatusCode,
};
use serde::{Deserialize, Serialize};
use serde_valid::Validate;
use std::collections::HashSet;
use std::time::Duration;

use tokio::sync::broadcast;

use tokio::sync::mpsc::{self};
use tokio::sync::oneshot;
use tokio::task::{JoinHandle};

pub const KV_SERVICE_NAME: &str = "kv";

fn unique_keys(data: &Vec<(String, Value)>) -> Result<(), serde_valid::validation::Error> {
    let keys: HashSet<&String> = data.into_iter().map(|(k, _)| k).collect();
    if keys.len() < data.len() {
        Err(serde_valid::validation::Error::Custom(
            "`data` should have unique keys".to_string(),
        ))
    } else {
        Ok(())
    }
}

// TODO test for proper serialization
// for untagged enum an order is important
// https://serde.rs/enum-representations.html#untagged
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Value {
    Integer(u64),
    Number(f64),
    Bool(bool),
    Datetime(DateTime<Utc>),
    Text(String),
}

impl Into<Datavalue> for Value {
    fn into(self) -> Datavalue {
        use Value::*;
        match self {
            Integer(v) => Datavalue::Integer(v),
            Number(v) => Datavalue::Number(v),
            Bool(v) => Datavalue::Bool(v),
            Datetime(v) => Datavalue::Datetime(v.naive_utc()),
            Text(v) => Datavalue::Text(v),
        }
    }
}

#[derive(Debug, Deserialize, Validate)]
#[rule(unique_keys(data))]
struct KVRow {
    log_name: String,
    datetime: DateTime<Utc>,
    #[validate(custom(unique_keys))]
    #[validate(min_items = 1)]
    data: Vec<(String, Value)>,
}

impl Into<Datarow> for KVRow {
    fn into(self) -> Datarow {
        Datarow::new(
            self.log_name,
            self.datetime.naive_utc(),
            self.data.into_iter().map(|(k, v)| (k, v.into())).collect(),
            None,
        )
    }
}

#[derive(Debug, Deserialize, Validate)]
struct Request {
    #[validate]
    rows: Vec<KVRow>,
}

#[derive(Debug, Serialize)]
struct Response {
    sheet_urls: Vec<String>,
}

struct AppendRequest {
    datarows: Vec<Datarow>,
    reply_to: oneshot::Sender<Result<Vec<String>, HttpResponse>>,
}

pub(crate) struct KvService {
    shared: Shared,
    spreadsheet_id: String,
    messenger_config: Option<MessengerConfig>,
    port: u16,
}

impl KvService {
    pub(crate) fn new(shared: Shared, config: Kv) -> KvService {
        Self {
            shared,
            spreadsheet_id: config.spreadsheet_id,
            port: config.port,
            messenger_config: config.messenger,
        }
    }

    async fn router(
        req: HyperRequest<Body>,
        data_bus: mpsc::Sender<AppendRequest>,
        send_notification: Sender,
    ) -> Result<HyperResponse<Body>, hyper::Error> {
        match (req.method(), req.uri().path()) {
            (&Method::POST, "/api/kv") => {
                let body = hyper::body::aggregate(req).await?;
                let req_body: Request = match serde_json::from_reader(body.reader()) {
                    Err(e) => {
                        return Ok(HyperResponse::builder()
                        .status(StatusCode::UNPROCESSABLE_ENTITY)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(e.to_string().into())
                        .expect("assert: should be able to construct response for invalid kv request"));
                    }
                    Ok(b) => b,
                };
                let (reply_to, rx) = oneshot::channel();

                let datarows = req_body
                    .rows
                    .into_iter()
                    .map(|kvrow| kvrow.into())
                    .collect();
                if let Err(e) = data_bus.send(AppendRequest { datarows, reply_to }).await {
                    let msg =
                        "kv service data messages queue has been unexpectedly closed".to_string();
                    tracing::error!("{}: {}", msg, e);
                    send_notification.fatal(msg).await;
                    panic!("assert: kv service data messages queue shouldn't be closed before shutdown signal");
                }

                let sheet_urls = match rx.await {
                    Err(e) => {
                        let msg = format!("kv service failed to get a result of append `{e}`");
                        tracing::error!("{}", msg);
                        send_notification.fatal(msg).await;
                        panic!("assert: kv service should be able to get a result of append");
                    }
                    Ok(Err(http_response)) => {
                        return Ok(HyperResponse::builder()
                            .status(
                                StatusCode::from_u16(http_response.status().as_u16())
                                    .expect("assert: http status codes from http crate are valid"),
                            )
                            .header(header::CONTENT_TYPE, "application/json")
                            .body(http_response.into_body())
                            .expect("assert: should be able to reuse http response body"));
                    }
                    Ok(Ok(sheet_urls)) => sheet_urls,
                };

                let json = serde_json::to_string(&Response { sheet_urls })
                    .expect("assert: should be able to serialize vector of sheet urls");
                let response = HyperResponse::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(json))
                    .expect("assert: should be able to serialize append return value");
                Ok(response)
            }
            _ => Ok(HyperResponse::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())
                .expect("assert: should be able to construct empty response body")),
        }
    }

    async fn start_server(
        &self,
        mut shutdown: broadcast::Receiver<u16>,
        data_bus: mpsc::Sender<AppendRequest>,
    ) -> JoinHandle<()> {
        let addr = ([127, 0, 0, 1], self.port).into();

        let send_notification = self.shared.send_notification.clone();
        let data_bus = data_bus.clone();
        let server = Server::bind(&addr).serve(make_service_fn(move |_| {
            let send_notification = send_notification.clone();
            let data_bus = data_bus.clone();
            async move {
                Ok::<_, hyper::Error>(service_fn(move |req| {
                    let send_notification = send_notification.clone();
                    let data_bus = data_bus.clone();
                    Self::router(req, data_bus, send_notification)
                }))
            }
        }));

        let server = server.with_graceful_shutdown(async move {
            tracing::info!("KV server is shutting down");
            shutdown.recv().await.ok();
        });

        tokio::spawn(async move {
            tracing::info!("KV server is listening on http://{}", addr);
            server.await.expect("cannot run KV server");
        })
    }
}

#[async_trait]
impl Service for KvService {
    fn name(&self) -> &str {
        KV_SERVICE_NAME
    }

    fn spreadsheet_id(&self) -> &str {
        self.spreadsheet_id.as_str()
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
        let (tx, mut data_receiver) = mpsc::channel(1);
        let server = self.start_server(shutdown.resubscribe(), tx).await;
        tokio::pin!(server);
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
                            while let Some(append_request) = data_receiver.recv().await {
                                let AppendRequest{datarows, reply_to} = append_request;
                                let res = log.append_no_retry(datarows).await;
                                if let Err(_) = reply_to.send(res) {
                                    tracing::warn!("client of the kv server dropped connection");
                                }
                            }
                        } => {
                            tracing::info!("{} service has successfully shutdowned", self.name());
                        }
                    }
                    return;
                },
                Some(append_request) = data_receiver.recv() => {
                    let AppendRequest{datarows, reply_to} = append_request;
                    let res = log.append_no_retry(datarows).await;
                    if let Err(_) = reply_to.send(res) {
                        tracing::warn!("client of the kv server dropped connection");
                    }
                }
                res = &mut server => {
                    res.unwrap(); // propagate panics from spawned server
                }
            }
        }
    }
}
