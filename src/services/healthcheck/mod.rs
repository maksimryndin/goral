pub(crate) mod configuration;

use crate::messenger::configuration::MessengerConfig;

use crate::configuration::APP_NAME;
use crate::services::healthcheck::configuration::{
    scrape_push_rule, Healthcheck, Liveness as LivenessConfig, LivenessType,
};
use crate::services::{Data, HttpClient, Service, TaskResult};
use crate::spreadsheet::datavalue::{Datarow, Datavalue};
use crate::storage::AppendableLog;
use crate::{Sender, Shared};
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use hyper::Uri;
use std::fmt::{self, Debug, Display};
use std::net::SocketAddr;
use std::process::Stdio;
use std::result::Result as StdResult;
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::net::TcpSocket;
use tokio::process::Command;
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::task::JoinHandle;
use tonic::transport::Endpoint;
use tonic_health::pb::{
    health_check_response::ServingStatus, health_client::HealthClient, HealthCheckRequest,
};
use tracing::Level;

pub const HEALTHCHECK_SERVICE_NAME: &str = "health";
const MAX_BYTES_LIVENESS_OUTPUT: usize = 1024;

#[derive(Clone, Debug)]
enum Probe {
    Http(Uri),
    Tcp(SocketAddr),
    Grpc(Endpoint),
    Command(Vec<String>),
}

#[derive(Clone)]
struct Liveness {
    name: Option<String>,
    initial_delay: Duration,
    period: Duration,
    timeout: Duration,
    probe: Probe,
}

impl Debug for Liveness {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(name) = self.name.as_ref() {
            write!(f, "{name} ({:?})", self.probe)
        } else {
            write!(f, "{:?}", self.probe)
        }
    }
}

impl Display for Liveness {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(name) = self.name.as_ref() {
            return write!(f, "{name}");
        }
        match &self.probe {
            Probe::Http(url) => write!(f, "{url}"),
            Probe::Grpc(endpoint) => write!(f, "{}", endpoint.uri()),
            Probe::Tcp(sock) => write!(f, "{sock}"),
            Probe::Command(c) => write!(f, "{}", c.join(" ")),
        }
    }
}

impl From<LivenessConfig> for Liveness {
    fn from(mut c: LivenessConfig) -> Self {
        let timeout = Duration::from_millis(c.timeout_ms.into());
        let endpoint = c.endpoint.take();
        let probe = match c.typ {
            LivenessType::Http => Probe::Http(
                endpoint
                    .map(|s| {
                        Uri::from_maybe_shared(s.into_bytes())
                            .expect("assert: endpoint url is validated in configuration")
                    })
                    .expect("assert: endpoint is set for http probe"),
            ),
            LivenessType::Tcp => Probe::Tcp(
                SocketAddr::from_str(
                    &endpoint
                        .expect("assert: endpoint is set for tcp probe")
                        .to_string(),
                )
                .expect("assert: tcp address is validated at configuration"),
            ),
            LivenessType::Grpc => Probe::Grpc(
                endpoint
                    .map(|s| {
                        Endpoint::from_shared(s)
                            .expect("assert: grpc endpoint is validated at configuration")
                            .connect_timeout(timeout)
                            .timeout(timeout)
                            .user_agent(APP_NAME)
                            .expect("assert: can set user agent")
                    })
                    .expect("assert: endpoint is set for grpc probe"),
            ),
            LivenessType::Command => {
                Probe::Command(c.command.expect("assert: command is set for command probe"))
            }
        };

        Self {
            name: c.name,
            initial_delay: Duration::from_secs(c.initial_delay_secs.into()),
            period: Duration::from_secs(c.period_secs.into()),
            timeout,
            probe,
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
        match &liveness.probe {
            Probe::Http(url) => {
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
            Probe::Command(command) => {
                let mut args = command.clone();
                match Command::new(&args[0])
                    .args(&mut args[1..])
                    .stdin(Stdio::null())
                    .output()
                    .await
                {
                    Ok(output) if output.status.success() => {
                        Ok(String::from_utf8_lossy(&output.stdout).to_string())
                    }
                    Ok(output) => Err(String::from_utf8_lossy(&output.stderr).to_string()),
                    Err(e) => Err(format!("failed to execute {command:?} with error {e}")),
                }
            }
            Probe::Tcp(addr) => {
                let socket = match addr {
                    SocketAddr::V4(_) => TcpSocket::new_v4()
                        .expect("assert: should be able to create tcp IPv4 sockets"),
                    SocketAddr::V6(_) => TcpSocket::new_v6()
                        .expect("assert: should be able to create tcp IPv6 sockets"),
                };
                socket
                    .connect(*addr)
                    .await
                    .map(|stream| {
                        format!(
                            "connected to tcp socket {}",
                            stream
                                .peer_addr()
                                .expect("assert: can obtain tcp socket peer address")
                        )
                    })
                    .map_err(|e| e.to_string())
            }
            Probe::Grpc(endpoint) => {
                let conn = endpoint.connect().await.map_err(|e| e.to_string())?;
                let mut client = HealthClient::new(conn);
                match client
                    .check(HealthCheckRequest {
                        service: "".to_string(),
                    })
                    .await
                {
                    Ok(response) => match response.into_inner().status() {
                        ServingStatus::Serving => Ok("".to_string()),
                        s => Err(s.as_str_name().to_string()),
                    },
                    Err(status) => Err(status.to_string()),
                }
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
        )
    }

    async fn run_check(
        is_shutdown: Arc<AtomicBool>,
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
                            if is_shutdown.load(Ordering::Relaxed) {
                                tracing::info!("finished check for {:?}", liveness);
                                return;
                            }
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
                "Liveness probe for `{:?}` failed with an output at the [spreadsheet]({}), the sheet may be created a bit later",
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

    fn shared(&self) -> &Shared {
        &self.shared
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

    async fn spawn_tasks(
        &mut self,
        is_shutdown: Arc<AtomicBool>,
        sender: mpsc::Sender<TaskResult>,
    ) -> Vec<JoinHandle<()>> {
        self.liveness
            .iter()
            .enumerate()
            .map(|(i, l)| {
                let liveness = l.clone();
                let sender = sender.clone();
                let is_shutdown = is_shutdown.clone();
                let send_notification = self.shared.send_notification.clone();
                tokio::spawn(async move {
                    Self::run_check(is_shutdown, i, liveness, sender, send_notification).await;
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::tests::{run_server, HEALTHY_REPLY, UNHEALTHY_REPLY};
    use std::net::TcpListener;
    use tokio::sync::mpsc;

    #[test]
    fn liveness_name_usage() {
        let liveness = Liveness {
            name: Some("check1".to_string()),
            initial_delay: Duration::from_secs(0),
            period: Duration::from_secs(1),
            timeout: Duration::from_millis(10),
            probe: Probe::Http(Uri::from_static("http://example.com/foo")),
        };
        assert!(
            liveness.to_string().contains("check1"),
            "if name is provided, it should be used for Display: {}",
            liveness.to_string()
        );
    }

    #[test]
    fn liveness_default_name_for_command() {
        let liveness = Liveness {
            name: None,
            initial_delay: Duration::from_secs(0),
            period: Duration::from_secs(1),
            timeout: Duration::from_millis(10),
            probe: Probe::Command(vec!["ls".to_string(), "-lha".to_string()]),
        };
        assert!(
            liveness.to_string().contains("ls -lha"),
            "the command itself is be used for Display if no name is provided: {}",
            liveness.to_string()
        );
    }

    #[test]
    fn liveness_default_name_for_tcp() {
        let liveness = Liveness {
            name: None,
            initial_delay: Duration::from_secs(0),
            period: Duration::from_secs(1),
            timeout: Duration::from_millis(10),
            probe: Probe::Tcp("127.0.0.1:53258".parse::<SocketAddr>().unwrap()),
        };
        assert!(
            liveness.to_string().contains("127.0.0.1:53258"),
            "the tcp sock addr is be used for Display if no name is provided: {}",
            liveness.to_string()
        );
    }

    #[tokio::test]
    #[should_panic(expected = "probe timeout 10ms")]
    async fn probe_timeout() {
        let liveness = Liveness {
            name: None,
            initial_delay: Duration::from_secs(0),
            period: Duration::from_secs(1),
            timeout: Duration::from_millis(10),
            probe: Probe::Http(Uri::from_static("http://nonexisting.com/foo")),
        };
        HealthcheckService::is_alive(&liveness).await.unwrap();
    }

    #[tokio::test]
    async fn http_probe() {
        tokio::spawn(async {
            run_server(53254).await;
        });

        const NUM_OF_PROBES: usize = 1;
        let (send_notification, mut notifications_receiver) = mpsc::channel(1);
        let send_notification = Sender::new(send_notification);
        let (data_sender, mut data_receiver) = mpsc::channel(NUM_OF_PROBES);
        let is_shutdown = Arc::new(AtomicBool::new(false));

        let liveness = Liveness {
            name: Some("test_check".to_string()),
            initial_delay: Duration::from_secs(0),
            period: Duration::from_secs(1),
            timeout: Duration::from_millis(50),
            probe: Probe::Http(Uri::from_static("http://127.0.0.1:53254/health")),
        };

        let notifications = tokio::spawn(async move {
            while let Some(notification) = notifications_receiver.recv().await {
                println!("Notification received: {notification:?}");
            }
        });

        let is_shutdown_clone = is_shutdown.clone();
        let checker_handle = tokio::spawn(async move {
            HealthcheckService::run_check(
                is_shutdown_clone,
                0,
                liveness,
                data_sender,
                send_notification,
            )
            .await;
        });

        tokio::time::sleep(Duration::from_secs(NUM_OF_PROBES as u64)).await;
        is_shutdown.store(true, Ordering::Release);
        data_receiver.close();

        if let Some(TaskResult {
            result: Ok(Data::Single(datarow)),
            ..
        }) = data_receiver.recv().await
        {
            assert_eq!(
                datarow.keys_values().get("output"),
                Some(&Datavalue::Text(HEALTHY_REPLY.to_string()))
            );
        } else {
            panic!("test assert: at least one successfull probe should be collected");
        }

        checker_handle.await.unwrap(); // checker should finish as the data channel is closed
        notifications.await.unwrap();
    }

    #[tokio::test]
    async fn http_probe_failure() {
        tokio::spawn(async {
            run_server(53255).await;
        });

        const NUM_OF_PROBES: usize = 2;
        let (send_notification, mut notifications_receiver) = mpsc::channel(NUM_OF_PROBES);
        let send_notification = Sender::new(send_notification);
        let (data_sender, mut data_receiver) = mpsc::channel(NUM_OF_PROBES);
        let is_shutdown = Arc::new(AtomicBool::new(false));

        let liveness = Liveness {
            name: Some("test_check".to_string()),
            initial_delay: Duration::from_secs(0),
            period: Duration::from_secs(1),
            timeout: Duration::from_millis(50),
            probe: Probe::Http(Uri::from_static("http://127.0.0.1:53255/unhealthy")),
        };

        let notifications = tokio::spawn(async move {
            while let Some(notification) = notifications_receiver.recv().await {
                println!("Notification received: {notification:?}");
            }
        });

        let is_shutdown_clone = is_shutdown.clone();
        let checker_handle = tokio::spawn(async move {
            HealthcheckService::run_check(
                is_shutdown_clone,
                0,
                liveness,
                data_sender,
                send_notification,
            )
            .await;
        });

        tokio::time::sleep(Duration::from_secs(NUM_OF_PROBES as u64)).await;
        is_shutdown.store(true, Ordering::Release);
        data_receiver.close();

        if let Some(TaskResult {
            result: Err(Data::Single(datarow)),
            ..
        }) = data_receiver.recv().await
        {
            assert_eq!(
                datarow.keys_values().get("output"),
                Some(&Datavalue::Text(UNHEALTHY_REPLY.to_string()))
            );
        } else {
            panic!("test assert: at least one unsuccessfull probe should be collected");
        }

        if let Some(TaskResult {
            result: Err(Data::Single(datarow)),
            ..
        }) = data_receiver.recv().await
        {
            assert_eq!(
                datarow.keys_values().get("output"),
                Some(&Datavalue::Text(UNHEALTHY_REPLY.to_string()))
            );
        } else {
            panic!("test assert: second unsuccessfull probe should be collected");
        }

        checker_handle.await.unwrap(); // checker should finish as the data channel is closed
        notifications.await.unwrap();
    }

    #[tokio::test]
    async fn http_probe_initial_delay() {
        let delay = Duration::from_millis(50);
        let cloned_delay = delay.clone();
        tokio::spawn(async move {
            tokio::time::sleep(cloned_delay).await;
            run_server(53256).await;
        });

        const NUM_OF_PROBES: usize = 1;
        let (send_notification, mut notifications_receiver) = mpsc::channel(1);
        let send_notification = Sender::new(send_notification);
        let (data_sender, mut data_receiver) = mpsc::channel(NUM_OF_PROBES);
        let is_shutdown = Arc::new(AtomicBool::new(false));

        let liveness = Liveness {
            name: Some("test_check".to_string()),
            initial_delay: delay,
            period: Duration::from_secs(1),
            timeout: Duration::from_millis(50),
            probe: Probe::Http(Uri::from_static("http://127.0.0.1:53256/health")),
        };

        let notifications = tokio::spawn(async move {
            while let Some(notification) = notifications_receiver.recv().await {
                println!("Notification received: {notification:?}");
            }
        });

        let is_shutdown_clone = is_shutdown.clone();
        let checker_handle = tokio::spawn(async move {
            HealthcheckService::run_check(
                is_shutdown_clone,
                0,
                liveness,
                data_sender,
                send_notification,
            )
            .await;
        });

        tokio::time::sleep(Duration::from_secs(NUM_OF_PROBES as u64)).await;
        is_shutdown.store(true, Ordering::Release);
        data_receiver.close();

        if let Some(TaskResult {
            result: Ok(Data::Single(datarow)),
            ..
        }) = data_receiver.recv().await
        {
            assert_eq!(
                datarow.keys_values().get("output"),
                Some(&Datavalue::Text(HEALTHY_REPLY.to_string()))
            );
        } else {
            panic!("test assert: at least one successfull probe should be collected");
        }

        checker_handle.await.unwrap(); // checker should finish as the data channel is closed
        notifications.await.unwrap();
    }

    #[tokio::test]
    async fn command_probe() {
        // echo command is common for Unix and Windows platforms
        let liveness = Liveness {
            name: None,
            initial_delay: Duration::from_secs(0),
            period: Duration::from_secs(1),
            timeout: Duration::from_millis(30),
            probe: Probe::Command(vec!["echo".to_string(), "goral".to_string()]),
        };
        // we trim as Windows and Unix have different line separators
        assert_eq!(
            HealthcheckService::is_alive(&liveness)
                .await
                .unwrap()
                .trim(),
            "goral".to_string()
        );
    }

    #[tokio::test]
    async fn tcp_probe() {
        let server = tokio::task::spawn_blocking(|| {
            let listener = TcpListener::bind("127.0.0.1:53257")
                .expect("test assert: should be able to create a listening tcp socket");
            for _stream in listener.incoming() {
                //accept just one connection
                return;
            }
        });
        tokio::time::sleep(Duration::from_millis(50)).await; // some time for a thread to start
        let liveness = Liveness {
            name: None,
            initial_delay: Duration::from_millis(0),
            period: Duration::from_secs(1),
            timeout: Duration::from_millis(10),
            probe: Probe::Tcp("127.0.0.1:53257".parse::<SocketAddr>().unwrap()),
        };
        HealthcheckService::is_alive(&liveness).await.unwrap();
        server.await.unwrap();
    }

    use tonic::{transport::Server, Request, Response, Status};
    // test.rs is generated from the proto file
    // syntax = "proto3";

    // package test;

    // service Test {
    // rpc call(Input) returns (Output);
    // }

    // message Input {}
    // message Output {}
    mod pb {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/services/healthcheck/test.rs"
        ));
    }

    #[derive(Default)]
    pub struct GrpcService {}

    #[tonic::async_trait]
    impl pb::test_server::Test for GrpcService {
        async fn call(&self, _request: Request<pb::Input>) -> Result<Response<pb::Output>, Status> {
            let reply = pb::Output {};
            Ok(Response::new(reply))
        }
    }

    #[tokio::test]
    async fn grpc_probe() {
        tokio::spawn(async {
            let (mut health_reporter, health_service) = tonic_health::server::health_reporter();

            health_reporter
                .set_serving::<pb::test_server::TestServer<GrpcService>>()
                .await;
            let addr = "[::1]:53258".parse().unwrap();
            let service = GrpcService::default();

            println!("HealthServer + TestServer listening on {}", addr);

            Server::builder()
                .add_service(health_service)
                .add_service(pb::test_server::TestServer::new(service))
                .serve(addr)
                .await
                .expect("test assert: able to run grpc service");
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        let liveness = Liveness {
            name: None,
            initial_delay: Duration::from_millis(0),
            period: Duration::from_secs(1),
            timeout: Duration::from_millis(50),
            probe: Probe::Grpc(Endpoint::from_static("http://[::1]:53258")),
        };
        HealthcheckService::is_alive(&liveness).await.unwrap();
    }
}
