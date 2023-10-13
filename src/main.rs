#![forbid(unsafe_code)]

use clap::Parser;
use futures::future::join_all;
use goral::configuration::{Configuration, APP_NAME};
use goral::services::Service;
use goral::spreadsheet::{get_google_auth, SpreadsheetAPI};
use goral::storage::{create_log, Storage};
use goral::{collect_messengers, collect_services, Sender, Shared};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::{broadcast, mpsc};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[cfg(target_os = "linux")]
pub async fn sigterm() -> tokio::io::Result<()> {
    signal::unix::signal(signal::unix::SignalKind::terminate())?
        .recv()
        .await;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub async fn sigterm() -> tokio::io::Result<()> {
    future::pending().await;
    Ok(())
}

/// Goral is a modular observability toolkit for small projects.
/// Goral is a lightweight daemon process which
/// stores collected data in Google Spreadsheet so no
/// additional storage required.
/// Any Goral service is easily replaced with enterprise-grade
/// solution like Prometheus, Loki or Zabbix because it scrapes
/// data in common industry formats.
/// TODO include docs github page?
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to TOML configuration file. Environment variables starting with `GORAL_`
    /// (e.g. `GORAL__RESOURCES__ALERT_IF_FREE_DISK_SPACE_PERCENT_LESS_THAN`)
    /// overwrite config values.
    #[arg(short, long)]
    config: String,
    /// Host identifier. Keep this argument persistent for this instance of Goral
    /// to properly identify your apps running at this host.
    /// It should be unique among all your hosts where Gorals observe your apps
    /// as in case of the same spreadsheet used for several hosts the host id allows
    /// to separate sheets with data coming from different hosts.
    /// If you move your app to another machine and want to keep on using the same
    /// sheets and charts, you just reuse that host id.
    #[arg(short, long)]
    id: String,
    /// Enable JSON log format
    #[arg(short, long)]
    json: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // Retrieve configuration
    let args = Args::parse();

    let config = Configuration::new(&args.config)
        .expect("Incorrect configuration (can be potentially overriden by environment variables starting with `GORAL__`)");

    let level = LevelFilter::from_level(config.general.log_level);
    let (json, plain) = if args.json {
        (
            Some(tracing_subscriber::fmt::layer().with_target(true).json()),
            None,
        )
    } else {
        (
            None,
            Some(tracing_subscriber::fmt::layer().with_target(true)),
        )
    };
    tracing_subscriber::registry()
        .with(level)
        .with(json)
        .with(plain)
        .init();

    let graceful_shutdown_timeout = config.general.graceful_timeout_secs;
    let auth = get_google_auth(&config.general.service_account_credentials_path).await;
    let (tx, rx) = mpsc::channel(5);
    let tx = Sender::new(tx);
    let sheets_api = SpreadsheetAPI::new(auth, tx.clone());
    let storage = Arc::new(Storage::new(args.id.to_string(), sheets_api, tx.clone()));
    let shared = Shared::new(tx.clone());

    let messengers = collect_messengers(&config);
    let mut services = collect_services(config, shared, messengers, rx);

    let (shutdown, _shutdown_receiver) = broadcast::channel(1);
    let mut tasks = Vec::with_capacity(services.len());
    while let Some(mut service) = services.pop() {
        let storage = storage.clone();
        let shutdown_receiver = shutdown.subscribe();
        tasks.push(tokio::spawn(async move {
            let log = create_log(
                storage,
                service.spreadsheet_id().to_string(),
                service.name().to_string(),
            );
            service.run(log, shutdown_receiver).await
        }));
    }

    tokio::select! {
        _ = async { signal::ctrl_c().await.expect("failed to listen for Ctrl+C event") } => {
            let msg = format!("{APP_NAME} has received `Ctrl+C` signal, will try to gracefully shutdown within {graceful_shutdown_timeout} secs");
            tracing::warn!("{}", msg.as_str());
            tx.try_warn(msg);
        }
        _ = async { sigterm().await.expect("failed to listen for Sigterm event") } => {
            let msg = format!("{APP_NAME} has received SIGTERM signal, will try to gracefully shutdown within {graceful_shutdown_timeout} secs");
            tracing::warn!("{}", msg.as_str());
            tx.try_warn(msg);
        }
    };
    shutdown
        .send(graceful_shutdown_timeout)
        .expect("assert: services should run when shutdown signal is sent");
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(graceful_shutdown_timeout.into())) => {
            tracing::warn!("{} couldn't gracefully shutdown", APP_NAME);
        }
        _ = join_all(tasks) => {
            tracing::info!("{} has gracefully shutdowned", APP_NAME);
        }
    };
}
