#![forbid(unsafe_code)]

use clap::Parser;
use futures::future::try_join_all;
use goral::configuration::{Configuration, APP_NAME};
use goral::spreadsheet::{get_google_auth, SpreadsheetAPI};
use goral::storage::{create_log, Storage};
use goral::{collect_messengers, collect_services, Sender, Shared};
use std::fmt::Debug;
use std::panic;
use std::process;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::{broadcast, mpsc};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[cfg(any(target_os = "freebsd", target_os = "linux", target_os = "macos",))]
pub async fn sigterm() -> tokio::io::Result<()> {
    signal::unix::signal(signal::unix::SignalKind::terminate())?
        .recv()
        .await;
    Ok(())
}

#[cfg(not(any(target_os = "freebsd", target_os = "linux", target_os = "macos",)))]
pub async fn sigterm() -> tokio::io::Result<()> {
    std::future::pending::<()>().await;
    Ok(())
}

/// Goral is a modular observability toolkit for small projects.
///
/// Goral is a lightweight daemon process which
/// stores collected data in a Google Spreadsheet so no
/// additional storage required.
/// Any Goral service is easily replaced with enterprise-grade
/// solution like Prometheus, Loki or Zabbix because it scrapes
/// data in common industry formats.
/// https://github.com/maksimryndin/goral#goral
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to TOML configuration file. Environment variables starting with `GORAL_`
    /// (e.g. `GORAL__RESOURCES__ALERT_IF_FREE_DISK_SPACE_PERCENT_LESS_THAN`)
    /// overwrite config values.
    #[arg(short, long)]
    config: String,
    /// Host identifier (8 characters). Keep this argument persistent for this instance of Goral
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
async fn main() -> Result<(), String> {
    panic::set_hook(Box::new(|panic_info| {
        let base_message = format!("\nCould you please open an issue https://github.com/maksimryndin/goral/issues with Bug label? Thank you for using {APP_NAME}!");
        if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            if s.starts_with("assert:") {
                println!(
                    "key assumption about {APP_NAME} behaviour is violated: {s:?}{base_message}"
                );
            } else {
                println!("Unexpected error happened: {}", panic_info);
            }
        } else {
            println!("Unexpected error happened: {}", panic_info);
        }
    }));
    {
        let args = Args::parse();

        let config = Configuration::new(&args.config).map_err(|e|
            format!(
                "Incorrect configuration (can be potentially overriden by environment variables starting with `GORAL__`): {e}"
            )
        )?;

        let level = LevelFilter::from_level(config.general.log_level);
        let (json, plain) = if args.json {
            (
                Some(tracing_subscriber::fmt::layer().with_target(true).json()),
                None,
            )
        } else {
            (None, Some(tracing_subscriber::fmt::layer()))
        };

        tracing_subscriber::registry()
            .with(level)
            .with(json)
            .with(plain)
            .init();

        let graceful_shutdown_timeout = config.general.graceful_timeout_secs;
        let (project_id, auth) =
            get_google_auth(&config.general.service_account_credentials_path).await;
        let (tx, rx) = mpsc::channel(5); // TODO estimate capacity via services quantity + some reserve
        let tx = Sender::new(tx);
        let sheets_api = SpreadsheetAPI::new(auth, tx.clone());
        let storage = Arc::new(Storage::new(
            args.id.to_string(),
            project_id,
            sheets_api,
            tx.clone(),
        ));
        let shared = Shared::new(tx.clone());

        let messengers = collect_messengers(&config);
        let mut services = collect_services(config, shared, messengers, rx);

        let (shutdown, _) = broadcast::channel(1);
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

        let goral = try_join_all(tasks);
        tokio::pin!(goral);
        storage.welcome().await;
        tracing::info!("{APP_NAME} started with pid {}", process::id());

        tokio::select! {
            biased;

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
            res = &mut goral => {
                res.unwrap(); // propagate panics from spawned tasks
            }
        }
        shutdown
            .send(graceful_shutdown_timeout)
            .expect("assert: services should run when shutdown signal is sent");

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(graceful_shutdown_timeout.into())) => {
                tracing::error!("{} couldn't gracefully shutdown", APP_NAME);
            },
            _ = &mut goral => {
                tracing::info!("{} has gracefully shutdowned. If you like it, consider giving a star to https://github.com/maksimryndin/goral. Thank you!", APP_NAME);
            }
        }
    }
    // in case we collect logs from stdin - there is no way to cancel the underlying blocking thread
    // so after the shutdown (which ensures proper termination) we exit the whole process
    // main body function is enclosed with braces to run destructors as the `exit` aborts immediately
    std::process::exit(0);
}
