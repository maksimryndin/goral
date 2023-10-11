#![forbid(unsafe_code)]

mod configuration;
mod messenger;
mod services;
mod spreadsheet;
mod storage;
use clap::Parser;
use configuration::Configuration;
use messenger::{get_messenger, BoxedMessenger};
use services::general::GeneralService;
use services::logs::LogsService;
use services::metrics::MetricsService;
use services::resources::ResourcesService;
use services::Service;
use spreadsheet::{get_google_auth, SpreadsheetAPI};
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::Arc;
use storage::Storage;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

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

fn collect_services(
    config: Configuration,
    shared: Shared,
) -> Vec<Box<dyn Service + Sync + Send + '_>> {
    let mut messengers_services: HashMap<Option<String>, Vec<Box<dyn Service + Sync + Send>>> =
        HashMap::new();

    let general_messenger_host = Some(config.general.messenger.host().to_string());
    let general_service1 = GeneralService::new(shared.clone(), config.general);
    let general_service = Box::new(general_service1) as Box<dyn Service + Sync + Send>;
    messengers_services
        .entry(general_messenger_host)
        .or_insert(vec![])
        .push(general_service);

    if let Some(metrics) = config.metrics {
        let metrics_service = MetricsService::new(shared.clone(), metrics);
        let metrics_service = Box::new(metrics_service) as Box<dyn Service + Sync + Send>;
        messengers_services
            .entry(None)
            .or_insert(vec![])
            .push(metrics_service);
    }

    if let Some(logs) = config.logs {
        let logs_messenger_host = logs.messenger.as_ref().map(|m| m.host().to_string());
        let logs_service = LogsService::new(shared.clone(), logs);
        let logs_service = Box::new(logs_service) as Box<dyn Service + Sync + Send>;
        messengers_services
            .entry(logs_messenger_host)
            .or_insert(vec![])
            .push(logs_service);
    }

    if let Some(resources) = config.resources {
        let resources_messenger_host = resources.messenger.as_ref().map(|m| m.host().to_string());
        let resources_service = ResourcesService::new(shared, resources);
        let resources_service = Box::new(resources_service) as Box<dyn Service + Sync + Send>;
        messengers_services
            .entry(resources_messenger_host)
            .or_insert(vec![])
            .push(resources_service);
    }

    let mut services = vec![];

    // create and assign shared messengers
    for (messenger_host, mut services_list) in messengers_services {
        if let Some(messenger_host) = messenger_host.as_ref() {
            let messenger = get_messenger(messenger_host).expect(
                format!("failed to create messenger for host `{}`", messenger_host).as_str(),
            );
            let messenger = Arc::new(messenger);
            while let Some(mut service) = services_list.pop() {
                service.set_messenger(messenger.clone());
                services.push(service);
            }
        } else {
            while let Some(service) = services_list.pop() {
                services.push(service);
            }
        }
    }
    // sort for pretty order (general service goes first)
    services.sort_unstable_by_key(|s| s.name().to_string());
    assert_eq!(services[0].name(), "general");
    services
}

#[derive(Clone)]
pub(crate) struct Shared<'a> {
    pub(crate) storage: &'a Storage,
    pub(crate) messenger: Option<Arc<BoxedMessenger>>,
    pub(crate) host_id: &'a str,
}

impl Debug for Shared<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Shared({})", self.host_id)
    }
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

    let auth = get_google_auth(&config.general.service_account_credentials_path).await;

    let sheets_api = SpreadsheetAPI::new(auth);
    let storage = Storage::new(args.id.to_string(), sheets_api);

    // TODO shared channel for health events
    let shared = Shared {
        storage: &storage,
        messenger: None,
        host_id: &args.id,
    };

    // collect services and group them by messengers hosts (for proper connection pooling)
    let mut services = collect_services(config, shared.clone());

    // for s in &mut services {
    //     println!("{:?}", s.name());
    //     s.initialize()
    //         .await
    //         .expect(format!("failed to initialize service `{}`", s.name()).as_str());
    //     // TODO finish say hello for other components
    //     // get current spreadsheet info and update cells limits and sheets
    //     // prepare sheets and charts
    // }
    for s in &mut services {
        s.run(storage.create_log(s.spreadsheet_id().to_string(), s.name().to_string()))
            .await
            .expect(format!("failed to run service `{}`", s.name()).as_str());
        // TODO finish say hello for other components
        // get current spreadsheet info and update cells limits and sheets
        // prepare sheets and charts
    }

    // Main loop

    // TODO graceful shutdown
}
