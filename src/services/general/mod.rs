pub(crate) mod configuration;
use crate::configuration::APP_NAME;
use crate::messenger::configuration::MessengerConfig;
use crate::messenger::BoxedMessenger;
use crate::services::general::configuration::{General, Liveness as LivenessConfig, LivenessType};
use crate::services::Service;
use crate::storage::{AppendableLog, Datarow, Datavalue};
use crate::Shared;
use anyhow::Result;
use async_trait::async_trait;
use chrono::NaiveDate;

use std::fmt::{self, Debug, Display};
use std::sync::Arc;
use std::time::Duration;
use tracing::instrument;
use url::Url;

const GENERAL_SERVICE_NAME: &str = "general";

pub(crate) struct Liveness {
    pub(crate) initial_delay: Duration,
    pub(crate) period: Duration,
    pub(crate) timeout: Duration,
    pub(crate) endpoint: Option<Url>,
    pub(crate) command: Option<String>,
    pub(crate) typ: LivenessType,
}

impl Debug for Liveness {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(endpoint) = &self.endpoint {
            write!(f, "{} ({})", endpoint.as_str(), self.typ)
        } else {
            write!(f, "{}", self.command.as_ref().unwrap())
        }
    }
}

impl Display for Liveness {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(endpoint) = &self.endpoint {
            write!(f, "{} ({})", endpoint.as_str(), self.typ)
        } else {
            write!(f, "{}", self.command.as_ref().unwrap())
        }
    }
}

impl From<LivenessConfig> for Liveness {
    fn from(c: LivenessConfig) -> Self {
        Self {
            initial_delay: Duration::from_secs(c.initial_delay_secs.into()),
            period: Duration::from_secs(c.period_secs.into()),
            timeout: Duration::from_millis(c.timeout_ms.into()),
            endpoint: c.endpoint,
            command: c.command,
            typ: c.typ,
        }
    }
}

#[derive(Debug)]
pub(crate) struct GeneralService<'s> {
    shared: Shared<'s>,
    spreadsheet_id: String,
    liveness: Vec<Liveness>,
    messenger_config: MessengerConfig,
}

impl<'s> GeneralService<'s> {
    pub(crate) fn new(shared: Shared<'s>, config: General) -> GeneralService<'s> {
        Self {
            shared,
            spreadsheet_id: config.spreadsheet_id,
            liveness: config.liveness.into_iter().map(|l| l.into()).collect(),
            messenger_config: config.messenger,
        }
    }
}

#[async_trait]
impl<'s> Service<'s> for GeneralService<'s> {
    fn name(&self) -> &str {
        GENERAL_SERVICE_NAME
    }

    fn spreadsheet_id(&self) -> &str {
        self.spreadsheet_id.as_str()
    }

    fn set_messenger(&mut self, messenger: Arc<BoxedMessenger>) {
        self.shared.messenger = Some(messenger);
    }

    #[instrument(skip_all)]
    async fn run(&mut self, mut log: AppendableLog<'_>) -> Result<()> {
        log.append(vec![Datarow::new(
            "liveness".to_string(),
            NaiveDate::from_ymd_opt(1900, 02, 1)
                .unwrap()
                .and_hms_opt(15, 0, 0)
                .unwrap(),
            vec![("log(http)".to_string(), Datavalue::Number(3.6))],
        )])
        .await?;
        let message = format!(
            "*{APP_NAME}* service _{}_ started for liveness probes `{:?}` and [spreadsheet]({})",
            self.name(),
            self.liveness,
            log.spreadsheet_url()
        );
        self.shared
            .messenger
            .clone()
            .unwrap()
            .send_info(&self.messenger_config, &message)
            .await;
        tracing::info!("{} initialized", self.name());
        Ok(())
    }
}
