pub(crate) mod configuration;
use crate::configuration::APP_NAME;
use crate::messenger::configuration::MessengerConfig;
use crate::messenger::BoxedMessenger;
use crate::services::general::configuration::{General, Liveness as LivenessConfig, LivenessType};
use crate::services::Service;
use crate::services::Shared;
use crate::spreadsheet::{filter_virtual_sheets, Header, Sheet, VirtualSheet};
use anyhow::Result;
use async_trait::async_trait;

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
    sheets: Vec<Sheet>,
    messenger_config: MessengerConfig,
}

impl<'s> GeneralService<'s> {
    pub(crate) fn new(shared: Shared<'s>, config: General) -> GeneralService<'s> {
        Self {
            shared,
            spreadsheet_id: config.spreadsheet_id,
            liveness: config.liveness.into_iter().map(|l| l.into()).collect(),
            sheets: vec![],
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

    #[instrument(skip(self))]
    async fn initialize(&mut self) -> Result<()> {
        let existing_sheets = self
            .shared
            .google
            .sheets_managed_by_service(self.spreadsheet_id(), self.name(), self.shared.host_id)
            .await?;

        tracing::info!(
            "existing sheets for service {}:\n{:?}",
            self.name(),
            existing_sheets
        );
        let headers: Vec<Header> = [Header::new(
            "timestamp".to_string(),
            Some("Datetime of check".to_string()),
        )]
        .into_iter()
        .chain(
            self.liveness
                .iter()
                .map(|l| Header::new(l.to_string(), None)),
        )
        .collect();

        let expected_sheets = vec![VirtualSheet::new_grid(
            format!("{}_{}", self.name(), self.shared.host_id),
            self.name().to_string(),
            self.shared.host_id.to_string(),
            headers,
        )];

        let sheets_to_add: Vec<VirtualSheet> =
            filter_virtual_sheets(expected_sheets, &existing_sheets);

        tracing::info!("sheets_to_add {:?}", sheets_to_add);

        let sheets = self
            .shared
            .google
            .add_sheets(&self.spreadsheet_id, sheets_to_add)
            .await?;
        self.sheets = existing_sheets
            .into_iter()
            .chain(sheets.into_iter())
            .collect();
        tracing::info!("{} initialized with sheets {:?}", self.name(), self.sheets);
        let message = format!(
            "*{APP_NAME}* service _{}_ started for liveness probes `{:?}` and [spreadsheet]({})",
            self.name(),
            self.liveness,
            self.shared.google.spreadsheet_url(&self.spreadsheet_id)
        );
        self.shared
            .messenger
            .clone()
            .unwrap()
            .send_info(&self.messenger_config, &message)
            .await
    }
}
