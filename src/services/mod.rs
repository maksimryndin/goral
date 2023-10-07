pub(crate) mod general;
pub(crate) mod logs;
pub(crate) mod metrics;
pub(crate) mod resources;
use crate::messenger::BoxedMessenger;
use crate::spreadsheet::SpreadsheetAPI;
use anyhow::Result;
use async_trait::async_trait;
use std::fmt::{self, Debug};
use std::sync::Arc;

#[async_trait]
pub(crate) trait Service<'s> {
    fn name(&self) -> &str;

    fn spreadsheet_id(&self) -> &str;

    fn set_messenger(&mut self, messenger: Arc<BoxedMessenger>);

    async fn initialize(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct Shared<'a> {
    pub(crate) google: &'a SpreadsheetAPI,
    pub(crate) messenger: Option<Arc<BoxedMessenger>>,
    pub(crate) host_id: &'a str,
}

impl Debug for Shared<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Shared({})", self.host_id)
    }
}
