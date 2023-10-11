pub(crate) mod general;
pub(crate) mod logs;
pub(crate) mod metrics;
pub(crate) mod resources;
use crate::messenger::BoxedMessenger;
use crate::storage::AppendableLog;
use anyhow::Result;
use async_trait::async_trait;

use std::sync::Arc;

#[async_trait]
pub(crate) trait Service<'s> {
    fn name(&self) -> &str;

    fn spreadsheet_id(&self) -> &str;

    fn set_messenger(&mut self, messenger: Arc<BoxedMessenger>);

    async fn run(&mut self, _appendable_log: AppendableLog<'_>) -> Result<()> {
        Ok(())
    }
}
