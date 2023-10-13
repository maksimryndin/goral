pub(crate) mod general;
pub(crate) mod healthcheck;
pub(crate) mod logs;
pub(crate) mod metrics;
pub(crate) mod resources;

use crate::storage::AppendableLog;

use async_trait::async_trait;

use tokio::sync::broadcast::Receiver;

#[async_trait]
pub trait Service {
    fn name(&self) -> &str;

    fn spreadsheet_id(&self) -> &str;

    async fn run(&mut self, _appendable_log: AppendableLog, _shutdown: Receiver<u16>) {}
}
