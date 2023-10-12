pub(crate) mod general;
pub(crate) mod healthcheck;
pub(crate) mod logs;
pub(crate) mod metrics;
pub(crate) mod resources;
use crate::messenger::BoxedMessenger;
use crate::storage::AppendableLog;
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;

#[async_trait]
pub trait Service {
    fn name(&self) -> &str;

    fn spreadsheet_id(&self) -> &str;

    async fn run(&mut self, _appendable_log: AppendableLog, shutdown: Receiver<u16>) {}
}
