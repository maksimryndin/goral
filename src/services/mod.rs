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

    async fn run(&mut self, _appendable_log: AppendableLog, mut shutdown: Receiver<u16>) {
        loop {
            tokio::select! {
                result = shutdown.recv() => {
                    let graceful_shutdown_timeout = match result {
                        Err(_) => panic!("assert: shutdown signal sender should be dropped after all service listeneres"),
                        Ok(graceful_shutdown_timeout) => graceful_shutdown_timeout,
                    };
                    tracing::info!("{} service has got shutdown signal", self.name());
                    return;
                }
            }
        }
    }
}
