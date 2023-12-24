use crate::messenger::configuration::MessengerConfig;
use crate::messenger::Messenger;
use crate::HttpsClient;
use anyhow::anyhow;
use anyhow::Result;
use async_trait::async_trait;
use hyper::{Body, Method, Request};
use serde_derive::Serialize;

pub(crate) struct Discord {
    client: HttpsClient,
}

impl Discord {
    pub(crate) fn new() -> Self {
        let client: HttpsClient = hyper::Client::builder().build(
            hyper_rustls::HttpsConnectorBuilder::new()
                .with_native_roots()
                .https_only()
                .enable_http1()
                .build(),
        );
        Self { client }
    }

    async fn send_message(&self, config: &MessengerConfig, markdown: &str) -> Result<()> {
        let mut url = config.url.clone();
        url.query_pairs_mut().clear();
        let body = DiscordRequestBody::new(markdown);
        let req = Request::builder()
            .method(Method::POST)
            .uri(url.as_str())
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&body)?))?;
        tracing::debug!("{:?}", req);
        let resp = self.client.request(req).await?;
        tracing::debug!("{:?}", resp);
        if resp.status() != 204 {
            tracing::error!("discord error response {:?}", resp);
            return Err(anyhow!("discord error"));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct DiscordRequestBody<'a> {
    content: &'a str,
}

impl<'a> DiscordRequestBody<'a> {
    fn new(content: &'a str) -> Self {
        Self { content }
    }
}

#[async_trait]
impl Messenger for Discord {
    async fn send_info(&self, config: &MessengerConfig, markdown: &str) -> Result<()> {
        self.send_message(config, format!("🟢 {markdown}").as_str())
            .await
    }

    async fn send_warning(&self, config: &MessengerConfig, markdown: &str) -> Result<()> {
        self.send_message(config, format!("🟡 {markdown}").as_str())
            .await
    }

    async fn send_error(&self, config: &MessengerConfig, markdown: &str) -> Result<()> {
        self.send_message(config, format!("🔴 {markdown}").as_str())
            .await
    }
}
