use crate::messenger::configuration::MessengerConfig;
use crate::messenger::Messenger;
use crate::HttpsClient;
use anyhow::anyhow;
use anyhow::Result;
use async_trait::async_trait;
use hyper::{Body, Method, Request};
use serde_derive::Serialize;

pub(crate) struct Telegram {
    client: HttpsClient,
}

impl Telegram {
    pub(crate) fn new() -> Self {
        let client: HttpsClient = hyper::Client::builder().build(
            hyper_rustls::HttpsConnectorBuilder::new()
                .with_native_roots()
                .https_or_http()
                .enable_http1()
                .build(),
        );
        Self { client }
    }

    async fn send_message(
        &self,
        config: &MessengerConfig,
        markdown: &str,
        disable_notification: bool,
    ) -> Result<()> {
        let mut url = config.url.clone();
        url.query_pairs_mut().clear();
        let body = TelegramRequestBody::new(&config.chat_id, markdown, disable_notification);
        let req = Request::builder()
            .method(Method::POST)
            .uri(url.as_str())
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&body)?))?;
        tracing::debug!("{:?}", req);
        // TODO retries ??
        // TODO timeout
        // TODO check trxt message for illegal characters for markdown??
        let resp = self.client.request(req).await?;
        tracing::debug!("{:?}", resp);
        if resp.status() == 400 {
            tracing::error!(
                "incorrect telegram configuration or markdown: response {:?}",
                resp
            );
            return Err(anyhow!("incorrect telegram configuration or markdown"));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct TelegramRequestBody<'a> {
    chat_id: &'a str,
    text: &'a str,
    parse_mode: &'a str,
    disable_notification: bool,
}

impl<'a> TelegramRequestBody<'a> {
    fn new(chat_id: &'a str, text: &'a str, disable_notification: bool) -> Self {
        Self {
            chat_id,
            text,
            disable_notification,
            parse_mode: "MarkdownV2",
        }
    }
}

#[async_trait]
impl Messenger for Telegram {
    async fn send_info(&self, config: &MessengerConfig, markdown: &str) -> Result<()> {
        self.send_message(config, markdown, true).await
    }

    async fn send_warning(&self, config: &MessengerConfig, markdown: &str) -> Result<()> {
        self.send_message(config, markdown, false).await
    }

    async fn send_error(&self, config: &MessengerConfig, markdown: &str) -> Result<()> {
        self.send_message(config, markdown, false).await
    }
}
