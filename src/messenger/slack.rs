use crate::messenger::configuration::{MessengerConfig, MessengerImplementation};
use crate::messenger::Messenger;
use crate::HttpsClient;
use anyhow::anyhow;
use anyhow::Result;
use async_trait::async_trait;
use hyper::{Body, Method, Request};
use lazy_static::lazy_static;
use regex::Regex;
use serde_derive::Serialize;
use std::borrow::Cow;

pub(crate) struct Slack {
    client: HttpsClient,
}

impl Slack {
    pub(crate) fn new() -> Self {
        let client: HttpsClient = hyper::Client::builder().build(
            hyper_rustls::HttpsConnectorBuilder::new()
                .with_native_roots()
                .expect("assert: can build slack client with native root certs")
                .https_only()
                .enable_http1()
                .build(),
        );
        Self { client }
    }

    async fn send_message(&self, config: &MessengerConfig, markdown: &str) -> Result<()> {
        let (channel, token) = match &config.implementation {
            Some(MessengerImplementation::Slack { channel, token }) => (channel, token),
            _ => panic!("assert: messenger implementation should be validated at configuration"),
        };
        let processed = process_links(markdown);
        let body = SlackRequestBody::new(channel, &processed);
        let req = Request::builder()
            .method(Method::POST)
            .uri(config.url.as_str())
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {}", token))
            .body(Body::from(serde_json::to_string(&body)?))?;
        tracing::debug!("{:?}", req);
        let resp = self.client.request(req).await?;
        tracing::debug!("{:?}", resp);
        if resp.status() != 200 {
            tracing::error!("slack error response {:?}", resp);
            return Err(anyhow!("slack error"));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct Text<'a> {
    #[serde(rename(serialize = "type"))]
    typ: &'a str,
    text: &'a str,
}

#[derive(Debug, Serialize)]
struct Block<'a> {
    #[serde(rename(serialize = "type"))]
    typ: &'a str,
    text: Text<'a>,
}

#[derive(Debug, Serialize)]
struct SlackRequestBody<'a> {
    channel: &'a str,
    blocks: [Block<'a>; 1],
}

impl<'a> SlackRequestBody<'a> {
    fn new(channel: &'a str, text: &'a str) -> Self {
        Self {
            channel,
            blocks: [Block {
                typ: "section",
                text: Text {
                    typ: "mrkdwn",
                    text,
                },
            }],
        }
    }
}

fn process_links(input: &str) -> Cow<str> {
    lazy_static! {
        static ref LINKS_REGEX: Regex =
            Regex::new(r"\[(?P<title>[^()\[\]]+)\]\((?P<url>[^()\[\]]+)\)").unwrap();
    }
    LINKS_REGEX.replace_all(input, "<$url|$title>")
}

#[async_trait]
impl Messenger for Slack {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replace_url_to_slack_links() {
        let text = "GORAL started with [api usage page](https://console.cloud.google.com/apis/dashboard?project=project-id&show=all) and [api quota page](https://console.cloud.google.com/iam-admin/quotas?project=project-id)";
        let processed_expected = "GORAL started with <https://console.cloud.google.com/apis/dashboard?project=project-id&show=all|api usage page> and <https://console.cloud.google.com/iam-admin/quotas?project=project-id|api quota page>";
        assert_eq!(
            process_links(text).into_owned(),
            processed_expected.to_string()
        );
    }
}
