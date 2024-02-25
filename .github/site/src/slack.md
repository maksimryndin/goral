# Slack

<details>
  <summary>App creation</summary>

Follow guides https://api.slack.com/start/quickstart and https://api.slack.com/tutorials/tracks/posting-messages-with-curl
</details>

Example configuration:

```toml
messenger.specific.token = "xoxb-slack-token"
messenger.specific.channel = "CHRISHWFH2"
messenger.url = "https://slack.com/api/chat.postMessage"
```

[Rate limit](https://api.slack.com/methods/chat.postMessage#rate_limiting)