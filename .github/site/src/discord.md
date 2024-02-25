# Discord

<details>
  <summary>Webhook creation</summary>

1) Create a text channel
2) In the settings of the channel (cogwheel) go to the Integrations -> Webhooks
3) Either use the default one or create a new webhook
</details>

Example configuration:

```toml
messenger.url = "https://discord.com/api/webhooks/<webhook_id>/<webhook_token>"
```

[Rate limits](https://discord.com/developers/docs/topics/rate-limits).