# Metrics

Metrics scrape endpoints with Prometheus metrics. Maximum response body is set to 65536 bytes.

<details open>
  <summary>Basic configuration</summary>

```toml
[metrics]
spreadsheet_id = "<spreadsheet_id>"
[[target]]
endpoint = "<prometheus metrics endpoint1>"
```
</details>

<details>
  <summary>Full configuration</summary>

```toml
[metrics]
spreadsheet_id = "<spreadsheet_id>"
# messenger.url = "<messenger api url for sending messages>"
# push_interval_secs = 30
# scrape_interval_secs = 10
# scrape_timeout_ms = 3000
# autotruncate_at_usage_percent = 20
[[target]]
endpoint = "<prometheus metrics endpoint1>"
name = "app1"
[[target]]
endpoint = "<prometheus metrics endpoint2>"
name = "app2"
```
</details>

For every scrape target and every metric Metrics service creates a separate sheet. When several targets are scraped, their sheet names start with `<name>:` to distinguish several instances of the same app.

If there is an error while scraping, it is sent via a configured messenger or via a default messenger of General service.

*Note:* if the observed app restarts, then its metric counters are reset. Goral just scrapes metrics as-is without _trying to merge them_. For metrics data it is usually acceptable. If you need some more reliable way to collect data - consider using [KV Log](./kv-log.md) as it uses a synchronous push strategy and allows you to setup a merge strategy on the app side.

If a messenger is configured, then any scraping error is sent via the messenger (even it is the same error, it is sent each time). In case of many scrape endpoints with short scrape intervals there is a risk to hit a messenger rate limit.
