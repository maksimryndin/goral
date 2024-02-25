# General

General service is responsible for reserved communication channel and important notifications about Goral itself. Also it periodically checks (72 hours at the moment) for new releases of Goral to notify. Its configuration

<details open>
  <summary>Basic configuration</summary>

```toml
[general]
service_account_credentials_path = "/path/to/service_account.json"
messenger.url = "<messenger api url for sending messages>"
```
</details>

<details>
  <summary>Full configuration</summary>

```toml
[general]
# log_level = "info"
service_account_credentials_path = "/path/to/service_account.json"
messenger.url = "<messenger api url for sending messages>"
# graceful_timeout_secs = 10
```
</details>

Configuration of General service is a minimum configuration for Goral.