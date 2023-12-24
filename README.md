# Goral

Observability toolkit for small projects. Easy-to-use and compatible with industry standards.

- [Overview](#overview)
    - [System requirements](#system-requirements)
    - [Installation](#installation)
- [Setup](#setup)
    - [Telegram](#telegram)
    - [Slack](#slack)
    - [Discord](#discord)
- [Services](#services)
      - [Storage quota](#storage-quota)
    - [General](#general)
    - [Healthcheck](#healthcheck)
    - [Metrics](#metrics)
    - [Logs](#logs)
    - [System](#system)
    - [KV Log](#kv-log)
- [Rules](#rules)
- [Recommended deployment](#recommended-deployment)

## Overview

Goral is a simple observability daemon developed with the following idea in mind: when you have your favorite application in its infantry, you usually don't need a full-blown observability toolkit (which require much more setup, maintenance and resources) around as the amount of data is not so huge. It is especially true for pet projects when you just want to test an idea and deploy the app at some free-tier commodity VPS.

So Goral provides the following features being deployed next to your app(s):
* Periodic healthchecks (aka [liveness probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/))
* Metrics collection (fully compatible with Prometheus to be easily replaced with more advanced stack as your project grows)
* Logs collection (importing logs from stdout/stderr of the target process)
* System telemetry (CPU, Memory, Free/Busy storage space etc)
* A general key-value appendable log storage (see [the user case below](#kv))
* Features are modular - all [services](#services) are switched on/off in the configuration.
* You can observe several instances of the same app or different apps on the same host with a single Goral daemon (except logs as logs are collected via stdin of Goral - see [below](#logs))
* You can configure different messengers and/or channels for every [service](#services) to get notifications on errors, liveness updates, system resources overlimit etc
* All the data collected is stored in Google Sheet with an automatic quota and limits checks and automatic data rotation - old data is deleted with a preliminary notification via configured messenger (see below). That way you don't have to buy a separate storage or overload your app VPS with Prometheus, ELK etc. Just a lean process next to your brilliant one which just sends app data in batches to Google Sheets for your ease of use. Google Sheets allow you to build your own diagrams over the metrics and analyse them, analyse liveness statistics and calculate uptime etc. By default Goral builds some charts for you.
* You can configure different spreadsheets and messengers for every service
* You can configure [rules](#rules) for notifications by messenger for any data.

### System requirements

* Memory: RSS 30M, 900M for virtual memory. An actual requirement may be different - as it depends on the amount of data, scrape and push intervals (see below for each [service](#services))
* Binary size is around 30 Mb
* Platforms: Linux, MacOS. Other platform will probably work also.

### Installation

You can install Goral
1) by downloading a prebuilt binary from https://github.com/maksimryndin/goral/releases
2) from source (you need Rust compiler and `cargo`) with a command
```sh
RUSTFLAGS='-C target-feature=+crt-static' cargo build --release --target <target triple>
```

## Setup

To use Goral you need to have a Google account and obtain a service account:
1) Create a project https://console.cloud.google.com/projectcreate (we suggest creating a separate GCP for Goral project for security reasons)
2) Enable Sheets from the products page https://console.cloud.google.com/workspace-api/products
3) Create a service account https://console.cloud.google.com/workspace-api/credentials with Editor role
4) After creating the service acoount create a private key (type JSON) for it (the private key should be downloaded by your browser)
5) Create a spreadsheet (where the scraped data will be stored) and add the service account email as an Editor to the spreadsheet
6) Extract spreadsheet id from the spreadsheet url

Note: you can also install Google Sheets app for your phone to have an access to the data.

And notifications are sent to messengers:

### Telegram

<details>
  <summary>Bot creation</summary>

1) Create a bot - see https://core.telegram.org/bots/features#creating-a-new-bot
2) Create a private group for notifications to be sent to
3) Add your bot to the group
4) Obtain a `chat_id` following the accepted answer https://stackoverflow.com/questions/33858927/how-to-obtain-the-chat-id-of-a-private-telegram-channel
</details>

Example configuration:

```toml
messenger.specific.chat_id = "-100129008371"
messenger.url = "https://api.telegram.org/bot<bot token>/sendMessage"
```

[Rate limit](https://core.telegram.org/bots/faq#my-bot-is-hitting-limits-how-do-i-avoid-this)

*Note* for Telegram all info-level messages are sent without notification so the phone doesn't vibrate or make any sound.

### Slack

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

### Discord

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

## Services

A workhorse abstraction of Goral over a sheet is an appendable log - just a table which grows with adding key-value records to it.

A sheet managed by Goral has a title `<log to collect data on>@<host_id>@<service> <creation datetime>`. You can change the title, column names and other elements of the sheet but be aware that Goral will continue to append data in the same order as when the sheet was created by Goral if the form of the data hasn't changed (either `<log to collect data on>` or its keys). Creation datetimes for sheets always differ by some amount of seconds (jittered) even those sheets were created at the same time - in order to prevent conflicts in sheet titles.

Commented lines (starting with `#`) for all configurations below are optional and example values are their defaults.
Every service has a messenger configuration (see [Setup](#setup)). It is recommended to have several messengers and different chats/channels for each messenger and take into account their rate limits when configuring a service.

#### Storage quota

Every service (except General) has an `autotruncate_at_usage_percent` configuration - the limit of the usage share by a service. Any Google spreadsheet can contain at most 10_000_000 cells so if a services takes more than `autotruncate_at_usage_percent` of 10_000_000 cells, Goral will truncate old data by either removing old rows or removing the same named sheets (`<log to collect data on>`) under a service.
For every service the cleanup will truncate the surplus (actual usage - limit) and 10% of the limit.

When providing your own limits, do remember to have a sum of limits no more than 100% for all services related to the same spreadsheet. Default settings assume conservatively that you write all the data to the same spreadsheet. If [KV service](#kv-log) is turned on, its default truncation limit is set for 100% (meaning nothing is truncated as a safe default), so you will get a warning that limits sum is over 100% if you use the same spreadsheet for other services.

Also if a spreadsheet includes other sheets not managed by Goral, take into account their usage.

### General

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

### Healthcheck

Healthcheck service with a configuration

<details open>
  <summary>Basic configuration</summary>

```toml
[healthcheck]
spreadsheet_id = "<spreadsheet_id>"
[[healthcheck.liveness]]
type = "Http"
endpoint = "http://127.0.0.1:9898"
```
</details>

<details>
  <summary>Full configuration</summary>

```toml
[healthcheck]
spreadsheet_id = "<spreadsheet_id>"
# messenger.url = "<messenger api url for sending messages>"
# push_interval_secs = 30
# autotruncate_at_usage_percent = 10
[[healthcheck.liveness]]
# name = "http://127.0.0.1:9898" # by default the endpoint itself is used as a name
# initial_delay_secs = 0
# period_secs = 3
type = "Http"
endpoint = "http://127.0.0.1:9898"
# timeout_ms = 1000 # should be less than or equal period_secs
[[healthcheck.liveness]]
# name = "ls -lha" # by default the command itself is used as a name
# initial_delay_secs = 0
# period_secs = 3
type = "Command"
command = ["ls", "-lha"]
# timeout_ms = 1000 # should be less than or equal period_secs
[[healthcheck.liveness]]
# name = "[::1]:9898" # by default the tcp socket addr itself is used as a name
# initial_delay_secs = 0
# period_secs = 3
type = "Tcp"
endpoint = "[::1]:9898"
# timeout_ms = 1000 # should be less than or equal period_secs
[[healthcheck.liveness]]
# name = "http://[::1]:50050" # by default the tcp socket addr itself is used as a name
# initial_delay_secs = 0
# period_secs = 3
type = "Grpc"
endpoint = "http://[::1]:50050"
# timeout_ms = 1000 # should be less than or equal period_secs
```
</details>

will create a sheet for every liveness probe.

Liveness probes follow the same rules as for [k8s](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/). Namely:
* you should choose among HTTP GET (any status `>=200` and `<400`), gRPC, TCP (Goral can open socket) and command (successful exit)
* you can specify initial delay, period of probe and timeout on probe
* misconfiguration of a liveness probe is considered a failure
* for gRPC Health service should be configured on the app side (see also [gRPC health checking protocol](https://github.com/grpc/grpc/blob/v1.59.1/doc/health-checking.md)). Only `http` scheme is supported at the moment. If you need a tls check, you can use a command probe with a [grpc health probe](https://github.com/grpc-ecosystem/grpc-health-probe) and specify proper certificates.

Goral saves probe time, status (true for alive) and text output (for HTTP GET - response text, for command - stdout output, for all probes - error text). Each probe is saved at a separate sheet with its own uptime chart.
In case an output is larger than 1024 bytes, it is truncated and you get permanent warnings in logs of Goral. So configure the output size of your healthcheck reasonably (healthcheck responses shouldn't be heavy).
For command healthchecks it is recommended to assign a name to liveness probe or wrap your command in some script so that in case of small changes in command arguments preserve the same sheet for data (otherwise Goral will create a new sheet since the title has changed).

If a messenger is configured, then any healthcheck change (healthy -> unhealthy and vice versa) is sent via the messenger. In case of many endpoints with short liveness periods there is a risk to hit a messenger rate limit. 

### Metrics

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

*Note:* if the observed app restarts, then its metric counters are reset. Goral just scrapes metrics as-is without _trying to merge them_. For metrics data it is usually acceptable. If you need some more reliable way to collect data - consider using [KV Log](#kv-log) as it uses a synchronous push strategy and allows you to setup a merge strategy on the app side.

If a messenger is configured, then any scraping error is sent via the messenger (even it is the same error, it is sent each time). In case of many scrape endpoints with short scrape intervals there is a risk to hit a messenger rate limit.

### Logs

Logs service with the following configuration:

<details open>
  <summary>Basic configuration</summary>

```toml
[logs]
spreadsheet_id = "<spreadsheet_id>"
```
</details>

<details>
  <summary>Full configuration</summary>

```toml
[logs]
spreadsheet_id = "<spreadsheet_id>"
# messenger.url = "<messenger api url for sending messages>"
# push_interval_secs = 5
# autotruncate_at_usage_percent = 30
# filter_if_contains = []
# drop_if_contains = []
```
</details>

will create a single sheet with columns `datetime`, `level`, `log_line`.
A log line is truncated to 50 000 chars as it is a Google Sheets limit for a cell.
Goral tries to extract log level and datetime from a log line. If it fails to extract a log level then `N/A` is displayed. If it fails to extract datetime, then the current system time is used.

For logs collecting Goral reads its stdin. Basically it is a portable way to collect stdout of another process without a privileged access.
There is a caveat - if we make a simple pipe like `instrumented_app | goral` then in case of a termination of the `instrumented_app` Goral will not see any input and will stop reading.
[There is a way with named pipes](https://www.baeldung.com/linux/stdout-to-multiple-commands#3-solve-the-problem-usingtee-and-named-pipes) (for Windows there should also be a way as it also supports named pipes). 
* You create a named pipe, say `instrumented_app_logs_pipe` with the command `mkfifo instrumented_app_logs_pipe` (it creates a pipe file in the current directory - you can choose an appropriate place)
* The named pipe exists till there is at least one writer. So we create a fake one `while true; do sleep 365d; done >instrumented_app_logs_pipe &`
* start Goral with its usual command args and the pipe: `goral -c config.toml --id "host" <instrumented_app_logs_pipe`
* start you app with `instrumented_app | tee instrumented_app_logs_pipe` - you will see your logs in stdout as before and they also be cloned to the named pipe which is read by Goral.

With this named pipes approach the `instrumented_app` restarts doesn't stop Goral from reading its stdin for logs.
Just be sure to autorecreate a fake writer in case of a host system restarts.
See also [Deployment](#recommended-deployment) section for an example.

As there may be a huge amount of logs, it is recommended to filter the volume by specifiying an array of substrings (_case sensitive_) in `filter_if_contains` (e.g. `["info", "warn", "error"]`) and `drop_if_contains`, and/or have a separate spreadsheet for log collection as a huge amount of them may hurdle the use of Google sheets due to the constant updates.


### System

System service configuration:

<details open>
  <summary>Basic configuration</summary>

```toml
[system]
spreadsheet_id = "<spreadsheet_id>"
```
</details>

<details>
  <summary>Full configuration</summary>

```toml
[system]
spreadsheet_id = "<spreadsheet_id>"
# push_interval_secs = 20
# autotruncate_at_usage_percent = 20
# scrape_interval_secs = 10
# scrape_timeout_ms = 3000
# messenger.url = "<messenger api url for sending messages>"
# mounts = ["/"]
# names = ["goral"]
```
</details>

With this configuration System service will create following sheets:
* `basic`: with general information about the system (boot time, memory, cpus, swap, number of processes)
* `network`: for every network interface number of bytes read/written _since the previous measurement_, total read/written
* `top_disk_read` - process which has read the most from disk _during the last second_
* `top_disk_write` - process which has written the most to disk _during the last second_
* `top_cpu` - process with the most cpu time during the last second
* `top_memory` - process with the most memory usage _during the last second_
* `top_open_files` (for Linux only) - among the processes with the same user as goral (!) - process with the most opened files
* for every process with name containing one of the substrings in `names` - a sheet with process info. Note: the first match (_case sensitive_) is used so plan accordingly a unique name for your binary.
* for every mount in `mounts` - disk usage and free space.

System service doesn't require root privileges to collect the telemetry.
For a process a cpu usage percent may be [more than 100%](https://blog.guillaume-gomez.fr/articles/2021-09-06+sysinfo%3A+how+to+extract+systems%27+information) in a case of a multi-threaded process on several cores. `memory_used` by process is a [resident-set size](https://www.baeldung.com/linux/resident-set-vs-virtual-memory-size).

If there is an error while collecting system info, it is sent via a configured messenger or via a default messenger of General service.

### KV Log

Goral allows you to append key-value data to Google spreadsheet. Let's take an example. You provide a SAAS for wholesalers and have several services, let's say "Orders" and "Marketing Campaigns". 
Your client asks you for a billing data for each of the services in a spreadsheet format at the end of the month. You turn on KV Goral service with the following configuration:

<details open>
  <summary>Basic configuration</summary>

```toml
[kv]
spreadsheet_id = "<spreadsheet_id>"
port = 49152 # port from the range 49152-65535
```
</details>

<details>
  <summary>Full configuration</summary>
  
```toml
[kv]
spreadsheet_id = "<spreadsheet_id>"
port = <"port from the range 49152-65535">
# autotruncate_at_usage_percent = 100
# messenger.url = "<messenger api url for sending messages>"
```
</details>

Such a configuration runs a server process in the Goral daemon listening at the specified port (localhost only for security reasons). From your app you periodically make a batch POST request to `localhost:<port>/api/kv` with a json body:
```json
[
    {
        "datetime": "2023-12-09T09:50:46.136945094Z", /* an RFC 3339 and ISO 8601 date and time string */
        "log_name": "orders", /* validated against regex ^[a-zA-Z_][a-zA-Z0-9_]*$ */
        "data": [["donuts", 10], ["chocolate bars", 3]], /* first datarow in "orders" log defines order of column headers for a sheet (if it should be created) */
    },
    {
        "datetime": "2023-12-09T09:50:46.136945094Z",
        "log_name": "orders",
        "data": [["chocolate bars", 3], ["donuts", 0]], /* you should provide the same keys (but the order is not important) for all datarows which go to the same sheet, otherwise a separate sheet is created */
    },
    {
        "datetime": "2023-12-09T09:50:46.136945094Z",
        "log_name": "campaigns",
        "data": [["name", "10% discount for buying 3 donuts"], ["is active", true], ["credits", -6], ["datetime", "2023-12-11 09:19:32.827321506"]], /* datatypes for values are string, integer (unsigned 64-bits), boolean, float (64 bits) and datetime string (in common formats without tz) */
    }
]
```

Appending to the log is *not idempotent*, i.e. if you retry the same request two times, then the same set of rows will be added twice. If it is absolutely important to avoid any duplication (which may happen in case of some unexpected failure or retrying logic), then it is recommended to some unique idempotence key to `data` to be able to filter duplicates in the spreadsheet and remove them manually.

Goral KV service responds with an array of urls of sheets for each datarow respectively.
Goral accepts every batch and creates corresponding sheets in the configured spreadsheet for "Orders" and "Marketing Campaigns". And the end of the month you have all the billing data neatly collected.
For even more interactive setup you can share a spreadsheet access with your client (for him/her see all the process online) and configure a messenger for alerts and notifications (see the section [Rules](#rules)) by adding your client to the chat.
Unlike other Goral services, this KV api is synchronous - if Goral responds successfully then sheets are created already and data is saved.

For every append operation Goral uses 2 Google api method calls, so under the quota limit of 300 requests per minute, we have 5 requests per second or 2 append operations (not considering other Goral services which use the same quota). That's why it is strongly recommended to use a batched approach (say send in batches every 10 seconds or so) otherwise you can exhaust [Google api quota](https://developers.google.com/sheets/api/limits) quickly (especially when other Goral services run). [Exponential backoff algorithm](https://developers.google.com/sheets/api/limits#exponential) is *not* applicable to KV service induced requests (in contrast to other Goral services). So retries are on the client app side and you may expect http response status code `429: Too many requests` in case if you generate an excessive load. And it can impact other Goral services.
In any case Goral put KV requests in the messages queue with a capacity 1, so any concurrent request will wait until the previous one is handled.

If there is an error while appending data, it is sent only via a default messenger of General service. Configured messenger is only used for notifications according to configured rules.

*Note*: for KV service the autotruncation mechanism is turned off by default (`autotruncate_at_usage_percent = 100`). It means that you should either set that value to some percent below 100 or clean up old data manually.

Another notable use case is to log console errors from the frontend - catch JS exceptions, accumulate them in some batch at your backend and send the batch to Goral KV service.

## Rules

Every service automatically creates a rules sheet (for some services reasonable defaults are provided) which allows you to set notifications (via configured messengers) on the data as it is collected.
Datetime for rules is optional and is set only for default rules. You choose a log name (everything up to the first `@` in a sheet title) and a key (a column header of a sheet), select a condition which is checked, and action: either send info/warn/error message or skip the rules match from any further rules processing.

Rules are fetched from a rules sheet by every Goral service dynamically every 15-31 seconds (the period is jittered to prevent hitting the Google quota).

## Recommended deployment

Goral follows a fail-fast approach - if something violates assumptions (marked with `assert:`), the safest thing is to panic and restart. It doesn't behave like that for recoverable errors, of course. For example, if Goral cannot send a message via messenger, it will try to message via General service notifications and its logs. And will continue working and collecting data. If it cannot connect to Google API, it will retry first. 

There is also a case of fatal errors (e.g. `MissingToken error` for Google API which usually means that system time has skewed). In that case only someone outside can help. And in case of such panics Goral first tries to notify you via a messenger configured for General service to let you know immediately.

So following Erlang's idea of [supervision trees](https://adoptingerlang.org/docs/development/supervision_trees/) we recommend to run Goral as a systemd service under Linux for automatic restarts in case of panics and other issues. An example systemd configuration:
```
[Unit]
Description=Goral observability daemon
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/goral -c /etc/goral.toml --id myhost
Restart=always
[Install]
WantedBy=multi-user.target
```
If you plan to use System service then you should not containerize Goral to get the actual system data.
Goral implements a graceful shutdown (its duration is configured) for SIGINT (Ctrl+C) and SIGTERM signals to safely send all the data in process to the permanent spreadsheet storage.
