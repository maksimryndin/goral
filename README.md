Goral
---
Observability toolkit for small projects. Easy-to-use and compatible with industry standards.


# Overview

Goral is a simple observability daemon developed with the following idea in mind: when you have your favorite application in its infantry, you usually don't need a full-blown observability toolkit (which require much more setup, maintenance and resources) around as the amount of data is not so huge. It is especially true for pet projects when you just want to test an idea and deploy the app at some free-tier commodity VPS.

So Goral provides the following features being deployed alongside (e.g. as a sidecar) your app(s):
* Periodic healthchecks (aka [liveness probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/))
* Metrics collection (fully compatible with Prometheus to be easily replaced with more advanced stack as your project grows)
* Logs collection (importing logs from stdout/stderr of the target process)
* Resource usage of the server (CPU, Memory, Free/Busy storage space etc)
* Features are modular - all services (healthchecks/metrics/logs/resources) are switched on/off in the configuration.
* You can observe several instances of the same app or different apps on the same host with a single Goral
* You can configure different messengers and/or channels for every service (healthchecks/metrics/logs/resources) to get notifications on errors in logs of your service, liveness updates, resources overlimit etc
* All the data collected is stored in Google Sheet with an automatic quota and limits checks and automatic data rotation - old data is deleted with a preliminary notification via configured messenger (see below). That way you don't have to buy a separate storage or overload your app VPS with Prometheus etc. Just lean process next to your brilliant one which just sends app data in batches to Google Sheets for your ease of use. Google Sheets allow you to build your own diagrams over the metrics and analyse them, analyse liveness statistics and calculate uptime etc. By default Goral builds some charts for you.

# Setup

To use Goral you need to have a Google account and obtain a service account:
1) Create a project https://console.cloud.google.com/projectcreate (we suggest creating a separate GCP for Goral project for security reasons)
2) Enable Sheets from the products page https://console.cloud.google.com/workspace-api/products
3) Create a service account https://console.cloud.google.com/workspace-api/credentials with Editor role
4) After creating the service acoount create a private key (type JSON) for it (the private key should be downloaded by your browser)
5) Create a spreadsheet (where your metrics, logs, healtchecks will be stored) and add the service account email as an Editor to the spreadsheet
6) Extract spreadsheet id from the spreadsheet url

Dashboard https://console.cloud.google.com/home/dashboard
Quotas https://console.cloud.google.com/iam-admin/quotas

And notifications are sent to messengers (at the moment, only Telegram is supported).

Telegram setup:
1) Create a bot - see https://core.telegram.org/bots/features#creating-a-new-bot
2) Create a private group for notifications to be sent to
3) Add your bot to the group
4) Obtain `chat_id` following the accepted answer https://stackoverflow.com/questions/33858927/how-to-obtain-the-chat-id-of-a-private-telegram-channel

TODO https://api.slack.com/tutorials/tracks/posting-messages-with-curl

You shouldn't change headers (first row) of sheets managed by Goral (which have titles <host_id>:<service>:<parameter> <update datetime>). If you change them, Goral will delete that sheet and recreate new one but you will lose your collected data.

# Services

### General

General service is responsible for reserved communication channel and important notifications about Goral itself. Its configuration

```toml
[general]
#log_level = "info"
service_account_credentials_path = "/path/to/service_account.json"
messenger.bot_token = "<bot token>"
messenger.chat_id = "<chat id>"
messenger.url = "https://api.telegram.org/bot<bot token>/sendMessage"
```
Commented lines (starting with #) are optional and an example values are their defaults. 

### Healthcheck

Liveness probes follow the same rules as for [k8s](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/). Namely:
* you should choose among HTTP GET (any status `>=200` and `<400`), gRPC, TCP (Goral can open socket) and command (exit successfully)
* you can specify initial delay, period of probe and timeout on probe
* misconfiguration of a liveness probe is considered a failure
* for gRPC Health service should be configured on the app side (see also [gRPC health checking protocol](https://github.com/grpc/grpc/blob/v1.59.1/doc/health-checking.md))

Goral saves probe time, status (true for alive) and text output (for HTTP GET - response text, for command - stdout output, for all probes - error text). Each probe is saved at a separate sheet with its own uptime chart.

### Metrics
### Logs
### Resources

## Recommended deployment

Under supervisor - either container orchestrator or Docker daemon, or systemd.
If you plan to use resources metrics then you should not containerize Goral to get the actual system data.

# Development
Run an example server
```
cargo run --example testapp
```

TODO
- validation for the same host for endpoints in Metrics And Healthchecks
- Slack and Discord support
- Each service with screenshots and features
- Minimal and maximal configurations
- Systemd service in help
- Create with Sheets some logs explorer (i.e. how to get specific logs and correlate them?)
- For logs - send error message and link to the spreadsheet where the error is and what row??
- LICENCE
- CONRIBUTING AND ARCHITECTURE
- TCP/Command/gRPC probes
- Metrics alerting rules/policies
- All messages and sheets must be identified by host id
- host_id length validation (less than 16 chars)
- Windows and MacOs support?
