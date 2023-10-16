Goral
---
Observability toolkit for small projects. Easy-to-use and compatible with industry standards.


# Overview

Goral is a simple observability daemon developed with the following idea in mind: when you have your favorite application in its infantry, you usually don't need a full-blown observability toolkit (which require much more setup, maintenance and resources) around as the amount of data is not so huge. It is especially true for pet projects when you just want to test an idea and deploy the app at some free-tier commodity VPS.

So Goral provides the following features being deployed next to your app(s):
* Periodic healthchecks (aka [liveness probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/))
* Metrics collection (fully compatible with Prometheus to be easily replaced with more advanced stack as your project grows)
* Logs collection (importing logs from stdout/stderr of the target process)
* Resource usage of the server (CPU, Memory, Free/Busy storage space etc)
* A general key-value appendable log storage (see the user case below)
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

Sheet managed by Goral has title <host_id>:<service>:<parameter to collect data on> <update datetime> <sheet id>. You can change the title, column names and other elemens of the sheet but be aware that Goral will continue to append data in the same order as when the sheet was created by Goral if the form of the data hasn't changed (in that case a new sheet is created).

## Services

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
* you should choose among HTTP GET (any status `>=200` and `<400`), gRPC, TCP (Goral can open socket) and command (successful exit)
* you can specify initial delay, period of probe and timeout on probe
* misconfiguration of a liveness probe is considered a failure
* for gRPC Health service should be configured on the app side (see also [gRPC health checking protocol](https://github.com/grpc/grpc/blob/v1.59.1/doc/health-checking.md))

Goral saves probe time, status (true for alive) and text output (for HTTP GET - response text, for command - stdout output, for all probes - error text). Each probe is saved at a separate sheet with its own uptime chart.
In case an output is larger than 1024 bytes, it is truncated and you get permanent warnings in logs of Goral. So configure the output size of your healthcheck reasonably (healthcheck responses shouldn't be heavy).
For command healthchecks it is recommended to wrap you command in some script or give it an alias so that in case of small changes in command arguments use the same sheet for data (otherwise Goral would create a new sheet).

### Metrics

Metrics scrape endpoints with Prometheus metrics. Maximum response body is set to 16384 bytes.
### Logs
### Resources

### KV

Goral allows you to append key-value data to Google spreadsheet. Let's take an example. You provide SAAS for wholesalers and have several services, let's say "Orders" and "Marketing Campaigns". 
Your client asks you for a billing data for each of the services in a spreadsheet format at the end of the month. You turn on KV Goral service with the following configuration:


Such a configuration runs a server process in the Goral daemon listening at the port `<port>` (localhost only for security reasons). From your app you periodically make a batch POST request to `localhost:<port>/api/kv` with a json body:
```json
[
    {
        "datetime": "", // in rfc*** format
        "log_name": "orders",
        "data": [["donuts", 10], ["chocolate bars", 3]], // first datarow in "orders" log defines order of column headers for a sheet (if it should be created)
    },
    {
        "datetime": "", // in rfc*** format
        "log_name": "orders",
        "data": [["chocolate bars", 3], ["donuts", 0]], // you should provide the same keys for all datarows which go to the same sheet, otherwise a separate sheet is created
    },
    {
        "datetime": "", // in rfc*** format
        "log_name": "campaigns",
        "data": [["name", "10% discount for buying 3 donuts"], ["is active", true], ["credits", -6]], // datatypes for values are string, boolean and float (64 bits). Even if a number is an integer, it will be parsed as float.
    }
]
```

Goral KV service responds with an array of urls of sheets for each datarow respectively.
Goral accepts every batch and creates corresponding sheets in the configured spreadsheet for "Orders" and "Marketing Campaigns". And the end of the month you have all the billing data neatly collected.
For even more interactive setup you can share a spreadsheet access with your client (for him/her see all the process online) and configure a messenger for alerts and notifications (see the section "Rules") by adding your client to the chat.
Unlike other Goral services, this KV api is synchronous - if Goral responds successfully then sheets are created already and data is saved.

For every append operation Goral uses 2 method calls, so under quota limit of 300 requests per minute, we have 5 requests per second or 2 append operations (not considering other Goral services which use the same quota). That's why it is strongly recommended to use a batched approach (say send in batches every 10 seconds or so) otherwise you can exhaust [Google api quota](https://developers.google.com/sheets/api/limits) quickly (especially when other Goral services run). [Exponential backoff algorithm](https://developers.google.com/sheets/api/limits#exponential) is *not* applicable to KV service induced requests (in contrast to other Goral services). So retries are on the client app side and you may expect http response status code `429: Too many requests` in case if you generate an excessive load. And it can impact other Goral services.

## Recommended deployment

Goral follows a fail-fast approach - if something violates assumptions (marked with `assert:`), the safest thing is to panic and restart. It doesn't behave like that for recoverable errors, of course. For example, if Goral cannot send a message to messenger, it will try to message via General service notifications, its logs. And will continue working and collecting data. If it cannot connect to Google API, it will retry. 

There is also a case of fatal errors (e.g. `MissingToken error` for Google API which usually means that system time has skewed). In that case only someone outside can help. And in case of such panics Goral first tries to notify you via a messenger configured for General service to let you know immediately.

So following Erlang's idea of [supervision trees](https://adoptingerlang.org/docs/development/supervision_trees/) we recommend to run Goral as a systemd service under Linux for automatic restarts in case of panics and other issues. An example systemd configuration:
```
```
If you plan to use Resources service then you should not containerize Goral to get the actual system data.
Goral implements a graceful shutdown (its duration is configured) for SIGINT (Ctrl+C) and SIGTERM signals to safely send all the data in process to the permanent spreadsheet storage.

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
- Windows and MacOs support?
