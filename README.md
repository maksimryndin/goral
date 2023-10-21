# Goral

Observability toolkit for small projects. Easy-to-use and compatible with industry standards.

[Overview](#overview)
    [System requirements](#system-requirements)
[Setup](#setup)
[Services](#services)
    [General](#general)
    [Healthcheck](#healthcheck)
    [Metrics](#metrics)
    [Logs](#logs)
    [System](#system)
    [KV](#kv)
[Recommended deployment](#recommended-deployment)

## Overview

Goral is a simple observability daemon developed with the following idea in mind: when you have your favorite application in its infantry, you usually don't need a full-blown observability toolkit (which require much more setup, maintenance and resources) around as the amount of data is not so huge. It is especially true for pet projects when you just want to test an idea and deploy the app at some free-tier commodity VPS.

So Goral provides the following features being deployed next to your app(s):
* Periodic healthchecks (aka [liveness probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/))
* Metrics collection (fully compatible with Prometheus to be easily replaced with more advanced stack as your project grows)
* Logs collection (importing logs from stdout/stderr of the target process)
* System telemetry (CPU, Memory, Free/Busy storage space etc)
* A general key-value appendable log storage (see [the user case below](#kv))
* Features are modular - all services (healthchecks/metrics/logs/system/kv) are switched on/off in the configuration.
* You can observe several instances of the same app or different apps on the same host with a single Goral daemon (except logs as logs are collected via stdin of Goral - see [below](#logs))
* You can configure different messengers and/or channels for every service (healthchecks/metrics/logs/resources) to get notifications on errors in logs of your service, liveness updates, resources overlimit etc
* All the data collected is stored in Google Sheet with an automatic quota and limits checks and automatic data rotation - old data is deleted with a preliminary notification via configured messenger (see below). That way you don't have to buy a separate storage or overload your app VPS with Prometheus etc. Just a lean process next to your brilliant one which just sends app data in batches to Google Sheets for your ease of use. Google Sheets allow you to build your own diagrams over the metrics and analyse them, analyse liveness statistics and calculate uptime etc. By default Goral builds some charts for you.
* You can configure different spreadsheets for every service

### System requirements

Memory: RSS 30-40M, 200-300M of virtual memory. An actual requirement may be lower - as it depends on the amount of data, scrape and push intervals (see below for each [service](#services))
Platforms: Linux. MacOS. Other platform will probably work also

## Setup

To use Goral you need to have a Google account and obtain a service account:
1) Create a project https://console.cloud.google.com/projectcreate (we suggest creating a separate GCP for Goral project for security reasons)
2) Enable Sheets from the products page https://console.cloud.google.com/workspace-api/products
3) Create a service account https://console.cloud.google.com/workspace-api/credentials with Editor role
4) After creating the service acoount create a private key (type JSON) for it (the private key should be downloaded by your browser)
5) Create a spreadsheet (where your metrics, logs, healtchecks will be stored) and add the service account email as an Editor to the spreadsheet
6) Extract spreadsheet id from the spreadsheet url

And notifications are sent to messengers (at the moment, only Telegram is supported).

Telegram setup:
1) Create a bot - see https://core.telegram.org/bots/features#creating-a-new-bot
2) Create a private group for notifications to be sent to
3) Add your bot to the group
4) Obtain `chat_id` following the accepted answer https://stackoverflow.com/questions/33858927/how-to-obtain-the-chat-id-of-a-private-telegram-channel

## Services

Sheet managed by Goral has title "<parameter to collect data on>@<host_id>@<service> <creation datetime>". You can change the title, column names and other elements of the sheet but be aware that Goral will continue to append data in the same order as when the sheet was created by Goral if the form of the data hasn't changed (either <parameter to collect data on> or its keys). Creation datetimes for sheets always differ by some amount of seconds (jittered) even those sheets were created at the same time - in order to prevent conflicts in sheet titles.

### General

General service is responsible for reserved communication channel and important notifications about Goral itself. Its configuration

```toml
[general]
# log_level = "info"
service_account_credentials_path = "/path/to/service_account.json"
messenger.bot_token = "<bot token>"
messenger.chat_id = "<chat id>"
messenger.url = "<messenger api url for sending messages>"
```
Commented lines (starting with #) are optional and an example values are their defaults. 

### Healthcheck

```toml
[healthcheck]
# messenger.bot_token = "<bot token>"
# messenger.chat_id = "<chat id>"
# messenger.url = "<messenger api url for sending messages>"
spreadsheet_id = "<spreadsheet_id>"
# push_interval_secs = 30
[[healthcheck.liveness]]
# initial_delay_secs = 0
# period_secs = 3
typ = "Http"
endpoint = "http://127.0.0.1:9898"
# timeout_ms = 3000 # should be less than or equal period_secs
```

Healthcheck creates sheet for every liveness probe.

Liveness probes follow the same rules as for [k8s](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/). Namely:
* you should choose among HTTP GET (any status `>=200` and `<400`), gRPC, TCP (Goral can open socket) and command (successful exit)
* you can specify initial delay, period of probe and timeout on probe
* misconfiguration of a liveness probe is considered a failure
* for gRPC Health service should be configured on the app side (see also [gRPC health checking protocol](https://github.com/grpc/grpc/blob/v1.59.1/doc/health-checking.md))

Goral saves probe time, status (true for alive) and text output (for HTTP GET - response text, for command - stdout output, for all probes - error text). Each probe is saved at a separate sheet with its own uptime chart.
In case an output is larger than 1024 bytes, it is truncated and you get permanent warnings in logs of Goral. So configure the output size of your healthcheck reasonably (healthcheck responses shouldn't be heavy).
For command healthchecks it is recommended to wrap you command in some script or give it an alias so that in case of small changes in command arguments use the same sheet for data (otherwise Goral will create a new sheet).

### Metrics

Metrics scrape endpoints with Prometheus metrics. Maximum response body is set to 16384 bytes.

```toml
[metrics]
spreadsheet_id = "<spreadsheet_id>"
# messenger.bot_token = "<bot token>"
# messenger.chat_id = "<chat id>"
# messenger.url = "<messenger api url for sending messages>"
# push_interval_secs = 30
# scrape_interval_secs = 10
#scrape_timeout_ms = 3000
endpoints = ["<prometheus metrics endpoint1>", "<prometheus metrics endpoint2>" ...]
#[[metrics.rules]]
```

For every endpoint and every metric Metrics service creates a separate sheet. When several endpoints are scraped, their sheet names start with `<port>:` to distinguish several instances of the same app.

If there is an error while scraping, it is sent via a configured messenger or via a default messenger of General service.

### Logs

Logs service with the following configuration:

```toml
[logs]
spreadsheet_id = "<spreadsheet_id>"
# filter_if_contains = []
# messenger.bot_token = "<bot token>"
# messenger.chat_id = "<chat id>"
# messenger.url = "<messenger api url for sending messages>"
# push_interval_secs = 30
#[[logs.rules]]
```

will create a single sheet with logs.

For logs collecting Goral reads its stdin. Basically it is a portable way to collect stdout of another process without a privileged access (ptrace is a little bit hacky for our purposes, has too much power and is limited to nix systems).
There is a caveat - if we make a simple pipe like `instrumented_app | goral` then in case of a termination of the `instrumented_app` Goral will not see any input and will stop reading.
[There is a way with named pipes](https://www.baeldung.com/linux/stdout-to-multiple-commands#3-solve-the-problem-usingtee-and-named-pipes) (for Windows there should also be a way as it also supports named pipes). 
* You create a named pipe, say `instrumented_app_logs_pipe` with the command `mkfifo instrumented_app_logs_pipe` (it creates a pipe file in the current directory - you can choose an appropriate place)
* The named pipe exists till there is at least one writer. So we create a fake one `while true; do sleep 365d; done >instrumented_app_logs_pipe &`
* start Goral with its usual command args and the pipe: `goral -c config.toml --id "host" <instrumented_app_logs_pipe`
* start you app with `instrumented_app | tee instrumented_app_logs_pipe` - you will see your logs in stdout as before and they also be cloned to the named pipe which is read by Goral.

With this named pipes approach the `instrumented_app` restarts doesn't stop Goral from reading its stdin for logs.
Just be sure to autorecreate a named pipe and a fake writer in case of a host system restarts.
See also [Deployment](#recommended-deployment) section for an example.

As there may be a huge amount of logs, it is recommended to filter the volume by specifiying an array of substrings (_case sensitive_) in `filter_if_contains` (e.g. `["info", "warn", "error"]`) and/or have a separate spreadsheet for log collection as a huge amount of them may hurdle the use of Google sheets due to the constant updates.

### System

System service configuration

```toml
[system]
spreadsheet_id = "<spreadsheet_id>"
# push_interval_secs = 30
# scrape_interval_secs = 10
# scrape_timeout_ms = 3000
# messenger.bot_token = "<bot token>"
# messenger.chat_id = "<chat id>"
# messenger.url = "<messenger api url for sending messages>"
# mounts = ["/"]
# names = ["goral"]
#[[system.rules]]
```

With this configuration System service will create following sheets:
* basic: with general information about the system (boot time, memory, cpus, swap, number of processes)
* network: for every network interface number of bytes read/written _since the previous measurement_, total read/written
* top_disk_read - process which has read the most from disk _during the last second_
* top_disk_write - process which has written the most to disk _during the last second_
* top_cpu - process with the most cpu time during the last second
* top_memory - process with the most memory usage _during the last second_
* top_open_files (for Linux only) - among the processes with the same user as goral (!) - process with the most opened files
* for every process with name containing one of the substrings in `names` - a sheet with process info. Note: the first match (_case sensitive_) is used so plan accordingly a unique name for your binary.
* for every mount in `mounts` - disk usage and free space.

System service doesn't require root privileges to collect the telemetry.
For a process a cpu usage percent may be [more than 100%](https://blog.guillaume-gomez.fr/articles/2021-09-06+sysinfo%3A+how+to+extract+systems%27+information) in a case of a multi-threaded process on several cores. `memory_used` by process is a [resident-set size](https://www.baeldung.com/linux/resident-set-vs-virtual-memory-size).

If there is an error while collecting system info, it is sent via a configured messenger or via a default messenger of General service.

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

Goral follows a fail-fast approach - if something violates assumptions (marked with `assert:`), the safest thing is to panic and restart. It doesn't behave like that for recoverable errors, of course. For example, if Goral cannot send a message to messenger, it will try to message via General service notifications, its logs. And will continue working and collecting data. If it cannot connect to Google API, it will retry first. 

There is also a case of fatal errors (e.g. `MissingToken error` for Google API which usually means that system time has skewed). In that case only someone outside can help. And in case of such panics Goral first tries to notify you via a messenger configured for General service to let you know immediately.

So following Erlang's idea of [supervision trees](https://adoptingerlang.org/docs/development/supervision_trees/) we recommend to run Goral as a systemd service under Linux for automatic restarts in case of panics and other issues. An example systemd configuration:
```
```
If you plan to use Resources service then you should not containerize Goral to get the actual system data.
Goral implements a graceful shutdown (its duration is configured) for SIGINT (Ctrl+C) and SIGTERM signals to safely send all the data in process to the permanent spreadsheet storage.

TODO logs example with fake writer