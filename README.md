Goral
---
Observability toolkit for small projects.

Goral is a simple application developed with the following idea in mind: when you have your favorite application in its infantry, you usually don't need a full-blown observability toolkit around as the amount of data is not so huge. It is especially true for pet projects when you just want to test an idea and deploy the app at some free-tier commodity VPS.

So Goral provides the following features being deployed alongside (e.g. as a sidecar) your app:
* metrics collection (fully compatible with Prometheus to be easily replaced with more advanced stack as your project grows)
* logs collection (importing logs from stdout/stderr of the target process or from syslog)
* periodic healthchecks (aka [liveness probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/))
* resource usage of the server (CPU, Memory, Free/Busy storage space)??

Features are modular - at the basic level you have healthchecks, and metrics/logs/resources are switched on/off in the configuration.

All the data collected is stored in Google Sheet (metrics, logs) with an automatic quota and limits checks and automatic data rotation (old data is deleted).
That way you don't have to buy a separate storage or overload your app VPS with Prometheus etc. Just lean process next to your brilliant one which just sends app data in batches to Google Sheets and Drive for your ease of use. Google Sheets allow you to build your own diagrams over the metrics and analyse them.

Initial Google setup
Under your Google account
1) Create a project https://console.cloud.google.com/projectcreate (we suggest creating a separate GCP project for security reasons)
2) Enable Sheets from the products page https://console.cloud.google.com/workspace-api/products
3) Create a service account https://console.cloud.google.com/workspace-api/credentials with Editor role
4) After creating the service acoount create a private key (type JSON) for it (the private key should be downloaded by your browser)
5) Create a spreadsheet (where your metrics, logs, healtchecks will be stored) and add the service account email as an Editor to the spreadsheet
6) Extract spreadsheet id from the spreadsheet url

Dashboard https://console.cloud.google.com/home/dashboard
Quotas https://console.cloud.google.com/iam-admin/quotas

And notifications are sent to messengers (at the moment, Telegram is supported).

Telegram setup (optional):
1) Create a bot - see https://core.telegram.org/bots/features#creating-a-new-bot
2) Create a private group for notifications to be sent to
3) Add your bot to the group
4) Obtain `chat_id` following the accepted answer https://stackoverflow.com/questions/33858927/how-to-obtain-the-chat-id-of-a-private-telegram-channel

TODO https://api.slack.com/tutorials/tracks/posting-messages-with-curl

You shouldn't
* rename sheets managed by Goral
* change their headers (first row)
* change their header row

Basically, you can delete them (new sheets are autocreated) or filter them with autofilter

### General
Liveness probes follow the same rules as for k8s.
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
- tab colors for service
