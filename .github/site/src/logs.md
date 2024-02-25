# Logs

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
Goral tries to extract a log level and datetime from a log line. If it fails to extract a log level then `N/A` is displayed. If it fails to extract datetime, then the current system time is used.

For logs collection Goral reads its stdin. Basically it is a portable way to collect stdout of another process without a privileged access.
There is a caveat - if we make a simple pipe like `instrumented_app | goral` then in case of a termination of the `instrumented_app` Goral will not see any input and will stop reading.
[There is a way with named pipes](https://www.baeldung.com/linux/stdout-to-multiple-commands#3-solve-the-problem-usingtee-and-named-pipes) (for Windows there should also be a way as it also supports named pipes). 
* You create a named pipe, say `instrumented_app_logs_pipe` with the command `mkfifo instrumented_app_logs_pipe` (it creates a pipe file in the current directory - you can choose an appropriate place)
* The named pipe exists till there is at least one writer. So we create a fake one `while true; do sleep 365d; done >instrumented_app_logs_pipe &`
* start Goral with its usual command args and the pipe: `goral -c config.toml --id "host" <instrumented_app_logs_pipe`
* start you app with `instrumented_app | tee instrumented_app_logs_pipe` - you will see your logs in stdout as before and they also be cloned to the named pipe which is read by Goral.

With this named pipes approach the `instrumented_app` restarts doesn't stop Goral from reading its stdin for logs.
Just be sure to autorecreate a fake writer in case of a host system restarts.
See also [Deployment](./recommended-deployment.md) section for an example.

As there may be a huge amount of logs, it is recommended to filter the volume by specifiying an array of substrings (_case sensitive_) in `filter_if_contains` (e.g. `["info", "warn", "error"]`) and `drop_if_contains`, and/or have a separate spreadsheet for logs collection as a huge amount of them may hurdle the use of Google sheets due to the constant UI updates.