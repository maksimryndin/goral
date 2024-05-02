# Recommended deployment

Goral follows a fail-fast approach - if something violates assumptions (marked with `assert:`), the safest thing is to panic and restart. It doesn't behave like that for recoverable errors, of course. For example, if Goral cannot send a message via messenger, it will try to message via General service notifications and its logs. And will continue working and collecting data. If it cannot connect to Google API, it will retry first.

For some services like healthcheck you may want to suppress sending of Google API errors with 

```toml
messenger.send_google_append_error = false
```

to minimize unactionable notifications. Failure to append rows to a spreadsheet doesn't impact [rules notifications](./rules.md) and [healthchecks](./healthcheck.md) as they are triggered before an append operation.

There is also a case of fatal errors (e.g. `MissingToken error` for Google API which usually means that system time has skewed). In that case only someone outside can help. And in case of such panics Goral first tries to notify you via a messenger configured for General service to let you know immediately.

## Linux

So following Erlang's idea of [supervision trees](https://adoptingerlang.org/docs/development/supervision_trees/) we recommend to run Goral as a systemd service under Linux for automatic restarts in case of panics and other issues. 

1. [Install](./installation.md) Goral
2. Make it system-wide available as an executable
```sh
sudo mv goral /usr/local/bin/goral
```
3. An example systemd configuration (can be created with `sudo systemctl edit --force --full goral.service`):
```
[Unit]
Description=Goral observability daemon
After=network.target
StartLimitIntervalSec=0

[Service]
Type=simple
ExecStart=/usr/local/bin/goral -c /etc/goral.toml --id myhost
Restart=always
User=myuser

[Install]
WantedBy=multi-user.target
```

*Note*: `User=myuser` will limit `top_open_files` for [System](./system.md) to processes only under `myuser`. So if you monitor open file descriptors of some process, make sure to run both it and Goral under the same user.

4. Create a service account file (e.g. at `/etc/goral_service_account.json`) and a config file `/etc/goral.toml`
5. Enable for restart after boot `sudo systemctl enable goral`
6. Start the service `sudo systemctl start goral`

Then to check errors in Goral's log (if any error is reported via a messenger):

```sh
sudo journalctl --no-pager -u goral -g ERROR
```

If you plan to use [System](./system.md) service then you should not containerize Goral to get the actual system data.
Goral implements a graceful shutdown (its duration is configured) for SIGINT (Ctrl+C) and SIGTERM signals to safely send all the data in process to the permanent spreadsheet storage.