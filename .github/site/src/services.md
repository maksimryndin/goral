# Services

A workhorse abstraction of Goral over a sheet is an appendable log - just a table which grows via addition of key-value records.

A sheet managed by Goral has a title `<log to collect data on>@<host_id>@<service> <creation datetime>`. You can change the title, column names and other elements of the sheet but be aware that Goral will continue to append data in the same order as when the sheet was created by Goral if the form of the data hasn't changed (either `<log to collect data on>` or its keys). Creation datetimes for sheets always differ by some amount of seconds (jittered) even those sheets were created at the same time - in order to prevent conflicts in sheet titles.

Commented lines (starting with `#`) for all configurations below are optional and example values are their defaults.
Every service has a messenger configuration (see [Setup](./setup.md)). It is recommended to have several messengers and different chats/channels for each messenger and take into account their rate limits when configuring a service.

## Storage quota

Every service (except General) has an `autotruncate_at_usage_percent` configuration - the limit of the usage share by a service. Any Google spreadsheet can contain at most 10_000_000 cells so if a services takes more than `autotruncate_at_usage_percent` of 10_000_000 cells, Goral will truncate old data by either removing old rows or removing the same named sheets (`<log to collect data on>`) under a service.
For every service the cleanup will truncate the surplus (actual usage - limit) and 10% of the limit.

When providing your own limits, do remember to have a sum of limits no more than 100% for all services related to the same spreadsheet. Default settings assume conservatively that you write all the data to the same spreadsheet. If [KV service](./kv-log.md) is turned on, its default truncation limit is set for 100% (meaning nothing is truncated as a safe default), so you will get a warning that limits sum is over 100% if you use the same spreadsheet for other services.

Also if a spreadsheet includes other sheets not managed by Goral, take into account their usage.