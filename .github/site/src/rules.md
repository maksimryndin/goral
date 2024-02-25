# Rules

Every service automatically creates a rules sheet (for some services reasonable defaults are provided) which allows you to set notifications (via configured messengers) on the data as it is collected.
Datetime for rules is optional and is set only for default rules. You choose a log name (everything up to the first `@` in a sheet title) and a key (a column header of a sheet), select a condition which is checked, and an action: either send info/warn/error message or skip the rule's match from any further rules processing.

Rules are fetched from a rules sheet by every Goral service dynamically every 30-46 seconds (the period is jittered to prevent hitting the Google quota).
By default warnings on rules update error are suppressed (to remove unnecessary messages noise). You can turn it on for a specific service with the setting:

```toml
messenger.send_rules_update_error = true
```