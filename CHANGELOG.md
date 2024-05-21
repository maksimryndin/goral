* 0.1.7
    * ssh log monitoring
    * rules for text now support "is" and "is not" conditions

* 0.1.6
    * safe numbers conversions
    * ids collision tests
    * latency measurements for healthchecks

* 0.1.5
    * increase max body size for latest release check
    * KV server shutdown message fix

* 0.1.4
    * minor improvements for notifications
    * urls with specified domains do not require ports

* 0.1.3
    * fix version message for telegram
    * releases for other platforms
    * fine grained notifications control
    * installer shell script https://maksimryndin.github.io/goral/install.sh
    * fix port validation for domain-specified urls
    * a separate website for docs https://maksimryndin.github.io/goral

* 0.1.2
    * improve an append error handling and reporting
    * rules are applied also at shutdown
    * a welcome service message is sent to a service messenger instead of a general service messenger
    * rules update warn is sent to a service messenger first
    * rules update interval is increased
    * the append timeout is set to the maximum backoff
    * rule fetch timeout is decreased to 2000ms (from 3000ms)

* 0.1.1
    * no panic for Google API access failure - just send an error to a messenger
    * rule fetch timeout is increased to 3000ms (from 1000ms)
    * if process user cannot be retrieved, NA is returned
    * fix fetch of a user id of a process
    * fix exponential backoff algorithm (decrease jittered)
    * fix repetitive truncation warning and truncation algorithm
    * binary size is reduced (by stripping debug info)

* 0.1.0
    * first public release 