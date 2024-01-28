* 0.1.1
    - no panic for Google API access failure - just send an error to a messenger
    - rule fetch timeout is increased to 3000ms (from 1000ms)
    - if process user cannot be retrieved, NA is returned
    - fix fetch of a user id of a process
    - fix exponential backoff algorithm (decrease jittered)
    - fix repetitive truncation warning and truncation algorithm
    - binary size is reduced (by stripping debug info)

* 0.1.0
    - first public release 