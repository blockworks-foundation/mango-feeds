connector-fills

This module parses event queues and exposes individual fills on a websocket.

TODO:

- [] early filter out all account writes we dont care about
- [] startup logic, dont accept websockets before first snapshot
- [] failover logic, kill all websockets when we receive a later snapshot, more
  frequent when running on home connections
- [] track websocket connect / disconnect
- [] track latency accountwrite -> websocket
- [] track queue length
