# service-mango-orderbook

This module parses bookside accounts and exposes L2 data and updates on a websocket

## Setup

1. Prepare the connector configuration file.

   [Here is an example](service-mango-orderbook/conf/example-config.toml).

   - `bind_ws_addr` is the listen port for the websocket clients
   - `rpc_ws_url` is unused and can stay empty.
   - `connection_string` for your `grpc_sources` must point to the gRPC server
     address configured for the plugin.
   - `rpc_http_url` must point to the JSON-RPC URL.
   - `program_id` must match what is configured for the gRPC plugin
   - `markets` need to contain all observed perp markets

2. Start the service binary.

   Pass the path to the config file as the first argument. It logs to stdout. It
   should be restarted on exit.

3. Monitor the logs

   `WARN` messages can be recovered from. `ERROR` messages need attention. The
   logs are very spammy changing the default log level is recommended when you
   dont want to analyze performance of the service.

## TODO
- [] startup logic, dont accept market subscriptions before first snapshot
- [] failover logic, kill all websockets when we receive a later snapshot, more
  frequent when running on home connections
- [] track latency accountwrite -> websocket
- [] create new model for fills so snapshot maps can be combined per market
