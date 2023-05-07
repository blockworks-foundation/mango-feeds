# service-mango-orderbook

This module parses bookside accounts and exposes L2 data and updates on a websocket

Public API: `https://api.mngo.cloud/orderbook/v1/`

## API Reference

Get a list of markets

```
{
   "command": "getMarkets"
}
```

```
{
    "ESdnpnNLgTkBCZRuTJkZLi5wKEZ2z47SG3PJrhundSQ2": "SOL-PERP",
    "HwhVGkfsSQ9JSQeQYu2CbkRCLvsh3qRZxG6m4oMVwZpN": "BTC-PERP",
    "Fgh9JSZ2qfSjCw9RPJ85W2xbihsp2muLvfRztzoVR7f1": "ETH-PERP",
}
```

Subscribe to markets

```
{
   "command": "subscribe"
   "marketId": "MARKET_PUBKEY"
}
```

```
{
    "success": true,
    "message": "subscribed to market MARKET_PUBKEY"
}
```

L2 Checkpoint - Sent upon initial subscription

```
{
    "market": "ESdnpnNLgTkBCZRuTJkZLi5wKEZ2z47SG3PJrhundSQ2",
    "bids":
        [22.17, 8.86],
        [22.15, 88.59],
    ],
    "asks": [
        [22.19, 9.17],
        [22.21, 91.7],
    ],
    "slot": 190826373,
    "write_version": 688377208758
}
```

L2 Update - Sent per side

```
{
    "market": "ESdnpnNLgTkBCZRuTJkZLi5wKEZ2z47SG3PJrhundSQ2",
    "bids":          // or asks
        [22.18, 6],  // new level added
        [22.17, 1],  // level changed
        [22.15, 0],  // level removed
    ],
    "slot": 190826375,
    "write_version": 688377208759
}
```


## Setup

## Local

1. Prepare the connector configuration file.

   [Here is an example](service-mango-orderbook/conf/example-config.toml).

   - `bind_ws_addr` is the listen port for the websocket clients
   - `rpc_ws_url` is unused and can stay empty.
   - `connection_string` for your `grpc_sources` must point to the gRPC server
     address configured for the plugin.
   - `rpc_http_url` must point to the JSON-RPC URL.
   - `program_id` must match what is configured for the gRPC plugin

2. Start the service binary.

   Pass the path to the config file as the first argument. It logs to stdout. It
   should be restarted on exit.

3. Monitor the logs

   `WARN` messages can be recovered from. `ERROR` messages need attention. The
   logs are very spammy changing the default log level is recommended when you
   dont want to analyze performance of the service.

## fly.io

