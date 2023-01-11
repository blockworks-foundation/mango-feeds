# mango-geyser-services

Mango v4 Geyser Services

# Components

- [`lib/`](lib/)

  Tools for building services

- [`service-mango-fills/`](service-mango-fills/)

  A service providing lowest-latency, bandwidth conserving access to fill events for Mango V4 Perp and Openbook markets
  as they are processed by the rpc node.

  - [`service-mango-pnl/`](service-mango-pnl/)

  A service providing pre-computed account lists ordered by unsettled PnL per market

  - [`service-mango-orderbook/`](service-mango-pnl/)

  A service providing Orderbook L2 state and delta updates for Mango V4 Perp and Openbook Spot markets
