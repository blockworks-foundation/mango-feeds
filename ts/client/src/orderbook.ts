import { ReconnectingWebsocketFeed } from './util';

interface OrderbookFeedOptions {
  subscriptions?: OrderbookFeedSubscribeParams;
  reconnectionIntervalMs?: number;
  reconnectionMaxAttempts?: number;
}

interface OrderbookFeedSubscribeParams {
  marketId?: string;
  marketIds?: string[];
}

interface OrderbookL2Update {
  market: string;
  side: 'bid' | 'ask';
  update: [number, number][];
  slot: number;
  writeVersion: number;
}

function isOrderbookL2Update(obj: any): obj is OrderbookL2Update {
  return obj.update !== undefined;
}

interface OrderbookL2Checkpoint {
  market: string;
  side: 'bid' | 'ask';
  bids: [number, number][];
  asks: [number, number][];
  slot: number;
  writeVersion: number;
}

function isOrderbookL2Checkpoint(obj: any): obj is OrderbookL2Checkpoint {
  return obj.bids !== undefined && obj.asks !== undefined;
}

export class OrderbookFeed extends ReconnectingWebsocketFeed {
  private _subscriptions?: OrderbookFeedSubscribeParams;

  private _onL2Update: ((update: OrderbookL2Update) => void) | null = null;
  private _onL2Checkpoint: ((update: OrderbookL2Checkpoint) => void) | null =
    null;

  constructor(url: string, options?: OrderbookFeedOptions) {
    super(
      url,
      options?.reconnectionIntervalMs,
      options?.reconnectionMaxAttempts,
    );
    this._subscriptions = options?.subscriptions;

    this.onMessage((data) => {
      if (isOrderbookL2Update(data) && this._onL2Update) {
        this._onL2Update(data);
      } else if (isOrderbookL2Checkpoint(data) && this._onL2Checkpoint) {
        this._onL2Checkpoint(data);
      }
    });

    if (this._subscriptions !== undefined) {
      this.subscribe(this._subscriptions);
    }
  }

  public subscribe(subscriptions: OrderbookFeedSubscribeParams) {
    if (this.connected()) {
      this._socket.send(
        JSON.stringify({
          command: 'subscribe',
          ...subscriptions,
        }),
      );
    } else {
      console.warn('[OrderbookFeed] attempt to subscribe when not connected');
    }
  }

  public unsubscribe(marketId: string) {
    if (this.connected()) {
      this._socket.send(
        JSON.stringify({
          command: 'unsubscribe',
          marketId,
        }),
      );
    } else {
      console.warn('[OrderbookFeed] attempt to unsubscribe when not connected');
    }
  }

  public onL2Update(callback: (update: OrderbookL2Update) => void) {
    this._onL2Update = callback;
  }

  public onL2Checkpoint(callback: (checkpoint: OrderbookL2Checkpoint) => void) {
    this._onL2Checkpoint = callback;
  }
}
