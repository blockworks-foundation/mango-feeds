import { ReconnectingWebsocketFeed } from './util';

interface FillsFeedOptions {
  subscriptions?: FillsFeedSubscribeParams;
  reconnectionIntervalMs?: number;
  reconnectionMaxAttempts?: number;
}

interface FillsFeedSubscribeParams {
  marketId?: string;
  marketIds?: string[];
  accountIds?: string[];
  headUpdates?: boolean;
}

interface FillEventUpdate {
  status: 'new' | 'revoke';
  marketKey: 'string';
  marketName: 'string';
  slot: number;
  writeVersion: number;
  event: {
    eventType: 'spot' | 'perp';
    maker: 'string';
    taker: 'string';
    takerSide: 'bid' | 'ask';
    timestamp: 'string'; // DateTime
    seqNum: number;
    makerClientOrderId: number;
    takerClientOrderId: number;
    makerFee: number;
    takerFee: number;
    price: number;
    quantity: number;
  };
}

function isFillEventUpdate(obj: any): obj is FillEventUpdate {
  return obj.event !== undefined;
}

interface HeadUpdate {
  head: number;
  previousHead: number;
  headSeqNum: number;
  previousHeadSeqNum: number;
  status: 'new' | 'revoke';
  marketKey: 'string';
  marketName: 'string';
  slot: number;
  writeVersion: number;
}

function isHeadUpdate(obj: any): obj is HeadUpdate {
  return obj.head !== undefined;
}

export class FillsFeed extends ReconnectingWebsocketFeed {
  private _subscriptions?: FillsFeedSubscribeParams;

  private _onFill: ((update: FillEventUpdate) => void) | null = null;
  private _onHead: ((update: HeadUpdate) => void) | null = null;

  constructor(url: string, options?: FillsFeedOptions) {
    super(
      url,
      options?.reconnectionIntervalMs,
      options?.reconnectionMaxAttempts,
    );
    this._subscriptions = options?.subscriptions;

    this.onMessage((data: any) => {
      if (isFillEventUpdate(data) && this._onFill) {
        this._onFill(data);
      } else if (isHeadUpdate(data) && this._onHead) {
        this._onHead(data);
      }
    });

    if (this._subscriptions !== undefined) {
      this.subscribe(this._subscriptions);
    }
  }

  public subscribe(subscriptions: FillsFeedSubscribeParams) {
    if (this.connected()) {
      this._socket.send(
        JSON.stringify({
          command: 'subscribe',
          ...subscriptions,
        }),
      );
    } else {
      console.warn('[FillsFeed] attempt to subscribe when not connected');
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
      console.warn('[FillsFeed] attempt to unsubscribe when not connected');
    }
  }

  public onFill(callback: (update: FillEventUpdate) => void) {
    this._onFill = callback;
  }

  public onHead(callback: (update: HeadUpdate) => void) {
    this._onHead = callback;
  }
}
