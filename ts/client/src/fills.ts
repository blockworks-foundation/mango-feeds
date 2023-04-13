import ws from 'ws';

const WebSocket = global.WebSocket || ws;

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

interface StatusMessage {
  success: boolean;
  message: string;
}

function isStatusMessage(obj: any): obj is StatusMessage {
  return obj.success !== undefined;
}

export class FillsFeed {
  private _url: string;
  private _socket: WebSocket;
  private _subscriptions?: FillsFeedSubscribeParams;
  private _connected: boolean;
  private _reconnectionIntervalMs;
  private _reconnectionAttempts;
  private _reconnectionMaxAttempts;

  private _onConnect: (() => void) | null = null;
  private _onDisconnect:
    | ((reconnectionAttemptsExhausted: boolean) => void)
    | null = null;
  private _onFill: ((update: FillEventUpdate) => void) | null = null;
  private _onHead: ((update: HeadUpdate) => void) | null = null;
  private _onStatus: ((update: StatusMessage) => void) | null = null;

  constructor(url: string, options?: FillsFeedOptions) {
    this._url = url;
    this._subscriptions = options?.subscriptions;
    this._reconnectionIntervalMs = options?.reconnectionIntervalMs ?? 5000;
    this._reconnectionAttempts = 0;
    this._reconnectionMaxAttempts = options?.reconnectionMaxAttempts ?? -1;

    this._connect();
  }

  private _reconnectionAttemptsExhausted(): boolean {
    return (
      this._reconnectionMaxAttempts != -1 &&
      this._reconnectionAttempts >= this._reconnectionMaxAttempts
    );
  }

  private _connect() {
    this._socket = new WebSocket(this._url);

    this._socket.addEventListener('error', (err: any) => {
      console.warn(`[FillsFeed] connection error: ${err.message}`);
      if (this._reconnectionAttemptsExhausted()) {
        console.error('[FillsFeed] fatal connection error');
        throw err.error;
      }
    });

    this._socket.addEventListener('open', () => {
      if (this._subscriptions !== undefined) {
        this._connected = true;
        this._reconnectionAttempts = 0;
        this.subscribe(this._subscriptions);
      }
      if (this._onConnect) this._onConnect();
    });

    this._socket.addEventListener('close', () => {
      this._connected = false;
      setTimeout(() => {
        if (!this._reconnectionAttemptsExhausted()) {
          this._reconnectionAttempts++;
          this._connect();
        }
      }, this._reconnectionIntervalMs);
      if (this._onDisconnect)
        this._onDisconnect(this._reconnectionAttemptsExhausted());
    });

    this._socket.addEventListener('message', (msg: any) => {
      try {
        const data = JSON.parse(msg.data);
        if (isFillEventUpdate(data) && this._onFill) {
          this._onFill(data);
        } else if (isHeadUpdate(data) && this._onHead) {
          this._onHead(data);
        } else if (isStatusMessage(data) && this._onStatus) {
          this._onStatus(data);
        }
      } catch (err) {
        console.warn('[FillsFeed] error deserializing message', err);
      }
    });
  }

  public subscribe(subscriptions: FillsFeedSubscribeParams) {
    if (this._connected) {
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
    if (this._connected) {
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

  public disconnect() {
    if (this._connected) {
      this._socket.close();
      this._connected = false;
    } else {
      console.warn('[FillsFeed] attempt to disconnect when not connected');
    }
  }

  public connected(): boolean {
    return this._connected;
  }

  public onConnect(callback: () => void) {
    this._onConnect = callback;
  }

  public onDisconnect(callback: () => void) {
    this._onDisconnect = callback;
  }

  public onFill(callback: (update: FillEventUpdate) => void) {
    this._onFill = callback;
  }

  public onHead(callback: (update: HeadUpdate) => void) {
    this._onHead = callback;
  }

  public onStatus(callback: (update: StatusMessage) => void) {
    this._onStatus = callback;
  }
}
