import ws from 'ws';

const WebSocket = global.WebSocket || ws;

interface StatusMessage {
  success: boolean;
  message: string;
}

function isStatusMessage(obj: any): obj is StatusMessage {
  return obj.success !== undefined;
}

export class ReconnectingWebsocketFeed {
  private _url: string;
  protected _socket: WebSocket;
  private _connected: boolean;
  private _reconnectionIntervalMs: number;
  private _reconnectionMaxAttempts: number;
  private _reconnectionAttempts: number;

  private _onConnect: (() => void) | null = null;
  private _onDisconnect:
    | ((reconnectionAttemptsExhausted: boolean) => void)
    | null = null;
  private _onStatus: ((update: StatusMessage) => void) | null = null;
  private _onMessage: ((data: any) => void) | null = null;

  constructor(
    url: string,
    reconnectionIntervalMs?: number,
    reconnectionMaxAttempts?: number,
  ) {
    this._url = url;
    this._reconnectionIntervalMs = reconnectionIntervalMs ?? 5000;
    this._reconnectionMaxAttempts = reconnectionMaxAttempts ?? -1;
    this._reconnectionAttempts = 0;

    this._connect();
  }

  public disconnect() {
    if (this._connected) {
      this._socket.close();
      this._connected = false;
    }
  }

  public connected(): boolean {
    return this._connected;
  }

  public onConnect(callback: () => void) {
    this._onConnect = callback;
  }

  public onDisconnect(
    callback: (reconnectionAttemptsExhausted: boolean) => void,
  ) {
    this._onDisconnect = callback;
  }

  public onStatus(callback: (update: StatusMessage) => void) {
    this._onStatus = callback;
  }

  protected onMessage(callback: (data: any) => void) {
    this._onMessage = callback;
  }

  private _connect() {
    this._socket = new WebSocket(this._url);

    this._socket.addEventListener('error', (err: any) => {
      console.warn(`[MangoFeed] connection error: ${err.message}`);
      if (this._reconnectionAttemptsExhausted()) {
        console.error('[MangoFeed] fatal connection error');
        throw err.error;
      }
    });

    this._socket.addEventListener('open', () => {
      this._connected = true;
      this._reconnectionAttempts = 0;
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
        if (isStatusMessage(data) && this._onStatus) {
          this._onStatus(data);
        } else if (this._onMessage) {
          this._onMessage(data);
        }
      } catch (err) {
        console.warn('[MangoFeed] error deserializing message', err);
      }
    });
  }

  private _reconnectionAttemptsExhausted(): boolean {
    return (
      this._reconnectionMaxAttempts != -1 &&
      this._reconnectionAttempts >= this._reconnectionMaxAttempts
    );
  }
}
