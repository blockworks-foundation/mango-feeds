import { OrderbookFeed } from '../src';

const RECONNECT_INTERVAL_MS = 1000;
const RECONNECT_ATTEMPTS_MAX = -1;

// Subscribe on connection
const orderbookFeed = new OrderbookFeed('ws://127.0.0.1:8080', {
  reconnectionIntervalMs: RECONNECT_INTERVAL_MS,
  reconnectionMaxAttempts: RECONNECT_ATTEMPTS_MAX,
  subscriptions: {
    marketId: '9XJt2tvSZghsMAhWto1VuPBrwXsiimPtsTR8XwGgDxK2',
  },
});

// Subscribe after connection
orderbookFeed.onConnect(() => {
  console.log('connected');
  orderbookFeed.subscribe({
    marketId: 'ESdnpnNLgTkBCZRuTJkZLi5wKEZ2z47SG3PJrhundSQ2',
  });
});

orderbookFeed.onDisconnect(() => {
  console.log(`disconnected, reconnecting in ${RECONNECT_INTERVAL_MS}...`);
});

orderbookFeed.onL2Update((update) => {
  console.log('update', update);
});
orderbookFeed.onL2Checkpoint((checkpoint) => {
  console.log('checkpoint', checkpoint);
});
orderbookFeed.onStatus((update) => {
  console.log('status', update);
});
