import { FillsFeed } from '../src';

const RECONNECT_INTERVAL_MS = 1000;
const RECONNECT_ATTEMPTS_MAX = -1;

// Subscribe on connection
const fillsFeed = new FillsFeed('ws://localhost:8080', {
  reconnectionIntervalMs: RECONNECT_INTERVAL_MS,
  reconnectionMaxAttempts: RECONNECT_ATTEMPTS_MAX,
  subscriptions: {
    accountIds: ['9XJt2tvSZghsMAhWto1VuPBrwXsiimPtsTR8XwGgDxK2'],
  },
});

// Subscribe after connection
fillsFeed.onConnect(() => {
  console.log('connected');
  fillsFeed.subscribe({
    marketIds: ['ESdnpnNLgTkBCZRuTJkZLi5wKEZ2z47SG3PJrhundSQ2'],
    headUpdates: true,
  });
});

fillsFeed.onDisconnect(() => {
  console.log(`disconnected, reconnecting in ${RECONNECT_INTERVAL_MS}...`);
});

fillsFeed.onFill((update) => {
  console.log('fill', update);
});

fillsFeed.onHead((update) => {
  console.log('head', update);
});

fillsFeed.onStatus((update) => {
  console.log('status', update);
});
