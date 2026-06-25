require('dotenv').config();
const WebSocket = require('ws');
const { Kafka } = require('kafkajs');

const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const TOPIC  = process.env.KAFKA_TOPIC  || 'crypto.trades.raw';

const STREAMS = [
  'btcusdt@trade',
  'ethusdt@trade',
];
const WS_URL = `wss://stream.binance.com:9443/stream?streams=${STREAMS.join('/')}`;

const kafka = new Kafka({
  clientId: 'cmms-binance-producer',
  brokers: [BROKER],
  retry: { retries: 10, initialRetryTime: 3000 },
});

const producer = kafka.producer({ allowAutoTopicCreation: false });

let connected = false;
let reconnectTimer = null;

async function initProducer() {
  await producer.connect();
  connected = true;
  console.log('[Binance] Kafka producer connected');
}

function connect() {
  console.log('[Binance] Connecting to WebSocket stream...');
  const ws = new WebSocket(WS_URL);

  ws.on('open', () => {
    console.log('[Binance] WebSocket connected');
    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
  });

  ws.on('message', async (data) => {
    if (!connected) return;
    try {
      const envelope = JSON.parse(data);
      const t = envelope.data;
      if (!t || t.e !== 'trade') return;

      const trade = {
        exchange:  'binance',
        symbol:    t.s,          // e.g. BTCUSDT
        price:     parseFloat(t.p),
        volume:    parseFloat(t.q),
        timestamp: t.T,          // trade time ms
        tradeId:   String(t.t),
        isBuyerMaker: t.m,
      };

      producer.send({
        topic: TOPIC,
        messages: [{
          key: trade.symbol,
          value: JSON.stringify(trade),
          timestamp: String(trade.timestamp),
        }],
      }).catch((err) => console.error('[Binance] Kafka send error:', err.message));

    } catch (err) {
      console.error('[Binance] Parse error:', err.message);
    }
  });

  ws.on('error', (err) => {
    console.error('[Binance] WebSocket error:', err.message);
  });

  ws.on('close', (code) => {
    console.warn(`[Binance] WebSocket closed (code ${code}) — reconnecting in 5s`);
    reconnectTimer = setTimeout(connect, 5000);
  });
}

async function main() {
  try {
    await initProducer();
    connect();
  } catch (err) {
    console.error('[Binance] Fatal startup error:', err.message);
    process.exit(1);
  }
}

process.on('SIGINT', async () => {
  console.log('[Binance] Shutting down...');
  connected = false;
  await producer.disconnect();
  process.exit(0);
});

main();
