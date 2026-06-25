require('dotenv').config();
const WebSocket = require('ws');
const { Kafka } = require('kafkajs');

const BROKER  = process.env.KAFKA_BROKER || 'localhost:9092';
const TOPIC   = process.env.KAFKA_TOPIC  || 'crypto.trades.raw';
const WS_URL  = 'wss://advanced-trade-ws.coinbase.com';

const SUBSCRIBE_MSG = JSON.stringify({
  type: 'subscribe',
  product_ids: ['BTC-USD', 'ETH-USD'],
  channel: 'market_trades',
});

const kafka = new Kafka({
  clientId: 'cmms-coinbase-producer',
  brokers: [BROKER],
  retry: { retries: 10, initialRetryTime: 3000 },
});

const producer = kafka.producer({ allowAutoTopicCreation: false });

let connected = false;
let reconnectTimer = null;
let heartbeatTimer = null;

async function initProducer() {
  await producer.connect();
  connected = true;
  console.log('[Coinbase] Kafka producer connected');
}

function connect() {
  console.log('[Coinbase] Connecting to WebSocket stream...');
  const ws = new WebSocket(WS_URL);

  ws.on('open', () => {
    console.log('[Coinbase] WebSocket connected — subscribing');
    ws.send(SUBSCRIBE_MSG);
    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }

    // Coinbase requires periodic pings to stay alive
    heartbeatTimer = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.ping();
      }
    }, 20000);
  });

  ws.on('message', async (data) => {
    if (!connected) return;
    try {
      const msg = JSON.parse(data);

      // channel: market_trades, event type: update
      if (msg.channel !== 'market_trades' || !msg.events) return;

      for (const event of msg.events) {
        if (!event.trades) continue;
        for (const t of event.trades) {
          const trade = {
            exchange:  'coinbase',
            symbol:    t.product_id.replace('-', ''),  // BTC-USD → BTCUSD
            price:     parseFloat(t.price),
            volume:    parseFloat(t.size),
            timestamp: new Date(t.time).getTime(),
            tradeId:   String(t.trade_id),
            side:      t.side,
          };

          producer.send({
            topic: TOPIC,
            messages: [{
              key: trade.symbol,
              value: JSON.stringify(trade),
              timestamp: String(trade.timestamp),
            }],
          }).catch((err) => console.error('[Coinbase] Kafka send error:', err.message));
        }
      }
    } catch (err) {
      console.error('[Coinbase] Parse error:', err.message);
    }
  });

  ws.on('error', (err) => {
    console.error('[Coinbase] WebSocket error:', err.message);
  });

  ws.on('close', (code) => {
    if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; }
    console.warn(`[Coinbase] WebSocket closed (code ${code}) — reconnecting in 5s`);
    reconnectTimer = setTimeout(connect, 5000);
  });
}

async function main() {
  try {
    await initProducer();
    connect();
  } catch (err) {
    console.error('[Coinbase] Fatal startup error:', err.message);
    process.exit(1);
  }
}

process.on('SIGINT', async () => {
  console.log('[Coinbase] Shutting down...');
  connected = false;
  if (heartbeatTimer) clearInterval(heartbeatTimer);
  await producer.disconnect();
  process.exit(0);
});

main();
