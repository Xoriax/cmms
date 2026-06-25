require('dotenv').config();
const { Kafka } = require('kafkajs');
const { connect: connectMongo } = require('../db/mongoose');
const { Alert } = require('../db/models');
const INTERNAL_URL = `http://localhost:${process.env.API_PORT || 3000}`;

const BROKER   = process.env.KAFKA_BROKER   || 'localhost:9092';
const TOPIC    = process.env.KAFKA_TOPIC    || 'crypto.trades.raw';
const GROUP_ID = process.env.KAFKA_GROUP_ID || 'cmms-consumers';

const kafka = new Kafka({
  clientId: 'cmms-consumer3',
  brokers: [BROKER],
  retry: { retries: 10, initialRetryTime: 3000 },
});

const consumer = kafka.consumer({ groupId: GROUP_ID });

// In-memory state per symbol for anomaly detection
// recentTrades: last N trades for moving average volume
// priceHistory: trades within last 10s for price spike detection
const state = {};

const VOL_WINDOW_SIZE = 50;  // trades used to compute moving average volume
const VOL_SPIKE_MULT  = 3;   // volume > 3× moving avg triggers alert
const PRICE_WINDOW_MS = 10_000; // 10-second window
const PRICE_SPIKE_PCT = 0.01;   // 1% price change

function getOrCreate(symbol) {
  if (!state[symbol]) {
    state[symbol] = {
      recentTrades: [],   // last VOL_WINDOW_SIZE trade volumes
      priceWindow: [],    // { price, ts } within last PRICE_WINDOW_MS
    };
  }
  return state[symbol];
}

function movingAvgVolume(trades) {
  if (trades.length === 0) return 0;
  return trades.reduce((s, v) => s + v, 0) / trades.length;
}

async function run() {
  await connectMongo();
  await consumer.connect();
  console.log('[Consumer3] Connected to Kafka');

  await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const raw = JSON.parse(message.value.toString());
        const symbol = raw.symbol.toUpperCase().replace(/[^A-Z0-9]/g, '');
        const { price, volume, exchange } = raw;
        const ts = raw.timestamp;
        const s = getOrCreate(symbol);

        // ── Volume spike detection ──────────────────────────────────────────
        const avgVol = movingAvgVolume(s.recentTrades);
        if (avgVol > 0 && volume > avgVol * VOL_SPIKE_MULT) {
          const alert = {
            symbol, type: 'LARGE_VOLUME', price, volume,
            threshold: avgVol * VOL_SPIKE_MULT,
            message: `Volume ${volume.toFixed(4)} > ${VOL_SPIKE_MULT}× moving avg (${avgVol.toFixed(4)})`,
            timestamp: new Date(ts), exchange,
          };
          Alert.create(alert).catch(console.error);
          fetch(`${INTERNAL_URL}/internal/alert`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(alert),
          }).catch(() => {});
          console.warn(`[Consumer3] ALERT LARGE_VOLUME ${symbol}: ${alert.message}`);
        }

        // Update rolling volume window
        s.recentTrades.push(volume);
        if (s.recentTrades.length > VOL_WINDOW_SIZE) s.recentTrades.shift();

        // ── Price spike detection ───────────────────────────────────────────
        const cutoff = ts - PRICE_WINDOW_MS;
        // Prune old entries
        let i = 0;
        while (i < s.priceWindow.length && s.priceWindow[i].ts < cutoff) i++;
        if (i > 0) s.priceWindow.splice(0, i);

        if (s.priceWindow.length > 0) {
          const oldest = s.priceWindow[0].price;
          const change = Math.abs((price - oldest) / oldest);
          if (change >= PRICE_SPIKE_PCT) {
            const alert = {
              symbol, type: 'PRICE_SPIKE', price, volume,
              threshold: PRICE_SPIKE_PCT * 100,
              message: `Price moved ${(change * 100).toFixed(2)}% in ${PRICE_WINDOW_MS / 1000}s ($${oldest.toFixed(2)} → $${price.toFixed(2)})`,
              timestamp: new Date(ts), exchange,
            };
            Alert.create(alert).catch(console.error);
            fetch(`${INTERNAL_URL}/internal/alert`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(alert),
          }).catch(() => {});
            console.warn(`[Consumer3] ALERT PRICE_SPIKE ${symbol}: ${alert.message}`);
          }
        }

        s.priceWindow.push({ price, ts });

      } catch (err) {
        console.error('[Consumer3] Process error:', err.message);
      }
    },
  });
}

consumer.on('consumer.crash', async ({ payload }) => {
  console.error('[Consumer3] Crash:', payload.error.message, '— restarting in 10s');
  await consumer.disconnect().catch(() => {});
  setTimeout(run, 10000);
});

process.on('SIGINT', async () => {
  console.log('[Consumer3] Shutting down...');
  await consumer.disconnect();
  process.exit(0);
});

run().catch((err) => {
  console.error('[Consumer3] Fatal:', err.message);
  process.exit(1);
});
