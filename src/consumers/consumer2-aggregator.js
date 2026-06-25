require('dotenv').config();
const { Kafka } = require('kafkajs');
const { connect: connectMongo } = require('../db/mongoose');
const { Aggregate } = require('../db/models');
const INTERNAL_URL = `http://localhost:${process.env.API_PORT || 3000}`;

const BROKER   = process.env.KAFKA_BROKER   || 'localhost:9092';
const TOPIC    = process.env.KAFKA_TOPIC    || 'crypto.trades.raw';
const GROUP_ID = process.env.KAFKA_GROUP_ID || 'cmms-consumers';

const kafka = new Kafka({
  clientId: 'cmms-consumer2',
  brokers: [BROKER],
  retry: { retries: 10, initialRetryTime: 3000 },
});

const consumer = kafka.consumer({ groupId: GROUP_ID });

// In-memory sliding windows per symbol: { [symbol]: [{ price, volume, ts }] }
const windows = {};

const WINDOW_CONFIGS = [
  { name: '1min',  ms: 60_000 },
  { name: '5min',  ms: 5 * 60_000 },
  { name: '15min', ms: 15 * 60_000 },
  { name: '1h',    ms: 60 * 60_000 },
];

function getOrCreate(symbol) {
  if (!windows[symbol]) windows[symbol] = [];
  return windows[symbol];
}

function prune(buf, cutoff) {
  let i = 0;
  while (i < buf.length && buf[i].ts < cutoff) i++;
  if (i > 0) buf.splice(0, i);
}

function computeWindow(buf, windowMs, now) {
  const cutoff = now - windowMs;
  const slice = buf.filter(t => t.ts >= cutoff);
  if (slice.length === 0) return null;
  const avgPrice  = slice.reduce((s, t) => s + t.price, 0) / slice.length;
  const cumVolume = slice.reduce((s, t) => s + t.volume, 0);
  return { avgPrice, cumVolume, tradeCount: slice.length };
}

async function run() {
  await connectMongo();
  await consumer.connect();
  console.log('[Consumer2] Connected to Kafka');

  await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const raw = JSON.parse(message.value.toString());
        const symbol = raw.symbol.toUpperCase().replace(/[^A-Z0-9]/g, '');
        const buf = getOrCreate(symbol);

        const entry = { price: raw.price, volume: raw.volume, ts: raw.timestamp };
        buf.push(entry);

        // Keep at most 1h of data in memory
        prune(buf, entry.ts - 60 * 60_000);

        const now = entry.ts;
        const statsPayload = { symbol, windows: {}, computedAt: new Date(now) };

        for (const { name, ms } of WINDOW_CONFIGS) {
          const result = computeWindow(buf, ms, now);
          if (!result) continue;

          statsPayload.windows[name] = result;

          // Fire-and-forget upsert
          Aggregate.findOneAndUpdate(
            { symbol, window: name },
            { ...result, computedAt: new Date(now) },
            { upsert: true }
          ).catch(console.error);
        }

        fetch(`${INTERNAL_URL}/internal/stats`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(statsPayload),
        }).catch(() => {});

      } catch (err) {
        console.error('[Consumer2] Process error:', err.message);
      }
    },
  });
}

consumer.on('consumer.crash', async ({ payload }) => {
  console.error('[Consumer2] Crash:', payload.error.message, '— restarting in 10s');
  await consumer.disconnect().catch(() => {});
  setTimeout(run, 10000);
});

process.on('SIGINT', async () => {
  console.log('[Consumer2] Shutting down...');
  await consumer.disconnect();
  process.exit(0);
});

run().catch((err) => {
  console.error('[Consumer2] Fatal:', err.message);
  process.exit(1);
});
