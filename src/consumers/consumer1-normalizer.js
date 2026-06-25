require('dotenv').config();
const { Kafka } = require('kafkajs');
const { connect: connectMongo } = require('../db/mongoose');
const { TradeRaw } = require('../db/models');
const INTERNAL_URL = `http://localhost:${process.env.API_PORT || 3000}`;

const BROKER   = process.env.KAFKA_BROKER  || 'localhost:9092';
const TOPIC    = process.env.KAFKA_TOPIC   || 'crypto.trades.raw';
const GROUP_ID = process.env.KAFKA_GROUP_ID || 'cmms-consumers';

const kafka = new Kafka({
  clientId: 'cmms-consumer1',
  brokers: [BROKER],
  retry: { retries: 10, initialRetryTime: 3000 },
});

const consumer = kafka.consumer({ groupId: GROUP_ID });

// Normalize symbol to canonical form: BTCUSDT, ETHUSDT, BTCUSD, etc.
function normalizeSymbol(raw) {
  return raw.toUpperCase().replace(/[^A-Z0-9]/g, '');
}

async function run() {
  await connectMongo();
  await consumer.connect();
  console.log('[Consumer1] Connected to Kafka');

  await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const raw = JSON.parse(message.value.toString());

        const trade = {
          symbol:    normalizeSymbol(raw.symbol),
          price:     raw.price,
          volume:    raw.volume,
          timestamp: new Date(raw.timestamp),
          exchange:  raw.exchange,
          tradeId:   raw.tradeId,
        };

        // Fire-and-forget MongoDB write
        TradeRaw.create(trade).catch(console.error);

        // Notify API server (cross-process)
        fetch(`${INTERNAL_URL}/internal/trade`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(trade),
        }).catch(() => {});

        console.log(`[Consumer1] ${trade.exchange} ${trade.symbol} $${trade.price} vol=${trade.volume}`);
      } catch (err) {
        console.error('[Consumer1] Process error:', err.message);
      }
    },
  });
}

consumer.on('consumer.crash', async ({ payload }) => {
  console.error('[Consumer1] Crash:', payload.error.message, '— restarting in 10s');
  await consumer.disconnect().catch(() => {});
  setTimeout(run, 10000);
});

process.on('SIGINT', async () => {
  console.log('[Consumer1] Shutting down...');
  await consumer.disconnect();
  process.exit(0);
});

run().catch((err) => {
  console.error('[Consumer1] Fatal:', err.message);
  process.exit(1);
});
