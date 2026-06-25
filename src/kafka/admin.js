require('dotenv').config();
const { Kafka } = require('kafkajs');

const BROKER  = process.env.KAFKA_BROKER || 'localhost:9092';
const TOPIC   = process.env.KAFKA_TOPIC  || 'crypto.trades.raw';

const kafka = new Kafka({
  clientId: 'cmms-admin',
  brokers: [BROKER],
  retry: { retries: 10, initialRetryTime: 1000 },
});

async function createTopics() {
  const admin = kafka.admin();
  try {
    await admin.connect();
    console.log('[Admin] Connected to Kafka broker:', BROKER);

    const existing = await admin.listTopics();
    if (existing.includes(TOPIC)) {
      console.log(`[Admin] Topic "${TOPIC}" already exists — skipping creation`);
      return;
    }

    await admin.createTopics({
      topics: [{
        topic: TOPIC,
        numPartitions: 3,
        replicationFactor: 1,
        configEntries: [
          { name: 'retention.ms', value: String(24 * 60 * 60 * 1000) }, // 24h
        ],
      }],
    });
    console.log(`[Admin] Topic "${TOPIC}" created with 3 partitions`);
  } catch (err) {
    console.error('[Admin] Error:', err.message);
    process.exit(1);
  } finally {
    await admin.disconnect();
    console.log('[Admin] Done.');
  }
}

createTopics();
