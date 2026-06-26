#!/usr/bin/env node
/**
 * Single-command startup: Docker (Kafka + MongoDB) → wait for ready → PM2
 * Usage: npm run dev
 */
const { execSync, exec } = require('child_process');
const net = require('net');

const KAFKA_HOST   = 'localhost';
const KAFKA_PORT   = 9093; // docker-compose maps host:9093 → container:9092
const MONGO_HOST   = 'localhost';
const MONGO_PORT   = 27017;
const MAX_WAIT_MS  = 90_000;
const POLL_MS      = 2_000;

function log(msg)  { console.log(`\x1b[36m[start]\x1b[0m ${msg}`); }
function ok(msg)   { console.log(`\x1b[32m[start]\x1b[0m ${msg}`); }
function err(msg)  { console.error(`\x1b[31m[start]\x1b[0m ${msg}`); }

function tcpReady(host, port) {
  return new Promise((resolve) => {
    const sock = new net.Socket();
    sock.setTimeout(1000);
    sock.on('connect', () => { sock.destroy(); resolve(true); });
    sock.on('error',   () => { sock.destroy(); resolve(false); });
    sock.on('timeout', () => { sock.destroy(); resolve(false); });
    sock.connect(port, host);
  });
}

async function waitFor(label, host, port) {
  log(`Waiting for ${label} on ${host}:${port}...`);
  const deadline = Date.now() + MAX_WAIT_MS;
  while (Date.now() < deadline) {
    if (await tcpReady(host, port)) {
      ok(`${label} is ready`);
      return;
    }
    await new Promise(r => setTimeout(r, POLL_MS));
    process.stdout.write('.');
  }
  process.stdout.write('\n');
  throw new Error(`${label} not ready after ${MAX_WAIT_MS / 1000}s`);
}

async function run() {
  // 1 — Start Docker services
  log('Starting Docker services (Kafka, Zookeeper, MongoDB)...');
  try {
    execSync('docker-compose up -d', { stdio: 'inherit' });
  } catch (e) {
    err('docker-compose failed — is Docker running?');
    process.exit(1);
  }

  // 2 — Wait for Kafka and MongoDB to be reachable
  try {
    await Promise.all([
      waitFor('Kafka',   KAFKA_HOST, KAFKA_PORT),
      waitFor('MongoDB', MONGO_HOST, MONGO_PORT),
    ]);
  } catch (e) {
    err(e.message);
    process.exit(1);
  }

  // 3 — Extra buffer so Kafka broker is fully elected
  log('Kafka & MongoDB ready — waiting 3s for broker election...');
  await new Promise(r => setTimeout(r, 3000));

  // 4 — Start all Node.js processes via PM2 (clean slate)
  log('Starting PM2 processes...');
  try {
    // Delete any stale/partial PM2 state before starting fresh
    try { execSync('npx pm2 delete ecosystem.config.js', { stdio: 'ignore' }); } catch (_) {}
    execSync('npx pm2 start ecosystem.config.js', { stdio: 'inherit' });
    execSync('npx pm2 status', { stdio: 'inherit' });
  } catch (e) {
    err('PM2 start failed: ' + e.message);
    process.exit(1);
  }

  ok('All services started. Dashboard → http://localhost:3000');
  ok('Logs: npm run logs  |  Status: npm run status  |  Stop: npm run stop:all');
}

run().catch(e => { err(e.message); process.exit(1); });
