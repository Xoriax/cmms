# Crypto Market Monitor — Real-Time Pipeline

Real-time crypto data pipeline:
**WebSocket (Binance/Coinbase) → Kafka → 3 Consumers → MongoDB → REST API + Socket.IO → Dashboard**

## Architecture

```
Binance WS ──┐
              ├─► Kafka (crypto.trades.raw, 3 partitions)
Coinbase WS ──┘         │
                ┌────────┼────────┐
           Consumer1  Consumer2  Consumer3
           Normalizer Aggregator  Anomaly
                │          │         │
             MongoDB    MongoDB   MongoDB
           trades_raw aggregates  alerts
                └──────────┴─────────┘
                           │
                     EventBus (in-memory)
                           │
                    Express + Socket.IO
                           │
                    Dashboard (HTML/JS)
```

## Prerequisites

- Docker & Docker Compose
- Node.js >= 18

## Startup (in order)

### 1. Copy environment file

```bash
cp .env.example .env
```

### 2. Start Kafka + MongoDB

```bash
docker-compose up -d
```

Wait ~20 seconds for Kafka to be fully ready.

### 3. Install dependencies

```bash
npm install
```

### 4. Create Kafka topic (run once)

```bash
node src/kafka/admin.js
```

Expected output: `Topic "crypto.trades.raw" created with 3 partitions`

### 5. Start the API server

```bash
node src/api/server.js
```

### 6. Start all consumers (3 separate terminals)

```bash
node src/consumers/consumer1-normalizer.js
node src/consumers/consumer2-aggregator.js
node src/consumers/consumer3-anomaly.js
```

### 7. Start data producers (2 separate terminals)

```bash
node src/ingestion/binanceProducer.js
node src/ingestion/coinbaseProducer.js
```

### 8. Open the dashboard

Navigate to: **http://localhost:3000**

The LIVE indicator turns green within seconds of trades flowing in.

## REST API

| Endpoint | Description |
|---|---|
| `GET /api/trades?symbol=BTCUSDT&limit=50` | Recent normalized trades |
| `GET /api/stats?symbol=BTCUSDT&window=5min` | Sliding window aggregates |
| `GET /api/alerts?limit=20` | Detected anomalies |
| `GET /api/health` | Health check |

## Socket.IO Events (server → client)

| Event | Payload |
|---|---|
| `trade` | `{ symbol, price, volume, timestamp, exchange }` |
| `stats` | `{ symbol, windows: { '1min': { avgPrice, cumVolume, tradeCount }, … } }` |
| `alert` | `{ symbol, type, message, price, volume, timestamp, exchange }` |

## Anomaly Detection Rules

- **LARGE_VOLUME**: trade volume > 3× moving average of last 50 trades
- **PRICE_SPIKE**: price change >= 1% within a 10-second window

## Stop everything

```bash
docker-compose down
# Ctrl+C each Node process
```
