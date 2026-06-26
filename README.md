# Crypto Market Monitor — Real-Time Pipeline

Real-time crypto data pipeline:
**WebSocket (Coinbase) → Kafka → 3 Consumers → MongoDB → REST API + Socket.IO → Dashboard**

## Architecture

```
Coinbase WS ──► Kafka (crypto.trades.raw, 3 partitions)
                        │
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
- PM2 (gestionnaire de processus)

```bash
npm install -g pm2
```

## Démarrage

### 1. Copier le fichier d'environnement

```bash
cp .env.example .env
```

### 2. Démarrer Kafka + MongoDB

```bash
docker-compose up -d
```

Attendre ~20 secondes que Kafka soit prêt.

### 3. Installer les dépendances

```bash
npm install
```

### 4. Créer le topic Kafka (une seule fois)

```bash
npm run kafka:init
```

Résultat attendu : `Topic "crypto.trades.raw" created with 3 partitions`

### 5. Démarrer tout le pipeline

```bash
npm start
```

PM2 démarre les 5 processus en arrière-plan :

| Processus        | Rôle                        |
|------------------|-----------------------------|
| `cmms-api`       | Serveur Express + Socket.IO |
| `cmms-normalizer`| Consumer 1 — normalisation  |
| `cmms-aggregator`| Consumer 2 — agrégation     |
| `cmms-anomaly`   | Consumer 3 — détection      |
| `cmms-producer`  | Producteur Coinbase WS      |

### 6. Ouvrir le dashboard

**http://localhost:3000**

Le voyant LIVE passe au vert en quelques secondes.

---

## Gestion du pipeline (PM2)

```bash
npm run status    # état de tous les processus
npm run logs      # logs en temps réel (tous les processus)
npm run stop      # arrêter tout
npm run restart   # redémarrer tout
npm run reload    # rechargement sans downtime
npm run flush     # vider les fichiers de logs
```

Les logs sont écrits dans le dossier `logs/` :

```
logs/
  api.out.log
  api.err.log
  normalizer.out.log
  aggregator.out.log
  anomaly.out.log
  producer.out.log
```

---

## REST API

| Endpoint | Description |
|---|---|
| `GET /api/trades?symbol=BTCUSDT&limit=50` | Trades normalisés récents |
| `GET /api/stats?symbol=BTCUSDT&window=5min` | Agrégats fenêtre glissante |
| `GET /api/alerts?limit=20` | Anomalies détectées |
| `GET /api/ohlc?symbol=BTCUSD&range=1D` | Chandeliers OHLC (1D/1W/1M/1Y) |
| `GET /api/health` | Health check |

## Socket.IO Events (server → client)

| Event | Payload |
|---|---|
| `trade` | `{ symbol, price, volume, timestamp, exchange }` |
| `stats` | `{ symbol, windows: { '1min': { avgPrice, cumVolume, tradeCount }, … } }` |
| `alert` | `{ symbol, type, message, price, volume, timestamp, exchange }` |

## Règles de détection d'anomalies

- **LARGE_VOLUME** : volume du trade > 3× la moyenne mobile des 50 derniers trades
- **PRICE_SPIKE** : variation de prix >= 1% sur une fenêtre de 10 secondes

## Arrêt

```bash
npm run stop
docker-compose down
```
