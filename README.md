# Crypto Market Monitor - Real-Time Pipeline

Real-time crypto data pipeline:
**WebSocket (Coinbase) -> Kafka -> 3 Consumers -> MongoDB -> REST API + Socket.IO -> Dashboard**

## Architecture

```
Coinbase WS --> Kafka (crypto.trades.raw, 3 partitions)
                        |
                +--------+--------+
           Consumer1  Consumer2  Consumer3
           Normalizer Aggregator  Anomaly
                |          |         |
             MongoDB    MongoDB   MongoDB
           trades_raw aggregates  alerts
                +-----------+---------+
                           |
                     EventBus (in-memory)
                           |
                    Express + Socket.IO
                           |
                    Dashboard (HTML/JS)
```

> Le dashboard ne touche jamais Kafka ni MongoDB directement. Toutes les données
> passent par la couche API (Express + Socket.IO). Ce découplage garantit la
> robustesse et la scalabilité du système.

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

### 2. Installer les dépendances

```bash
npm install
```

### 3. Créer le topic Kafka (une seule fois)

```bash
npm run kafka:init
```

Résultat attendu : `Topic "crypto.trades.raw" created with 3 partitions`

### 4. Tout démarrer en une commande

```bash
npm run dev
```

Ce script unique :
1. Lance Docker Compose (Kafka + Zookeeper + MongoDB)
2. Attend que Kafka (port 9093) et MongoDB (port 27017) soient joignables (max 90s)
3. Démarre tous les processus Node.js via PM2

PM2 gère les 5 processus en arrière-plan :

| Processus         | Rôle                        |
|-------------------|-----------------------------|
| `cmms-api`        | Serveur Express + Socket.IO |
| `cmms-normalizer` | Consumer 1 - normalisation  |
| `cmms-aggregator` | Consumer 2 - agrégation     |
| `cmms-anomaly`    | Consumer 3 - détection      |
| `cmms-producer`   | Producteur Coinbase WS      |

### 5. Ouvrir le dashboard

**http://localhost:3000**

Le voyant LIVE passe au vert en quelques secondes.

---

## Gestion du pipeline (PM2)

```bash
npm run status    # état de tous les processus
npm run logs      # logs en temps réel (tous les processus)
npm run stop      # arrêter les processus PM2
npm run stop:all  # arrêter PM2 + Docker
npm run restart   # redémarrer tout
npm run reload    # rechargement sans downtime
npm run flush     # vider les fichiers de logs
```

Les logs sont écrits dans le dossier `logs/` :

```
logs/
  api.out.log / api.err.log
  normalizer.out.log / normalizer.err.log
  aggregator.out.log / aggregator.err.log
  anomaly.out.log / anomaly.err.log
  producer.out.log / producer.err.log
```

---

## Dashboard

Accessible sur **http://localhost:3000**, le dashboard affiche en temps réel :

- **Ticker** : prix live BTC/USD et ETH/USD avec variation
- **4 KPI cards** : Prix actuel, Trades/seconde, Anomalies (10 min), Vol glissant 5min
- **Graphique prix** : fenêtre live + historique OHLC (1J / 1S / 1M / 1Y) avec moyenne mobile
- **Trades récents** : flux live avec exchange, côté (BUY/SELL), variation de prix
- **Alertes temps réel** : LARGE_VOLUME et PRICE_SPIKE avec message détaillé
- **Volume par fenêtre** : barres glissantes 1min / 5min / 15min / 1h
- **Santé pipeline** : état WebSocket, Kafka, Socket.IO, débit d'ingestion

### Theme dark / light

Bouton Dark/Light dans le header, préférence sauvegardée dans `localStorage`.

---

## REST API

| Endpoint | Description |
|---|---|
| `GET /api/trades?symbol=BTCUSDT&limit=50` | Trades normalisés récents |
| `GET /api/stats?symbol=BTCUSDT&window=5min` | Agrégats fenêtre glissante |
| `GET /api/alerts?limit=20` | Anomalies détectées |
| `GET /api/ohlc?symbol=BTCUSD&range=1D` | Chandeliers OHLC (1D/1W/1M/1Y) |
| `GET /api/health` | Health check |

## Socket.IO Events (server -> client)

| Event | Payload |
|---|---|
| `trade` | `{ symbol, price, volume, timestamp, exchange, side, tradeId }` |
| `stats` | `{ symbol, windows: { '1min': { avgPrice, cumVolume, tradeCount }, ... } }` |
| `alert` | `{ symbol, type, message, price, volume, threshold, timestamp, exchange }` |

---

## Règles de détection d'anomalies

| Type | Condition | Fenêtre |
|---|---|---|
| `LARGE_VOLUME` | volume > **2x** la moyenne mobile des 50 derniers trades | par trade (warmup : 5 trades min) |
| `PRICE_SPIKE`  | variation de prix >= **0.5%** | fenêtre glissante de 10 secondes |

---

## Arrêt

```bash
npm run stop:all   # PM2 + Docker en une commande
```

Ou manuellement :

```bash
npm run stop
docker-compose down
```
