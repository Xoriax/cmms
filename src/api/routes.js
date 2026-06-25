const express = require('express');
const { TradeRaw, Aggregate, Alert } = require('../db/models');

const router = express.Router();

// GET /api/trades?symbol=BTCUSDT&limit=50
router.get('/trades', async (req, res) => {
  try {
    const symbol = req.query.symbol ? req.query.symbol.toUpperCase() : null;
    const limit  = Math.min(parseInt(req.query.limit) || 50, 200);
    const query  = symbol ? { symbol } : {};

    const trades = await TradeRaw
      .find(query)
      .sort({ timestamp: -1 })
      .limit(limit)
      .lean();

    res.json({ ok: true, count: trades.length, data: trades });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/stats?symbol=BTCUSDT&window=5min
router.get('/stats', async (req, res) => {
  try {
    const symbol = req.query.symbol ? req.query.symbol.toUpperCase() : null;
    const window = req.query.window || null;
    const query  = {};
    if (symbol) query.symbol = symbol;
    if (window) query.window = window;

    const stats = await Aggregate
      .find(query)
      .sort({ computedAt: -1 })
      .lean();

    res.json({ ok: true, count: stats.length, data: stats });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/alerts?symbol=BTCUSDT&limit=20
router.get('/alerts', async (req, res) => {
  try {
    const symbol = req.query.symbol ? req.query.symbol.toUpperCase() : null;
    const limit  = Math.min(parseInt(req.query.limit) || 20, 100);
    const query  = symbol ? { symbol } : {};

    const alerts = await Alert
      .find(query)
      .sort({ timestamp: -1 })
      .limit(limit)
      .lean();

    res.json({ ok: true, count: alerts.length, data: alerts });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/ohlc?symbol=BTCUSD&range=1D|1W|1M|1Y
const OHLC_CONFIG = {
  '1D': { granularity: 900,   hours: 24 },
  '1W': { granularity: 3600,  hours: 24 * 7 },
  '1M': { granularity: 21600, hours: 24 * 30 },
  '1Y': { granularity: 86400, hours: 24 * 300 },
};

router.get('/ohlc', async (req, res) => {
  try {
    const symbol = (req.query.symbol || 'BTCUSD').toUpperCase();
    const range  = req.query.range || '1D';
    const cfg    = OHLC_CONFIG[range];
    if (!cfg) return res.status(400).json({ ok: false, error: 'Invalid range. Use 1D, 1W, 1M or 1Y' });

    // BTCUSD → BTC-USD, ETHUSD → ETH-USD
    const productId = symbol.slice(0, 3) + '-' + symbol.slice(3);
    const end   = new Date();
    const start = new Date(end.getTime() - cfg.hours * 3600 * 1000);

    const url = `https://api.exchange.coinbase.com/products/${productId}/candles` +
      `?granularity=${cfg.granularity}&start=${start.toISOString()}&end=${end.toISOString()}`;

    const upstream = await fetch(url, { headers: { 'User-Agent': 'cmms/1.0' } });
    if (!upstream.ok) return res.status(502).json({ ok: false, error: `Coinbase returned ${upstream.status}` });

    const candles = await upstream.json();
    if (!Array.isArray(candles)) return res.status(502).json({ ok: false, error: 'Unexpected response from Coinbase' });

    // Coinbase: [time, low, high, open, close, volume] newest first → sort asc
    const data = candles
      .sort((a, b) => a[0] - b[0])
      .map(([time, low, high, open, close, volume]) => ({ time, open, high, low, close, volume }));

    res.json({ ok: true, count: data.length, data });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/health — pipeline health check
router.get('/health', (_req, res) => {
  res.json({ ok: true, ts: new Date().toISOString() });
});

module.exports = router;
