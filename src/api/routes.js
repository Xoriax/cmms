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

// GET /api/health — pipeline health check
router.get('/health', (_req, res) => {
  res.json({ ok: true, ts: new Date().toISOString() });
});

module.exports = router;
