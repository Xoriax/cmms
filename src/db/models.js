const { mongoose } = require('./mongoose');
const { Schema } = mongoose;

// Raw normalized trades — TTL 24h
const tradeRawSchema = new Schema({
  symbol:    { type: String, required: true, index: true },
  price:     { type: Number, required: true },
  volume:    { type: Number, required: true },
  timestamp: { type: Date,   required: true },
  exchange:  { type: String, required: true },
  tradeId:   { type: String },
  side:      { type: String }, // 'BUY' | 'SELL'
}, { versionKey: false });

tradeRawSchema.index({ timestamp: 1 }, { expireAfterSeconds: 86400 }); // 24h TTL

// Sliding window aggregates
const aggregateSchema = new Schema({
  symbol:    { type: String, required: true, index: true },
  window:    { type: String, required: true }, // '1min','5min','15min','1h'
  avgPrice:  { type: Number },
  cumVolume: { type: Number },
  tradeCount:{ type: Number },
  computedAt:{ type: Date, default: Date.now },
}, { versionKey: false });

aggregateSchema.index({ symbol: 1, window: 1 });

// Anomaly alerts
const alertSchema = new Schema({
  symbol:    { type: String, required: true, index: true },
  type:      { type: String, required: true }, // 'LARGE_VOLUME' | 'PRICE_SPIKE'
  price:     { type: Number },
  volume:    { type: Number },
  threshold: { type: Number },
  message:   { type: String },
  timestamp: { type: Date, default: Date.now, index: true },
  exchange:  { type: String },
}, { versionKey: false });

const TradeRaw  = mongoose.model('TradeRaw',  tradeRawSchema,  'trades_raw');
const Aggregate = mongoose.model('Aggregate', aggregateSchema, 'aggregates');
const Alert     = mongoose.model('Alert',     alertSchema,     'alerts');

module.exports = { TradeRaw, Aggregate, Alert };
