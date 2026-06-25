require('dotenv').config();
const express = require('express');
const http    = require('http');
const path    = require('path');
const { Server: SocketIO } = require('socket.io');
const { connect: connectMongo } = require('../db/mongoose');
const routes  = require('./routes');
const eventBus = require('./eventBus');

const PORT = parseInt(process.env.API_PORT) || 3000;

const app    = express();
const server = http.createServer(app);
const io     = new SocketIO(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
});

app.use(express.json());
app.use(express.static(path.join(__dirname, '../../dashboard')));

app.use('/api', routes);

// Internal endpoints called by consumers (cross-process event bridge)
app.post('/internal/trade', (req, res) => { eventBus.emit('trade', req.body); res.sendStatus(204); });
app.post('/internal/stats', (req, res) => { eventBus.emit('stats', req.body); res.sendStatus(204); });
app.post('/internal/alert', (req, res) => { eventBus.emit('alert', req.body); res.sendStatus(204); });

// ── Socket.IO ────────────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  console.log(`[API] Dashboard connected: ${socket.id}`);
  socket.emit('connected', { ts: new Date().toISOString() });

  socket.on('disconnect', () => {
    console.log(`[API] Dashboard disconnected: ${socket.id}`);
  });
});

// Bridge in-memory event bus → Socket.IO broadcast
eventBus.on('trade', (trade) => {
  io.emit('trade', trade);
});

eventBus.on('stats', (stats) => {
  io.emit('stats', stats);
});

eventBus.on('alert', (alert) => {
  io.emit('alert', alert);
});

// ── Startup ──────────────────────────────────────────────────────────────────
async function main() {
  await connectMongo();
  server.listen(PORT, () => {
    console.log(`[API] Server running on http://localhost:${PORT}`);
    console.log(`[API] Dashboard at http://localhost:${PORT}/index.html`);
  });
}

main().catch((err) => {
  console.error('[API] Fatal:', err.message);
  process.exit(1);
});
