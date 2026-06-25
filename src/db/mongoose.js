require('dotenv').config();
const mongoose = require('mongoose');

const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/cmms';

let connected = false;

async function connect() {
  if (connected) return;
  try {
    await mongoose.connect(MONGO_URI);
    connected = true;
    console.log('[MongoDB] Connected to', MONGO_URI);
  } catch (err) {
    console.error('[MongoDB] Connection error:', err.message);
    setTimeout(connect, 5000);
  }
}

mongoose.connection.on('disconnected', () => {
  connected = false;
  console.warn('[MongoDB] Disconnected — retrying in 5s');
  setTimeout(connect, 5000);
});

mongoose.connection.on('error', (err) => {
  console.error('[MongoDB] Error:', err.message);
});

module.exports = { connect, mongoose };
