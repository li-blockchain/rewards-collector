// Load environment variables from .env file
require('dotenv').config();
const { createPublicClient, http } = require('viem');
const { mainnet } = require('viem/chains');

// Use the client URL from the .env file
const publicClient = createPublicClient({
  chain: mainnet,
  transport: http(process.env.CLIENT_URL) // Load URL from .env
});

module.exports = { publicClient };
