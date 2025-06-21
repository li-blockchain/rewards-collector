# CDP Monitoring Setup Guide

This guide explains how to set up the CDP (Collateralized Debt Position) monitoring functionality for your Discord bot using your own Ethereum nodes.

## Overview

The CDP monitoring system allows you to:
- Check your Aave position status with `!cdp` command
- Monitor position health automatically via cron job
- Receive alerts when your position is at risk
- Connect to your own Ethereum nodes (no external RPC providers needed)

## Environment Variables

Add these environment variables to your `.env` file:

```bash
# Discord Bot Configuration
DISCORD_BOT_TOKEN=your_discord_bot_token_here

# CDP Monitoring Configuration
CDP_POSITION_ADDRESS=0xYourPositionAddressHere
CDP_HEALTH_THRESHOLD=1.5
CDP_ALERT_CHANNEL_ID=1234567890123456789

# Web3 Configuration - Your Local Node
RPC_URL=http://localhost:8545
# Alternative connection types:
# RPC_URL=ws://localhost:8546          # WebSocket
# RPC_URL=ipc:///path/to/geth.ipc      # IPC (Unix domain socket)

# Discord Webhook (for simple monitoring script)
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/your-webhook-url
```

### Configuration Details

- `CDP_POSITION_ADDRESS`: The Ethereum address of your CDP position (wallet/contract address)
- `CDP_HEALTH_THRESHOLD`: Minimum collateralization ratio (default: 1.5 = 150%)
- `CDP_ALERT_CHANNEL_ID`: Discord channel ID where alerts should be sent (for bot-based monitoring)
- `RPC_URL`: Connection to your local Ethereum node
- `DISCORD_WEBHOOK_URL`: Discord webhook URL for simple monitoring script

### Node Connection Types

The system supports multiple connection types to your local nodes:

#### HTTP Connection (Default)
```bash
RPC_URL=http://localhost:8545
RPC_URL=http://192.168.1.100:8545
```

#### WebSocket Connection
```bash
RPC_URL=ws://localhost:8546
RPC_URL=wss://your-node.com:8546
```

#### IPC Connection (Unix Domain Socket)
```bash
RPC_URL=ipc:///path/to/geth.ipc
RPC_URL=/path/to/geth.ipc
```

## Usage

### Manual Position Check

Use the `!cdp` command in Discord to get a snapshot of your position:

```
!cdp
```

This will return a detailed report including:
- WETH collateral supplied and balance
- GHO debt borrowed and balance
- Current prices
- Collateralization ratio
- Health factor
- Position status (HEALTHY/AT RISK)

### Automated Monitoring

You have two options for automated monitoring:

#### Option 1: Simple Webhook-Based Monitoring (Recommended)

This option uses Discord webhooks and avoids asyncio issues:

1. **Create a Discord Webhook**:
   - Go to your Discord channel settings
   - Select "Integrations" → "Webhooks"
   - Create a new webhook and copy the URL
   - Add it to your `.env` file as `DISCORD_WEBHOOK_URL`

2. **Set up cron job with virtual environment**:
   ```bash
   # Check every 30 minutes
   */30 * * * * cd /path/to/your/bot && source venv/bin/activate && python cdp_monitor_simple.py

   # Check every hour
   0 * * * * cd /path/to/your/bot && source venv/bin/activate && python cdp_monitor_simple.py

   # Check every 4 hours
   0 */4 * * * * cd /path/to/your/bot && source venv/bin/activate && python cdp_monitor_simple.py
   ```

#### Option 2: Bot-Based Monitoring

This option uses the full Discord bot client:

```bash
# Check every 30 minutes
*/30 * * * * cd /path/to/your/bot && source venv/bin/activate && python cdp_monitor.py

# Check every hour
0 * * * * cd /path/to/your/bot && source venv/bin/activate && python cdp_monitor.py

# Check every 4 hours
0 */4 * * * * cd /path/to/your/bot && source venv/bin/activate && python cdp_monitor.py
```

### Alternative Cron Setup Methods

#### Method 1: Using a Shell Script Wrapper

Create a shell script to handle the environment setup:

```bash
# Create cdp_monitor_wrapper.sh
#!/bin/bash
cd /path/to/your/bot
source venv/bin/activate
python cdp_monitor_simple.py
```

Make it executable:
```bash
chmod +x cdp_monitor_wrapper.sh
```

Then use in cron:
```bash
# Check every 30 minutes
*/30 * * * * /path/to/your/bot/cdp_monitor_wrapper.sh
```

#### Method 2: Using Full Paths

Specify the full path to Python in your virtual environment:

```bash
# Check every 30 minutes
*/30 * * * * cd /path/to/your/bot && /path/to/your/bot/venv/bin/python cdp_monitor_simple.py
```

#### Method 3: Environment Variables in Cron

Add environment variables directly to the cron job:

```bash
# Check every 30 minutes
*/30 * * * * cd /path/to/your/bot && source venv/bin/activate && RPC_URL=http://localhost:8545 CDP_POSITION_ADDRESS=0xYourAddress DISCORD_WEBHOOK_URL=your-webhook-url python cdp_monitor_simple.py
```

### Testing Your Cron Setup

1. **Test the script manually first**:
   ```bash
   cd /path/to/your/bot
   source venv/bin/activate
   python cdp_monitor_simple.py
   ```

2. **Test the cron command**:
   ```bash
   # Run the exact cron command manually
   cd /path/to/your/bot && source venv/bin/activate && python cdp_monitor_simple.py
   ```

3. **Check cron logs**:
   ```bash
   # View cron logs
   tail -f /var/log/cron

   # Or check system logs
   journalctl -u cron
   ```

The monitoring scripts will:
- Check your position health
- Send alerts to Discord if the position is at risk
- Log all activities to `cdp_monitor.log`

## Position Details

The system monitors:
- **Collateral**: WETH supplied to Aave
- **Debt**: GHO borrowed from Aave
- **Health Factor**: Calculated based on collateralization ratio
- **Risk Threshold**: Configurable minimum health factor

## Alerts

You'll receive alerts when:
- Position collateralization ratio falls below threshold
- Monitoring system encounters errors
- Position data cannot be retrieved

## Troubleshooting

### Common Issues

1. **"CDP_POSITION_ADDRESS environment variable not set"**
   - Make sure you've set the correct position address in your `.env` file
   - Ensure the `.env` file is in the correct directory when running from cron

2. **"Failed to connect to Ethereum node"**
   - Check your RPC_URL is correct and your node is running
   - Verify the node is accessible from the bot's location
   - Test connection: `curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}' http://localhost:8545`

3. **"Error getting position data"**
   - Verify the position address has activity on Aave
   - Check your node is fully synced
   - Ensure your node supports the required RPC methods

4. **"Could not find Discord channel"** (bot-based monitoring)
   - Ensure CDP_ALERT_CHANNEL_ID is set to a valid channel ID
   - Make sure the bot has permissions to send messages in that channel

5. **"DISCORD_WEBHOOK_URL environment variable not set"** (webhook-based monitoring)
   - Create a Discord webhook and add the URL to your `.env` file
   - Ensure the webhook URL is valid and accessible

6. **"SSL Transport Errors"** (bot-based monitoring)
   - Use the simple webhook-based monitoring script instead
   - The webhook version avoids asyncio cleanup issues

7. **"Module not found" or "Import errors" in cron**
   - Ensure the virtual environment is properly activated in the cron job
   - Check that all dependencies are installed in the virtual environment
   - Verify the working directory is correct

8. **"Environment variables not found" in cron**
   - The `.env` file must be in the working directory when the script runs
   - Consider using Method 3 above to set environment variables directly in cron
   - Or use a shell script wrapper to ensure proper environment setup

### Node Requirements

Your Ethereum node should:
- Be fully synced to mainnet
- Support JSON-RPC API
- Have the following methods enabled:
  - `eth_call`
  - `eth_chainId`
  - `eth_getBalance`

### Logs

Check the `cdp_monitor.log` file for detailed error messages and monitoring activity.

## Security Notes

- Keep your `.env` file secure and never commit it to version control
- Use a dedicated Discord bot token for monitoring
- Your local node connection is more secure than external RPC providers
- Regularly review and update your health threshold based on market conditions
- Consider using a dedicated monitoring node to avoid impacting your main node's performance
- Webhook URLs should be kept secret as they allow posting to your Discord channel 