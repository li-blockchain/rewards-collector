# Validator Performance Aggregator (Rewards Collector)

A Python-based toolkit for extracting Ethereum validator rewards using the Beaconcha.in API. The system monitors validators every 100 epochs and captures consensus layer withdrawals and execution layer block proposal rewards.

## Features

- Validator rewards collection from Beaconcha.in API
- Continuous monitoring with configurable intervals
- Historical backfilling with progress tracking
- Discord bot integration for queries and alerts
- CDP (Collateralized Debt Position) monitoring
- Invoice generation with LEB reward adjustments
- Efficient Parquet file storage for analytics

## Quick Start

```bash
# Install dependencies
cd src
pip install -r requirements.txt

# Configure environment
cp ../.env.dist ../.env
# Edit .env with your API keys

# Run rewards collector for a specific epoch
python rewards_collector.py 390000

# Run continuous monitoring
python rewards_monitor.py

# Run historical backfilling
python rewards_backfiller.py
```

## Documentation

- [Rewards Collector Guide](src/REWARDS_COLLECTOR_README.md) - Detailed usage for rewards collection
- [CDP Setup Guide](src/CDP_SETUP.md) - CDP monitoring configuration

## Configuration

Copy `.env.dist` to `.env` and configure. Key variables:

- `API_KEY` - Beaconcha.in API key (required)
- `DISCORD_BOT_TOKEN` - Discord bot token
- `RPC_URL` - Ethereum RPC endpoint
- `EPOCH_START` - Starting epoch for monitoring
- `EPOCH_INTERVAL` - Epochs between checks (default: 100)

See `.env.dist` for full configuration options.

## Validator Data Format

Place validator CSV files in the `data/` directory with format:
```
Index,Pubkey,Type,Node,Minipool address
```

The Type field is used for LEB (Liquid Ethereum Bond) reward adjustments:
- Type 8-14: LEB8 (14% commission on borrowed portion)
- Type 16: LEB16 (15% commission on borrowed portion)

## Project Structure

```
src/
├── rewards_collector.py   # Single epoch collection
├── rewards_monitor.py     # Continuous monitoring
├── rewards_backfiller.py  # Historical backfilling
├── bot.py                 # Discord bot
├── cdp_monitor.py         # CDP monitoring
├── invoice.py             # Invoice generation
└── generate_invoice.py    # Invoice export
data/
└── validators.csv         # Validator list
```
